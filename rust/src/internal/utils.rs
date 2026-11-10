use std::collections::VecDeque;
use std::hash::Hash;
use std::io;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use futures::stream::{BoxStream, Stream, StreamExt};
use rustc_hash::FxHashSet;
use tokio::sync::{broadcast, watch};

use super::FileAddress;
use crate::model::{IndexConfig, IndexStatusUpdate};

pub fn debug_log(cfg: &dyn IndexConfig, s: impl FnOnce() -> String) {
    if cfg.enable_logging().load(Ordering::Relaxed) {
        println!("{}", s());
    }
}

pub fn now_millis() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// Kotlin `Flow<T>.distinct()`.
pub fn distinct<T, S>(s: S) -> impl Stream<Item = T>
where
    S: Stream<Item = T>,
    T: Clone + Eq + Hash,
{
    let mut seen = FxHashSet::default();
    s.filter(move |x| futures::future::ready(seen.insert(x.clone())))
}

/// One-shot latch that can be awaited many times (Kotlin: `CompletableDeferred<Unit>`).
#[derive(Clone)]
pub struct Latch {
    tx: Arc<watch::Sender<bool>>,
}

impl Default for Latch {
    fn default() -> Self {
        Latch { tx: Arc::new(watch::channel(false).0) }
    }
}

impl Latch {
    pub fn complete(&self) {
        self.tx.send_replace(true);
    }

    pub fn is_completed(&self) -> bool {
        *self.tx.borrow()
    }

    pub async fn wait(&self) {
        let mut rx = self.tx.subscribe();
        // the sender lives in `self`, so this cannot fail
        let _ = rx.wait_for(|v| *v).await;
    }
}

/// Interns file addresses so that every event about the same file shares one allocation
/// (Kotlin: Guava `Interners.newWeakInterner()`). Entries nobody references any more are purged lazily.
#[derive(Default)]
pub struct Interner {
    inner: Mutex<InternerInner>,
}

#[derive(Default)]
struct InternerInner {
    set: FxHashSet<Arc<str>>,
    purge_at: usize,
}

impl Interner {
    pub fn intern(&self, s: String) -> FileAddress {
        let mut g = self.inner.lock().unwrap();
        if let Some(a) = g.set.get(s.as_str()) {
            return FileAddress(a.clone());
        }
        let a: Arc<str> = s.into();
        g.set.insert(a.clone());
        if g.set.len() > g.purge_at {
            g.set.retain(|a| Arc::strong_count(a) > 1);
            g.purge_at = (g.set.len() * 2).max(1024);
        }
        FileAddress(a)
    }
}

/// Kotlin `File.readLines()`: strict UTF-8 (malformed input is an error) and `\n`, `\r`, `\r\n` line breaks.
pub fn read_lines(path: &Path) -> io::Result<Vec<String>> {
    let bytes = std::fs::read(path)?;
    let text = String::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(split_lines(&text).map(str::to_owned).collect())
}

pub fn split_lines(text: &str) -> impl Iterator<Item = &str> {
    let mut rest = Some(text);
    std::iter::from_fn(move || {
        let s = rest?;
        if s.is_empty() {
            rest = None;
            return None;
        }
        match s.find(['\n', '\r']) {
            None => {
                rest = None;
                Some(s)
            }
            Some(i) => {
                let skip = if s[i..].starts_with("\r\n") { 2 } else { 1 };
                rest = Some(&s[i + skip..]);
                Some(&s[..i])
            }
        }
    })
}

/// Kotlin `File.canonicalPath`: also works for files that no longer exist (e.g. after a delete event).
pub fn canonical_path(path: &Path) -> String {
    if let Ok(p) = std::fs::canonicalize(path) {
        return p.to_string_lossy().into_owned();
    }
    if let (Some(parent), Some(name)) = (path.parent(), path.file_name()) {
        if let Ok(p) = std::fs::canonicalize(parent) {
            return p.join(name).to_string_lossy().into_owned();
        }
    }
    path.to_string_lossy().into_owned()
}

pub fn rss_kb() -> (u64, u64) {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        status
            .lines()
            .find(|l| l.starts_with(name))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    };
    (field("VmRSS:"), field("VmHWM:"))
}

/// Kotlin: `MutableSharedFlow<IndexStatusUpdate>(replay = 16, onBufferOverflow = DROP_OLDEST)`.
pub struct StatusBus {
    replay: Mutex<VecDeque<IndexStatusUpdate>>,
    tx: broadcast::Sender<IndexStatusUpdate>,
}

const REPLAY: usize = 16;

impl Default for StatusBus {
    fn default() -> Self {
        StatusBus { replay: Mutex::new(VecDeque::with_capacity(REPLAY)), tx: broadcast::channel(REPLAY).0 }
    }
}

impl StatusBus {
    pub fn emit(&self, update: IndexStatusUpdate) {
        let mut replay = self.replay.lock().unwrap();
        if replay.len() == REPLAY {
            replay.pop_front();
        }
        replay.push_back(update.clone());
        let _ = self.tx.send(update);
    }

    /// Replays the buffered updates, then follows live ones; ends after emitting `Failed`.
    pub fn subscribe(self: &Arc<Self>) -> BoxStream<'static, IndexStatusUpdate> {
        let (replayed, mut rx) = {
            let replay = self.replay.lock().unwrap();
            (replay.iter().cloned().collect::<Vec<_>>(), self.tx.subscribe())
        };
        async_stream::stream! {
            for u in replayed {
                let failed = u.is_failed();
                yield u;
                if failed { return; }
            }
            loop {
                match rx.recv().await {
                    Ok(u) => {
                        let failed = u.is_failed();
                        yield u;
                        if failed { return; }
                    }
                    // slow subscriber: oldest values dropped, like DROP_OLDEST
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_lines_like_java() {
        let v: Vec<_> = split_lines("a\nb\r\nc\rd\n").collect();
        assert_eq!(v, vec!["a", "b", "c", "d"]);
        assert_eq!(split_lines("").count(), 0);
        let v: Vec<_> = split_lines("\n\nx").collect();
        assert_eq!(v, vec!["", "", "x"]);
    }

    #[test]
    fn interner_returns_same_allocation() {
        let i = Interner::default();
        let a = i.intern("x".into());
        let b = i.intern("x".into());
        assert!(a.ptr_eq(&b));
    }
}
