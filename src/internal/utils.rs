use std::collections::HashSet;
use std::hash::Hash;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::model::{FileAddress, IndexConfig};

pub(crate) fn debug_log(cfg: &dyn IndexConfig, s: impl FnOnce() -> String) {
    if cfg.enable_logging().load(Ordering::Relaxed) {
        println!("{}", s());
    }
}

pub(crate) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Emits only elements that were not seen before.
pub(crate) fn distinct<T, S>(stream: S) -> impl Stream<Item = T>
where
    T: Clone + Eq + Hash,
    S: Stream<Item = T>,
{
    let mut seen = HashSet::new();
    stream.filter(move |it| futures::future::ready(seen.insert(it.clone())))
}

/// Splits text into lines the same way `BufferedReader.readLine` does: on `\n`, `\r\n` and `\r`.
pub(crate) fn lines(text: &str) -> impl Iterator<Item = &str> {
    let mut rest = text;
    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        match rest.find(['\n', '\r']) {
            None => {
                let line = rest;
                rest = "";
                Some(line)
            }
            Some(i) => {
                let line = &rest[..i];
                let skip = if rest[i..].starts_with("\r\n") { 2 } else { 1 };
                rest = &rest[i + skip..];
                Some(line)
            }
        }
    })
}

/// Reads a file decoding it as UTF-8, replacing malformed sequences.
pub(crate) fn read_text(path: &Path) -> std::io::Result<String> {
    let bytes = std::fs::read(path)?;
    Ok(match String::from_utf8(bytes) {
        Ok(s) => s,
        Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
    })
}

/// Canonical (absolute, symlinks resolved) path. Works for files that do not exist (anymore).
pub(crate) fn canonical_path(path: &Path) -> String {
    if let Ok(p) = std::fs::canonicalize(path) {
        return p.to_string_lossy().into_owned();
    }
    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let resolved = match (absolute.parent(), absolute.file_name()) {
        (Some(parent), Some(name)) => std::fs::canonicalize(parent)
            .map(|p| p.join(name))
            .unwrap_or_else(|_| absolute.clone()),
        _ => absolute.clone(),
    };
    resolved.to_string_lossy().into_owned()
}

/// `Files.isRegularFile` - follows symlinks, false on error.
pub(crate) fn is_regular_file(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|m| m.is_file())
        .unwrap_or(false)
}

/// Interns file addresses so that equal paths share one allocation.
/// Entries that are not referenced anywhere else are periodically dropped.
#[derive(Default)]
pub(crate) struct Interner {
    inner: Mutex<InternerState>,
}

#[derive(Default)]
struct InternerState {
    set: HashSet<FileAddress>,
    cleanup_threshold: usize,
}

impl Interner {
    pub fn intern(&self, path: String) -> FileAddress {
        let mut state = self.inner.lock().unwrap();
        let fa = FileAddress::new(path);
        if let Some(existing) = state.set.get(&fa) {
            return existing.clone();
        }
        if state.set.len() >= state.cleanup_threshold.max(1024) {
            state.set.retain(|it| it.ref_count() > 1);
            state.cleanup_threshold = state.set.len() * 2;
        }
        state.set.insert(fa.clone());
        fa
    }
}

/// One-shot latch: can be opened once, can be awaited by many.
#[derive(Clone, Default)]
pub(crate) struct Latch(CancellationToken);

impl Latch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(&self) {
        self.0.cancel()
    }

    pub async fn wait(&self) {
        self.0.cancelled().await
    }
}

/// Blocking send which gives up once `cancel` is triggered (or the channel is closed).
pub(crate) fn send_blocking_cancellable<T>(
    tx: &flume::Sender<T>,
    mut msg: T,
    cancel: &CancellationToken,
) -> bool {
    loop {
        if cancel.is_cancelled() {
            return false;
        }
        match tx.send_timeout(msg, Duration::from_millis(50)) {
            Ok(()) => return true,
            Err(flume::SendTimeoutError::Timeout(m)) => msg = m,
            Err(flume::SendTimeoutError::Disconnected(_)) => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_lines_like_java() {
        assert_eq!(
            lines("a\nb\r\nc\rd").collect::<Vec<_>>(),
            vec!["a", "b", "c", "d"]
        );
        assert_eq!(lines("a\n\nb\n").collect::<Vec<_>>(), vec!["a", "", "b"]);
        assert_eq!(lines("").count(), 0);
    }

    #[test]
    fn interner_returns_same_instance() {
        let interner = Interner::default();
        let a = interner.intern("a".into());
        let b = interner.intern("a".into());
        assert!(a.ptr_eq(&b));
    }
}
