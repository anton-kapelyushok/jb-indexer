use std::fmt;

use tokio::sync::oneshot;

use crate::model::{
    FileAddress, IndexSearchResult, IndexState, SharedError, TokenPredicate, TokenSet,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FileEventSource {
    InitialSync,
    Watcher,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FileEventType {
    Create,
    Delete,
    Modify,
}

#[derive(Clone, Debug)]
pub(crate) struct FileSyncEvent {
    pub t: i64,
    pub file_address: FileAddress,
    pub source: FileEventSource,
    pub kind: FileEventType,
}

#[derive(Clone, Debug)]
pub(crate) enum StatusUpdate {
    WatcherStarted,
    FileSyncFailed { t: i64, reason: SharedError },
    AllFilesDiscovered,
    WatcherDiscoveredFileDuringInitialization,
    // source is only used for logging and tests
    FileUpdated(#[allow(dead_code)] FileEventSource),
}

pub(crate) enum FileUpdateRequest {
    UpdateFile {
        t: i64,
        file_address: FileAddress,
        tokens: TokenSet,
        source: FileEventSource,
    },
    RemoveFile {
        t: i64,
        file_address: FileAddress,
        source: FileEventSource,
    },
}

impl fmt::Debug for FileUpdateRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileUpdateRequest::UpdateFile {
                t,
                file_address,
                tokens,
                source,
            } => write!(
                f,
                "UpdateFile(t={t}, tokens={}, fileAddress={file_address}, source={source:?})",
                tokens.len()
            ),
            FileUpdateRequest::RemoveFile {
                t,
                file_address,
                source,
            } => {
                write!(
                    f,
                    "RemoveFile(t={t}, fileAddress={file_address}, source={source:?})"
                )
            }
        }
    }
}

pub(crate) enum UserRequest {
    FindFilesByToken {
        query: String,
        result: oneshot::Sender<Vec<FileAddress>>,
    },
    FindTokensMatchingPredicate {
        predicate: TokenPredicate,
        result: oneshot::Sender<Vec<String>>,
    },
    Status {
        result: oneshot::Sender<IndexState>,
    },
    Compact {
        result: oneshot::Sender<()>,
    },
}

impl fmt::Debug for UserRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UserRequest::FindFilesByToken { query, .. } => write!(f, "FindFilesByToken({query:?})"),
            UserRequest::FindTokensMatchingPredicate { .. } => {
                f.write_str("FindTokensMatchingPredicate")
            }
            UserRequest::Status { .. } => f.write_str("Status"),
            UserRequest::Compact { .. } => f.write_str("Compact"),
        }
    }
}

pub(crate) struct SearchInFileRequest {
    pub fa: FileAddress,
    pub query: std::sync::Arc<str>,
    pub result: oneshot::Sender<Vec<IndexSearchResult>>,
}
