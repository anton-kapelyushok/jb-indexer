use std::fmt;

use rustc_hash::FxHashSet;
use tokio::sync::oneshot;

use super::FileAddress;
use crate::model::{IndexSearchResult, IndexState, Reason, TokenPredicate};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileEventSource {
    InitialSync,
    Watcher,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileEventType {
    Create,
    Delete,
    Modify,
}

#[derive(Clone, Debug)]
pub struct FileSyncEvent {
    pub t: i64,
    pub file_address: FileAddress,
    pub source: FileEventSource,
    pub kind: FileEventType,
}

#[derive(Clone)]
pub enum StatusUpdate {
    WatcherStarted,
    FileSyncFailed { t: i64, reason: Reason },
    AllFilesDiscovered,
    WatcherDiscoveredFileDuringInitialization,
    FileUpdated { source: FileEventSource },
}

impl fmt::Debug for StatusUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatusUpdate::WatcherStarted => write!(f, "WatcherStarted"),
            StatusUpdate::FileSyncFailed { t, reason } => write!(f, "FileSyncFailed(t={t}, reason={reason:#})"),
            StatusUpdate::AllFilesDiscovered => write!(f, "AllFilesDiscovered"),
            StatusUpdate::WatcherDiscoveredFileDuringInitialization => {
                write!(f, "WatcherDiscoveredFileDuringInitialization")
            }
            StatusUpdate::FileUpdated { source } => write!(f, "FileUpdated({source:?})"),
        }
    }
}

pub enum FileUpdateRequest {
    UpdateFile { t: i64, file_address: FileAddress, tokens: FxHashSet<String>, source: FileEventSource },
    RemoveFile { t: i64, file_address: FileAddress, source: FileEventSource },
}

impl fmt::Debug for FileUpdateRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileUpdateRequest::UpdateFile { t, file_address, tokens, source } => write!(
                f,
                "UpdateFileContentRequest(t={t}, tokens={}, fileAddress={file_address}, source={source:?})",
                tokens.len()
            ),
            FileUpdateRequest::RemoveFile { t, file_address, source } => {
                write!(f, "RemoveFileRequest(t={t}, fileAddress={file_address}, source={source:?})")
            }
        }
    }
}

pub enum UserRequest {
    FindFilesByToken { query: String, result: oneshot::Sender<Vec<FileAddress>> },
    FindTokensMatchingPredicate { predicate: TokenPredicate, result: oneshot::Sender<Vec<String>> },
    Status { result: oneshot::Sender<IndexState> },
    Compact { result: oneshot::Sender<()> },
}

impl fmt::Debug for UserRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UserRequest::FindFilesByToken { query, .. } => write!(f, "FindFilesByToken(query={query})"),
            UserRequest::FindTokensMatchingPredicate { .. } => write!(f, "FindTokensMatchingPredicate"),
            UserRequest::Status { .. } => write!(f, "Status"),
            UserRequest::Compact { .. } => write!(f, "Compact"),
        }
    }
}

pub struct SearchInFileRequest {
    pub fa: FileAddress,
    pub query: std::sync::Arc<str>,
    pub result: oneshot::Sender<Vec<IndexSearchResult>>,
}
