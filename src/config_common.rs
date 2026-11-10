use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use futures::Stream;

use crate::model::{FileAddress, Index};

pub(crate) type ErrorHandler = Arc<dyn Fn(&anyhow::Error) + Send + Sync>;

pub(crate) struct CommonConfig {
    pub enable_logging: Arc<AtomicBool>,
    pub enable_watcher: bool,
    pub handle_watcher_error: Option<ErrorHandler>,
    pub handle_initial_file_sync_error: Option<ErrorHandler>,
}

impl CommonConfig {
    pub fn watcher_error(&self, e: &anyhow::Error) {
        if let Some(f) = &self.handle_watcher_error {
            f(e)
        }
    }

    pub fn initial_file_sync_error(&self, e: &anyhow::Error) {
        if let Some(f) = &self.handle_initial_file_sync_error {
            f(e)
        }
    }

    pub fn new(enable_watcher: bool) -> Self {
        CommonConfig {
            enable_logging: Arc::new(AtomicBool::new(false)),
            enable_watcher,
            handle_watcher_error: None,
            handle_initial_file_sync_error: None,
        }
    }
}

/// Files containing any of the tokens.
pub(crate) fn files_by_tokens(
    index: Arc<dyn Index>,
    tokens: Vec<String>,
) -> impl Stream<Item = FileAddress> {
    async_stream::stream! {
        for token in tokens {
            for fa in index.find_files_by_token(&token).await {
                yield fa;
            }
        }
    }
}

macro_rules! impl_config_builder {
    ($ty:ty) => {
        impl $ty {
            /// Called when the watcher fails (it is restarted afterwards).
            pub fn on_watcher_error(
                mut self,
                f: impl Fn(&anyhow::Error) + Send + Sync + 'static,
            ) -> Self {
                self.common.handle_watcher_error = Some(std::sync::Arc::new(f));
                self
            }

            /// Called when the initial directory walk fails (it is retried afterwards).
            pub fn on_initial_file_sync_error(
                mut self,
                f: impl Fn(&anyhow::Error) + Send + Sync + 'static,
            ) -> Self {
                self.common.handle_initial_file_sync_error = Some(std::sync::Arc::new(f));
                self
            }

            pub fn with_watcher(mut self, enable_watcher: bool) -> Self {
                self.common.enable_watcher = enable_watcher;
                self
            }
        }
    };
}

macro_rules! impl_config_common_methods {
    () => {
        fn enable_logging(&self) -> &std::sync::Arc<std::sync::atomic::AtomicBool> {
            &self.common.enable_logging
        }

        fn enable_watcher(&self) -> bool {
            self.common.enable_watcher
        }
    };
}

pub(crate) use {impl_config_builder, impl_config_common_methods};
