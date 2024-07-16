use std::future::Future;

use crate::fs::Fs;

pub trait Executor: Fs {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static;
}

#[cfg(any(feature = "tokio", test))]
pub mod tokio {
    use std::future::Future;

    use tokio::runtime::Handle;

    use super::Executor;

    pub struct TokioExecutor {
        handle: Handle,
    }

    impl Default for TokioExecutor {
        fn default() -> Self {
            Self::new()
        }
    }

    impl TokioExecutor {
        pub fn new() -> Self {
            Self {
                handle: Handle::current(),
            }
        }
    }

    impl Executor for TokioExecutor {
        fn spawn<F>(&self, future: F)
        where
            F: Future<Output = ()> + Send + 'static,
        {
            self.handle.spawn(future);
        }
    }
}
