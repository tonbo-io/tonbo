use std::future::Future;

pub trait Executor {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static;
}

#[cfg(any(test, feature = "tokio"))]
pub mod tokio {
    use std::future::Future;

    use tokio::runtime::Handle;

    use super::Executor;

    #[derive(Debug)]
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
