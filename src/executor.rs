use std::future::Future;

pub trait Executor {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static;
}

#[cfg(any(feature = "tokio", test))]
pub mod tokio {
    use std::future::Future;

    use super::Executor;

    pub struct TokioExecutor {
        tokio: tokio::runtime::Runtime,
    }

    impl TokioExecutor {
        pub fn new(tokio: tokio::runtime::Runtime) -> Self {
            Self { tokio }
        }
    }

    impl Executor for TokioExecutor {
        fn spawn<F>(&self, future: F)
        where
            F: Future<Output = ()> + Send + 'static,
        {
            self.tokio.spawn(future);
        }
    }
}
