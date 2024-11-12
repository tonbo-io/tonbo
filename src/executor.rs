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

#[cfg(all(feature = "opfs", target_arch = "wasm32"))]
pub mod opfs {
    use std::future::Future;

    use wasm_bindgen::prelude::*;

    use super::Executor;

    #[derive(Debug)]
    #[wasm_bindgen]
    pub struct OpfsExecutor {}

    impl Default for OpfsExecutor {
        fn default() -> Self {
            Self {}
        }
    }

    impl OpfsExecutor {
        pub fn new() -> Self {
            Self {}
        }
    }

    impl Executor for OpfsExecutor {
        fn spawn<F>(&self, future: F)
        where
            F: Future<Output = ()> + Send + 'static,
        {
            wasm_bindgen_futures::spawn_local(future);
        }
    }
}
