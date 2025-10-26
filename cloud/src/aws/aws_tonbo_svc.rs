use std::sync::Arc;

use tonic::Response;

use crate::{
    aws::{AWSTonbo, AwsTonboRPC},
    gen::grpc,
    ScanRequest,
};

#[derive(Clone)]
pub struct AWSTonboSvc {
    inner: Arc<AWSTonbo>,
}

impl AWSTonboSvc {
    pub fn new(tonbo: Arc<AWSTonbo>) -> Self {
        AWSTonboSvc { inner: tonbo }
    }
}

#[tonic::async_trait]
impl AwsTonboRPC for AWSTonboSvc {
    async fn get_parquet_metadata(
        &self,
        request: tonic::Request<grpc::ScanRequest>,
    ) -> Result<tonic::Response<grpc::ParquetMetadata>, tonic::Status> {
        let tx = self.inner.tonbo.transaction().await;
        let scan_request = ScanRequest::from(request.into_inner());

        let (row_count, row_size) = self
            .inner
            .parquet_metadata(&tx, &scan_request)
            .await
            .unwrap();
        let meta = grpc::ParquetMetadata {
            row_count,
            row_size,
        };
        Ok(Response::new(meta))
    }
}
