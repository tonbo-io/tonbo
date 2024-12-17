use std::{
    io::Cursor,
    ops::Bound,
    pin::{pin, Pin},
    sync::Arc,
};

use ::http::HeaderName;
use async_stream::stream;
use futures_core::Stream;
use futures_util::StreamExt;
use itertools::Itertools;
use tonbo::{
    executor::tokio::TokioExecutor,
    record::{Column, Datatype, DynRecord},
    serdes::{Decode, Encode},
    DB,
};
use tonic::{transport::Server, Code, Request, Response, Status};
use tonic_web::GrpcWebLayer;
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::{
    proto::{
        tonbo_rpc_server::{TonboRpc, TonboRpcServer},
        ColumnDesc, Empty, GetReq, GetResp, InsertReq, RemoveReq, ScanReq, ScanResp, SchemaResp,
    },
    ServerError,
};

const DEFAULT_EXPOSED_HEADERS: [&str; 3] =
    ["grpc-status", "grpc-message", "grpc-status-details-bin"];
const DEFAULT_ALLOW_HEADERS: [&str; 4] =
    ["x-grpc-web", "content-type", "x-user-agent", "grpc-timeout"];

pub async fn service(addr: String, db: DB<DynRecord, TokioExecutor>) -> Result<(), ServerError> {
    let service = TonboService {
        inner: Arc::new(db),
    };
    let addr = addr.parse()?;
    println!("addr: {}", addr);

    Server::builder()
        .accept_http1(true)
        .layer(
            CorsLayer::new()
                .allow_origin(AllowOrigin::mirror_request())
                .expose_headers(
                    DEFAULT_EXPOSED_HEADERS
                        .iter()
                        .cloned()
                        .map(HeaderName::from_static)
                        .collect::<Vec<HeaderName>>(),
                )
                .allow_headers(
                    DEFAULT_ALLOW_HEADERS
                        .iter()
                        .cloned()
                        .map(HeaderName::from_static)
                        .collect::<Vec<HeaderName>>(),
                ),
        )
        .layer(GrpcWebLayer::new())
        .add_service(TonboRpcServer::new(service))
        .serve(addr)
        .await?;
    Ok(())
}

#[derive(Clone)]
struct TonboService {
    inner: Arc<DB<DynRecord, TokioExecutor>>,
}

impl TonboService {
    fn primary_key_index(&self) -> usize {
        self.inner.instance().primary_key_index::<DynRecord>() - 2
    }
}

#[tonic::async_trait]
impl TonboRpc for TonboService {
    async fn schema(&self, _: Request<Empty>) -> Result<Response<SchemaResp>, Status> {
        let instance = self.inner.instance();
        let desc = instance
            .dyn_columns()
            .iter()
            .map(|column| {
                let ty: crate::proto::Datatype = column.datatype.into();
                ColumnDesc {
                    name: column.name.clone(),
                    ty: ty as i32,
                    is_nullable: column.is_nullable,
                }
            })
            .collect_vec();

        Ok(Response::new(SchemaResp {
            desc,
            primary_key_index: instance.primary_key_index::<DynRecord>() as u32 - 2,
        }))
    }

    async fn get(&self, request: Request<GetReq>) -> Result<Response<GetResp>, Status> {
        let mut req = request.into_inner();
        let key = Column::decode(&mut Cursor::new(&mut req.key))
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;

        let tuple = self
            .inner
            .get(&key, |e| Some(e.get().columns))
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        let record = if let Some(tuple) = tuple {
            let mut bytes = Vec::new();
            let mut writer = Cursor::new(&mut bytes);
            (tuple.len() as u32)
                .encode(&mut writer)
                .await
                .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
            (self.primary_key_index() as u32)
                .encode(&mut writer)
                .await
                .map_err(|e| Status::new(Code::Internal, e.to_string()))?;

            for col in tuple.iter() {
                col.encode(&mut writer)
                    .await
                    .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
            }
            Some(bytes)
        } else {
            None
        };
        Ok(Response::new(GetResp { record }))
    }

    type ScanStream = Pin<Box<dyn Stream<Item = Result<ScanResp, Status>> + Send>>;

    async fn scan(&self, request: Request<ScanReq>) -> Result<Response<Self::ScanStream>, Status> {
        let mut req = request.into_inner();
        let db = self.inner.clone();
        let primary_key_index = self.primary_key_index();

        let stream = stream! {
            let min = Bound::<Column>::decode(&mut Cursor::new(&mut req.min)).await
                .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
            let max = Bound::<Column>::decode(&mut Cursor::new(&mut req.max)).await
                .map_err(|e| Status::new(Code::Internal, e.to_string()))?;

            let mut stream = pin!(db
                .scan((min.as_ref(), max.as_ref()), |e| Some(e.get().columns))
                .await);
            while let Some(entry) = stream.next().await {
                let Some(columns) = entry.map_err(|e| Status::new(Code::Internal, e.to_string()))? else { continue };
                let mut bytes = Vec::new();

                let mut writer = Cursor::new(&mut bytes);
                (columns.len() as u32)
                    .encode(&mut writer)
                    .await
                    .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
                (primary_key_index as u32)
                    .encode(&mut writer)
                    .await
                    .map_err(|e| Status::new(Code::Internal, e.to_string()))?;

                for col in columns.iter() {
                    col.encode(&mut writer)
                        .await
                        .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
                }

                yield Ok::<ScanResp, Status>(ScanResp { record: bytes });
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn insert(&self, request: Request<InsertReq>) -> Result<Response<Empty>, Status> {
        let mut req = request.into_inner();
        let record = DynRecord::decode(&mut Cursor::new(&mut req.record))
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;

        self.inner
            .insert(record)
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        Ok(Response::new(Empty {}))
    }

    async fn remove(&self, request: Request<RemoveReq>) -> Result<Response<Empty>, Status> {
        let mut req = request.into_inner();
        let column = Column::decode(&mut Cursor::new(&mut req.key))
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        self.inner
            .remove(column)
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        Ok(Response::new(Empty {}))
    }

    async fn flush(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        self.inner
            .flush()
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        Ok(Response::new(Empty {}))
    }
}

impl Into<crate::proto::Datatype> for Datatype {
    fn into(self) -> crate::proto::Datatype {
        match self {
            Datatype::UInt8 => crate::proto::Datatype::Uint8,
            Datatype::UInt16 => crate::proto::Datatype::Uint16,
            Datatype::UInt32 => crate::proto::Datatype::Uint32,
            Datatype::UInt64 => crate::proto::Datatype::Uint64,
            Datatype::Int8 => crate::proto::Datatype::Int8,
            Datatype::Int16 => crate::proto::Datatype::Int16,
            Datatype::Int32 => crate::proto::Datatype::Int32,
            Datatype::Int64 => crate::proto::Datatype::Int64,
            Datatype::String => crate::proto::Datatype::String,
            Datatype::Boolean => crate::proto::Datatype::Boolean,
            Datatype::Bytes => crate::proto::Datatype::Bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use fusio::path::Path;
    use tempfile::TempDir;
    use tonbo::{
        executor::tokio::TokioExecutor,
        record::{ColumnDesc, Datatype, DynRecord},
        DbOption, DB,
    };

    use crate::server::service;

    #[tokio::test]
    async fn test_service() {
        let temp_dir = TempDir::new().unwrap();
        let desc = vec![
            ColumnDesc::new("id".to_string(), Datatype::Int64, false),
            ColumnDesc::new("name".to_string(), Datatype::String, true),
            ColumnDesc::new("like".to_string(), Datatype::Int32, true),
        ];
        let option2 = DbOption::with_path(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            "id".to_string(),
            0,
        );
        let db: DB<DynRecord, TokioExecutor> =
            DB::with_schema(option2, TokioExecutor::new(), desc, 0)
                .await
                .unwrap();

        service("[::1]:50051".to_string(), db).await.unwrap()
    }
}
