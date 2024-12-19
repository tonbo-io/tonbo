use std::{
    io::Cursor,
    ops::Bound,
    pin::{pin, Pin},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use ::http::HeaderName;
use async_stream::stream;
use fusio::path::Path;
use futures_core::Stream;
use futures_util::StreamExt;
use tokio::sync::RwLock;
use tonbo::{
    executor::tokio::TokioExecutor,
    record::{Column, Datatype, DynRecord, Record},
    serdes::{Decode, Encode},
    DbOption, DB,
};
use tonic::{transport::Server, Code, Request, Response, Status};
use tonic_web::GrpcWebLayer;
use tower_http::cors::{AllowOrigin, CorsLayer};
use ttl_cache::TtlCache;

use crate::{
    proto::{
        tonbo_rpc_server::{TonboRpc, TonboRpcServer},
        CreateTableReq, Empty, FlushReq, GetReq, GetResp, InsertReq, RemoveReq, ScanReq, ScanResp,
    },
    ServerError,
};

const DEFAULT_EXPOSED_HEADERS: [&str; 3] =
    ["grpc-status", "grpc-message", "grpc-status-details-bin"];
const DEFAULT_ALLOW_HEADERS: [&str; 4] =
    ["x-grpc-web", "content-type", "x-user-agent", "grpc-timeout"];

const DB_DURATION: Duration = Duration::from_secs(20 * 60);
const DB_WRITE_LIMIT: usize = 100 * 1024 * 1024;

pub async fn service<P: AsRef<std::path::Path>>(
    addr: String,
    base_path: P,
) -> Result<(), ServerError> {
    let service = TonboService {
        base_path: base_path.as_ref().to_path_buf(),
        inner: RwLock::new(TtlCache::new(256)),
        write_limit: Default::default(),
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

struct TonboService {
    base_path: std::path::PathBuf,
    inner: RwLock<TtlCache<String, Arc<DB<DynRecord, TokioExecutor>>>>,
    write_limit: AtomicUsize,
}

#[tonic::async_trait]
impl TonboRpc for TonboService {
    async fn create_table(
        &self,
        request: Request<CreateTableReq>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let mut guard = self.inner.write().await;

        let pk_index = req.primary_key_index as usize;
        let mut descs = Vec::with_capacity(req.desc.len());

        for desc in req.desc {
            let datatype = crate::proto::Datatype::try_from(desc.ty)
                .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
            descs.push(tonbo::record::ColumnDesc::new(
                desc.name,
                Datatype::from(datatype),
                desc.is_nullable,
            ));
        }
        if let Some(db_instance) = guard.get(&req.table_name) {
            let instance = db_instance.instance();

            if instance.primary_key_index::<DynRecord>() != req.primary_key_index as usize {
                return Err(Status::new(
                    Code::Internal,
                    "`primary_key_index` does not match the existing schema".to_string(),
                ));
            }
            // FIXME: why `dyn_columns` returns not ColumnDesc
            for (column, column_desc) in instance.dyn_columns().iter().zip(descs.iter()) {
                if column.name != column_desc.name
                    || column.datatype != column_desc.datatype
                    || column.is_nullable != column_desc.is_nullable
                {
                    return Err(Status::new(
                        Code::Internal,
                        "`descs` does not match the existing schema".to_string(),
                    ));
                }
            }
        } else {
            let option = DbOption::with_path(
                Path::from_absolute_path(self.base_path.join(&req.table_name)).unwrap(),
                descs[pk_index].name.to_string(),
                pk_index,
            );
            let db: DB<DynRecord, TokioExecutor> =
                DB::with_schema(option, TokioExecutor::new(), descs, pk_index)
                    .await
                    .unwrap();
            guard.insert(req.table_name, Arc::new(db), DB_DURATION);
        }

        Ok(Response::new(Empty {}))
    }

    async fn get(&self, request: Request<GetReq>) -> Result<Response<GetResp>, Status> {
        let mut req = request.into_inner();
        let guard = self.inner.read().await;
        let Some(db_instance) = guard.get(&req.table_name) else {
            return Err(Status::new(Code::NotFound, "table not found"));
        };

        let key = Column::decode(&mut Cursor::new(&mut req.key))
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;

        let tuple = db_instance
            .get(&key, |e| Some(e.get().columns))
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        let record = if let Some(tuple) = tuple {
            let primary_key_index = db_instance.instance().primary_key_index::<DynRecord>() - 2;
            let mut bytes = Vec::new();
            let mut writer = Cursor::new(&mut bytes);
            (tuple.len() as u32)
                .encode(&mut writer)
                .await
                .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
            (primary_key_index as u32)
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
        let guard = self.inner.read().await;
        let Some(db_instance) = guard.get(&req.table_name) else {
            return Err(Status::new(Code::NotFound, "table not found"));
        };
        let primary_key_index = db_instance.instance().primary_key_index::<DynRecord>() - 2;
        let db = db_instance.clone();

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
        if self.write_limit.load(Ordering::Relaxed) > DB_WRITE_LIMIT {
            return Err(Status::new(
                Code::Internal,
                "write limit exceeded".to_string(),
            ));
        }
        let guard = self.inner.read().await;
        let Some(db_instance) = guard.get(&req.table_name) else {
            return Err(Status::new(Code::NotFound, "table not found"));
        };

        let record_size = record.size();
        db_instance
            .insert(record)
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        self.write_limit.fetch_add(record_size, Ordering::SeqCst);
        Ok(Response::new(Empty {}))
    }

    async fn remove(&self, request: Request<RemoveReq>) -> Result<Response<Empty>, Status> {
        let mut req = request.into_inner();
        let column = Column::decode(&mut Cursor::new(&mut req.key))
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        if self.write_limit.load(Ordering::Relaxed) > DB_WRITE_LIMIT {
            return Err(Status::new(
                Code::Internal,
                "write limit exceeded".to_string(),
            ));
        }
        let guard = self.inner.read().await;

        let Some(db_instance) = guard.get(&req.table_name) else {
            return Err(Status::new(Code::NotFound, "table not found"));
        };

        let column_size = column.size();
        db_instance
            .remove(column)
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        self.write_limit.fetch_add(column_size, Ordering::SeqCst);
        Ok(Response::new(Empty {}))
    }

    async fn flush(&self, request: Request<FlushReq>) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let guard = self.inner.read().await;
        let Some(db_instance) = guard.get(&req.table_name) else {
            return Err(Status::new(Code::NotFound, "table not found"));
        };

        db_instance
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

impl From<crate::proto::Datatype> for Datatype {
    fn from(value: crate::proto::Datatype) -> Self {
        match value {
            crate::proto::Datatype::Uint8 => Datatype::UInt8,
            crate::proto::Datatype::Uint16 => Datatype::UInt16,
            crate::proto::Datatype::Uint32 => Datatype::UInt32,
            crate::proto::Datatype::Uint64 => Datatype::UInt64,
            crate::proto::Datatype::Int8 => Datatype::Int8,
            crate::proto::Datatype::Int16 => Datatype::Int16,
            crate::proto::Datatype::Int32 => Datatype::Int32,
            crate::proto::Datatype::Int64 => Datatype::Int64,
            crate::proto::Datatype::String => Datatype::String,
            crate::proto::Datatype::Boolean => Datatype::Boolean,
            crate::proto::Datatype::Bytes => Datatype::Bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::server::service;

    #[tokio::test]
    async fn test_service() {
        let temp_dir = TempDir::new().unwrap();

        service("[::1]:50051".to_string(), temp_dir.path())
            .await
            .unwrap()
    }
}
