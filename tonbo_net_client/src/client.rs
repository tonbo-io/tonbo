use std::{io::Cursor, ops::Bound};

use async_stream::stream;
use futures_core::Stream;
use futures_util::StreamExt;
use tonbo::{
    record::{Column, Datatype, DynRecord, Record},
    serdes::{Decode, Encode},
};
use tonic::Request;

use crate::{
    proto::{
        tonbo_rpc_client::TonboRpcClient, ColumnDesc, CreateTableReq, FlushReq, GetReq, InsertReq,
        RemoveReq, ScanReq,
    },
    ClientError,
};

pub struct TonboClient {
    #[cfg(not(feature = "wasm"))]
    conn: TonboRpcClient<tonic::transport::Channel>,
    #[cfg(feature = "wasm")]
    conn: TonboRpcClient<tonic_web_wasm_client::Client>,
    table_name: String,
    descs: Vec<tonbo::record::ColumnDesc>,
    pk_index: usize,
}

impl TonboClient {
    pub async fn connect(
        addr: String,
        table_name: String,
        descs: Vec<tonbo::record::ColumnDesc>,
        pk_index: usize,
    ) -> Result<TonboClient, ClientError> {
        #[cfg(not(feature = "wasm"))]
        let mut conn = TonboRpcClient::connect(addr).await?;
        #[cfg(feature = "wasm")]
        let mut conn = {
            let client = tonic_web_wasm_client::Client::new(addr);
            TonboRpcClient::new(client)
        };
        let desc = descs
            .iter()
            .map(|desc| {
                let ty: crate::proto::Datatype = desc.datatype.into();
                ColumnDesc {
                    name: desc.name.clone(),
                    ty: ty as i32,
                    is_nullable: desc.is_nullable,
                }
            })
            .collect::<Vec<_>>();
        conn.create_table(CreateTableReq {
            table_name: table_name.clone(),
            desc,
            primary_key_index: pk_index as u32,
        })
        .await?;

        Ok(TonboClient {
            conn,
            table_name,
            descs,
            pk_index,
        })
    }

    pub async fn get(&mut self, column: Column) -> Result<Option<DynRecord>, ClientError> {
        let mut bytes = Vec::new();

        column.encode(&mut Cursor::new(&mut bytes)).await?;
        let resp = self
            .conn
            .get(Request::new(GetReq {
                table_name: self.table_name.clone(),
                key: bytes,
            }))
            .await?;

        let Some(mut value) = resp.into_inner().record else {
            return Ok(None);
        };
        Ok(Some(DynRecord::decode(&mut Cursor::new(&mut value)).await?))
    }

    pub async fn scan(
        &mut self,
        min: Bound<Column>,
        max: Bound<Column>,
    ) -> Result<impl Stream<Item = Result<DynRecord, ClientError>>, ClientError> {
        let mut min_bytes = Vec::new();
        let mut max_bytes = Vec::new();
        min.encode(&mut Cursor::new(&mut min_bytes)).await?;
        max.encode(&mut Cursor::new(&mut max_bytes)).await?;

        let resp = self
            .conn
            .scan(Request::new(ScanReq {
                table_name: self.table_name.clone(),
                min: min_bytes,
                max: max_bytes,
            }))
            .await?;

        Ok(Box::pin(stream! {
            let mut stream = resp.into_inner();

            while let Some(result) = stream.next().await {
                let mut record = result?;
                yield Ok(DynRecord::decode(&mut Cursor::new(&mut record.record)).await?)
            }
        }))
    }

    pub async fn insert(&mut self, record: DynRecord) -> Result<(), ClientError> {
        let mut bytes = Vec::new();

        record
            .as_record_ref()
            .encode(&mut Cursor::new(&mut bytes))
            .await?;

        let _ = self
            .conn
            .insert(Request::new(InsertReq {
                table_name: self.table_name.clone(),
                record: bytes,
            }))
            .await?;

        Ok(())
    }

    pub async fn remove(&mut self, column: Column) -> Result<(), ClientError> {
        let mut bytes = Vec::new();

        column.encode(&mut Cursor::new(&mut bytes)).await?;
        let _ = self
            .conn
            .remove(Request::new(RemoveReq {
                table_name: self.table_name.clone(),
                key: bytes,
            }))
            .await?;

        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), ClientError> {
        let _ = self
            .conn
            .flush(Request::new(FlushReq {
                table_name: self.table_name.clone(),
            }))
            .await?;

        Ok(())
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
