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
        tonbo_rpc_client::TonboRpcClient, ColumnDesc, Empty, GetReq, InsertReq, RemoveReq, ScanReq,
    },
    ClientError,
};

pub struct TonboSchema {
    pub desc: Vec<ColumnDesc>,
    pub primary_key_index: usize,
}

impl TonboSchema {
    pub fn primary_key_desc(&self) -> &ColumnDesc {
        &self.desc[self.primary_key_index]
    }

    pub fn len(&self) -> usize {
        self.desc.len()
    }

    pub fn is_empty(&self) -> bool {
        self.desc.is_empty()
    }
}

pub struct TonboClient {
    #[cfg(not(feature = "wasm"))]
    conn: TonboRpcClient<tonic::transport::Channel>,
    #[cfg(feature = "wasm")]
    conn: TonboRpcClient<tonic_web_wasm_client::Client>,
}

impl TonboClient {
    pub async fn connect(addr: String) -> Result<TonboClient, ClientError> {
        #[cfg(not(feature = "wasm"))]
        let conn = TonboRpcClient::connect(addr).await?;
        #[cfg(feature = "wasm")]
        let conn = {
            let client = tonic_web_wasm_client::Client::new(addr);
            TonboRpcClient::new(client)
        };
        Ok(TonboClient { conn })
    }

    pub async fn schema(&mut self) -> Result<TonboSchema, ClientError> {
        let resp = self.conn.schema(Request::new(Empty {})).await?.into_inner();

        Ok(TonboSchema {
            desc: resp.desc,
            primary_key_index: resp.primary_key_index as usize,
        })
    }

    pub async fn get(&mut self, column: Column) -> Result<Option<DynRecord>, ClientError> {
        let mut bytes = Vec::new();

        column.encode(&mut Cursor::new(&mut bytes)).await?;
        let resp = self.conn.get(Request::new(GetReq { key: bytes })).await?;

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
            .insert(Request::new(InsertReq { record: bytes }))
            .await?;

        Ok(())
    }

    pub async fn remove(&mut self, column: Column) -> Result<(), ClientError> {
        let mut bytes = Vec::new();

        column.encode(&mut Cursor::new(&mut bytes)).await?;
        let _ = self
            .conn
            .remove(Request::new(RemoveReq { key: bytes }))
            .await?;

        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), ClientError> {
        let _ = self.conn.flush(Request::new(Empty {})).await?;

        Ok(())
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
