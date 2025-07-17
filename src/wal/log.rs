use common::PrimaryKey;
use fusio::{SeqRead, Write};
use fusio_log::{Decode, Encode};

use crate::{record::Record, timestamp::Ts};

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum LogType {
    Full,
    First,
    Middle,
    Last,
}

impl From<u8> for LogType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Full,
            1 => Self::First,
            2 => Self::Middle,
            3 => Self::Last,
            _ => unreachable!(),
        }
    }
}

pub(crate) struct Log<R>
where
    R: Record,
{
    pub(crate) key: Ts<PrimaryKey>,
    pub(crate) value: Option<R>,
    pub(crate) log_type: Option<LogType>,
}

impl<R> Log<R>
where
    R: Record,
{
    pub(crate) fn new(ts: Ts<PrimaryKey>, value: Option<R>, log_type: Option<LogType>) -> Self {
        Self {
            key: ts,
            value,
            log_type,
        }
    }
}

impl<R> Encode for Log<R>
where
    R: Record,
{
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: Write,
    {
        if let Some(log_type) = self.log_type {
            (log_type as u8).encode(writer).await?;
        } else {
            unreachable!()
        }
        self.key.encode(writer).await.unwrap();
        self.value
            .as_ref()
            .map(R::as_record_ref)
            .encode(writer)
            .await
            .unwrap();
        Ok(())
    }

    fn size(&self) -> usize {
        self.key.size() + self.value.as_ref().map(R::as_record_ref).size() + size_of::<u8>()
    }
}

impl<Re> Decode for Log<Re>
where
    Re: Record,
{
    async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
    where
        R: SeqRead,
    {
        let log_type = LogType::from(u8::decode(reader).await?);
        let key = Ts::<PrimaryKey>::decode(reader).await.unwrap();
        let record = Option::<Re>::decode(reader).await.unwrap();

        Ok(Log::new(key, record, Some(log_type)))
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use common::PrimaryKey;
    use fusio_log::{Decode, Encode};
    use tokio::io::AsyncSeekExt;

    use crate::{
        timestamp::Ts,
        wal::log::{Log, LogType},
    };

    #[tokio::test]
    async fn encode_and_decode() {
        let entry: Log<String> = Log::new(
            Ts::new(
                PrimaryKey::new(vec![Arc::new("hello".to_string())]),
                1.into(),
            ),
            Some("hello".into()),
            Some(LogType::Middle),
        );
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);
        entry.encode(&mut cursor).await.unwrap();

        let decode_entry = {
            cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            Log::<String>::decode(&mut cursor).await.unwrap()
        };

        assert_eq!(entry.value, decode_entry.value);
        assert_eq!(entry.key, entry.key);
    }
}
