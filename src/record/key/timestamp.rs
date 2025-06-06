use std::sync::Arc;

use arrow::array::TimestampMillisecondArray;
use chrono::{DateTime, NaiveDateTime};
use fusio_log::{Decode, Encode};

use super::{Key, KeyRef};

/// Timestamp without timezone
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(pub(crate) i64);

impl Key for Timestamp {
    type Ref<'r> = Timestamp;
    fn as_key_ref(&self) -> Self::Ref<'_> {
        *self
    }

    fn to_arrow_datum(&self) -> std::sync::Arc<dyn arrow::array::Datum> {
        Arc::new(TimestampMillisecondArray::new_scalar(self.0))
    }
}

impl<'r> KeyRef<'r> for Timestamp {
    type Key = Timestamp;

    fn to_key(self) -> Self::Key {
        self
    }
}

impl Decode for Timestamp {
    type Error = fusio::Error;

    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: fusio::SeqRead,
    {
        Ok(Timestamp(i64::decode(reader).await?))
    }
}

impl Encode for Timestamp {
    type Error = fusio::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: fusio::Write,
    {
        self.0.encode(writer).await
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

impl From<i64> for Timestamp {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<Timestamp> for i64 {
    fn from(value: Timestamp) -> Self {
        value.0
    }
}

impl From<&Timestamp> for i64 {
    fn from(value: &Timestamp) -> Self {
        value.0
    }
}

impl Timestamp {
    /// build [`Timestamp`] from [`NaiveDateTime`]
    pub fn from_naive_date_time(naive: NaiveDateTime) -> Option<Self> {
        let utc = naive.and_utc();
        let millis = utc.timestamp().checked_mul(1_000)?;
        millis
            .checked_add(utc.timestamp_subsec_millis() as i64)
            .map(Timestamp)
    }

    /// convert [`Timestamp`] to [`NaiveDateTime`]
    pub fn to_naive_date_time(&self) -> Option<NaiveDateTime> {
        Some(DateTime::from_timestamp_millis(self.0)?.naive_utc())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, SeekFrom};

    use fusio_log::{Decode, Encode};
    use tokio::io::AsyncSeekExt;

    use super::*;

    #[tokio::test]
    async fn test_timestamp_encode_decode() {
        let ts = Timestamp(1717507203412);
        let mut bytes = Vec::new();
        let mut buf = Cursor::new(&mut bytes);
        ts.encode(&mut buf).await.unwrap();

        buf.seek(SeekFrom::Start(0)).await.unwrap();
        let ts2 = Timestamp::decode(&mut buf).await.unwrap();

        assert_eq!(ts, ts2);
    }
}
