use std::{
    any::Any,
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::Arc,
};

use chrono::{DateTime, NaiveDateTime};
use fusio_log::{Decode, Encode};

use super::{Date32, Date64, Key, KeyRef, Time32, Time64, Value, ValueRef};
use crate::{
    datatype::DataType,
    key::{MICROSECONDS, MILLISECONDS, NANOSECONDS, SECONDS_IN_DAY},
};

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl TimeUnit {
    pub(crate) fn factor(&self) -> i64 {
        match self {
            TimeUnit::Second => 1_000_000_000,
            TimeUnit::Millisecond => 1_000_000,
            TimeUnit::Microsecond => 1_000,
            TimeUnit::Nanosecond => 1,
        }
    }
}

impl From<arrow::datatypes::TimeUnit> for TimeUnit {
    fn from(value: arrow::datatypes::TimeUnit) -> Self {
        match value {
            arrow::datatypes::TimeUnit::Second => TimeUnit::Second,
            arrow::datatypes::TimeUnit::Millisecond => TimeUnit::Millisecond,
            arrow::datatypes::TimeUnit::Microsecond => TimeUnit::Microsecond,
            arrow::datatypes::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }
}

impl From<&arrow::datatypes::TimeUnit> for TimeUnit {
    fn from(value: &arrow::datatypes::TimeUnit) -> Self {
        match value {
            arrow::datatypes::TimeUnit::Second => TimeUnit::Second,
            arrow::datatypes::TimeUnit::Millisecond => TimeUnit::Millisecond,
            arrow::datatypes::TimeUnit::Microsecond => TimeUnit::Microsecond,
            arrow::datatypes::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }
}

impl From<TimeUnit> for arrow::datatypes::TimeUnit {
    fn from(value: TimeUnit) -> Self {
        match value {
            TimeUnit::Second => arrow::datatypes::TimeUnit::Second,
            TimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
            TimeUnit::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
        }
    }
}

impl Debug for TimeUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeUnit::Second => write!(f, "Second"),
            TimeUnit::Millisecond => write!(f, "Millisecond"),
            TimeUnit::Microsecond => write!(f, "Microsecond"),
            TimeUnit::Nanosecond => write!(f, "Nanosecond"),
        }
    }
}

/// Timestamp without timezone
#[derive(Debug, Clone, Copy)]
pub struct Timestamp {
    pub(crate) ts: i64,
    pub(crate) unit: TimeUnit,
}

impl Key for Timestamp {
    type Ref<'r> = Timestamp;
    fn as_key_ref(&self) -> Self::Ref<'_> {
        *self
    }

    fn as_value(&self) -> &dyn Value {
        self
    }
}

impl<'r> KeyRef<'r> for Timestamp {
    type Key = Timestamp;

    fn to_key(self) -> Self::Key {
        self
    }
}

impl Value for Timestamp {
    fn data_type(&self) -> DataType {
        match &self.unit {
            TimeUnit::Second => DataType::Timestamp(TimeUnit::Second),
            TimeUnit::Millisecond => DataType::Timestamp(TimeUnit::Millisecond),
            TimeUnit::Microsecond => DataType::Timestamp(TimeUnit::Microsecond),
            TimeUnit::Nanosecond => DataType::Timestamp(TimeUnit::Nanosecond),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn size_of(&self) -> usize {
        8
    }

    fn is_none(&self) -> bool {
        false
    }

    fn is_some(&self) -> bool {
        false
    }

    fn clone_arc(&self) -> ValueRef {
        Arc::new(*self)
    }
}

impl Value for Option<Timestamp> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        match self {
            Some(ts) => ts.data_type(),
            None => DataType::Timestamp(TimeUnit::Second),
        }
    }

    fn size_of(&self) -> usize {
        8
    }

    fn is_none(&self) -> bool {
        self.is_none()
    }

    fn is_some(&self) -> bool {
        self.is_some()
    }

    fn clone_arc(&self) -> ValueRef {
        Arc::new(*self)
    }
}

impl Decode for Timestamp {
    async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
    where
        R: fusio::SeqRead,
    {
        let ts = i64::decode(reader).await?;
        let unit = match u8::decode(reader).await? {
            0 => TimeUnit::Second,
            1 => TimeUnit::Millisecond,
            2 => TimeUnit::Microsecond,
            3 => TimeUnit::Nanosecond,
            _ => unreachable!(),
        };
        Ok(Timestamp { ts, unit })
    }
}

impl Encode for Timestamp {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: fusio::Write,
    {
        self.ts.encode(writer).await?;
        match self.unit {
            TimeUnit::Second => 0u8.encode(writer).await?,
            TimeUnit::Millisecond => 1u8.encode(writer).await?,
            TimeUnit::Microsecond => 2u8.encode(writer).await?,
            TimeUnit::Nanosecond => 3u8.encode(writer).await?,
        };
        Ok(())
    }

    fn size(&self) -> usize {
        self.ts.size() + 1
    }
}

impl PartialEq for Timestamp {
    fn eq(&self, other: &Self) -> bool {
        self.ts * self.unit.factor() == other.ts * other.unit.factor()
    }
}

impl Eq for Timestamp {}

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.ts * self.unit.factor()).cmp(&(other.ts * other.unit.factor()))
    }
}

impl Hash for Timestamp {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.ts * self.unit.factor()).hash(state);
    }
}

impl Timestamp {
    pub fn new(ts: i64, unit: TimeUnit) -> Self {
        Self { ts, unit }
    }

    pub fn new_seconds(ts: i64) -> Self {
        Self {
            ts,
            unit: TimeUnit::Second,
        }
    }

    pub fn new_millis(ts: i64) -> Self {
        Self {
            ts,
            unit: TimeUnit::Millisecond,
        }
    }
    pub fn new_micros(ts: i64) -> Self {
        Self {
            ts,
            unit: TimeUnit::Microsecond,
        }
    }

    pub fn new_nanos(ts: i64) -> Self {
        Self {
            ts,
            unit: TimeUnit::Nanosecond,
        }
    }

    pub fn timestamp(&self) -> i64 {
        match self.unit {
            TimeUnit::Second => self.ts,
            TimeUnit::Millisecond => self.ts.saturating_mul(1000),
            TimeUnit::Microsecond => self.ts.saturating_mul(1_000_000),
            TimeUnit::Nanosecond => self.ts.saturating_mul(1_000_000_000),
        }
    }

    pub fn timestamp_millis(&self) -> i64 {
        match self.unit {
            TimeUnit::Second => self.ts.saturating_div(1_000),
            TimeUnit::Millisecond => self.ts,
            TimeUnit::Microsecond => self.ts.saturating_mul(1_000),
            TimeUnit::Nanosecond => self.ts.saturating_mul(1_000_000),
        }
    }

    pub fn timestamp_micros(&self) -> i64 {
        match self.unit {
            TimeUnit::Second => self.ts.saturating_div(1_000_000),
            TimeUnit::Millisecond => self.ts.saturating_div(1_000),
            TimeUnit::Microsecond => self.ts,
            TimeUnit::Nanosecond => self.ts.saturating_mul(1_000),
        }
    }

    pub fn timestamp_nanos(&self) -> i64 {
        match self.unit {
            TimeUnit::Second => self.ts.saturating_div(1_000_000_000),
            TimeUnit::Millisecond => self.ts.saturating_div(1_000_000),
            TimeUnit::Microsecond => self.ts.saturating_div(1_000),
            TimeUnit::Nanosecond => self.ts,
        }
    }

    /// build [`Timestamp`] from [`NaiveDateTime`]
    pub fn from_naive_date_time(naive: NaiveDateTime, unit: TimeUnit) -> Option<Self> {
        let utc = naive.and_utc();
        match unit {
            TimeUnit::Second => {
                let ts = utc.timestamp();
                Some(Timestamp { ts, unit })
            }
            TimeUnit::Millisecond => {
                let millis = utc.timestamp().checked_mul(MILLISECONDS)?;
                millis
                    .checked_add(utc.timestamp_subsec_millis() as i64)
                    .map(|ts| Timestamp { ts, unit })
            }
            TimeUnit::Microsecond => {
                let micros = utc.timestamp().checked_mul(MICROSECONDS)?;
                micros
                    .checked_add(utc.timestamp_subsec_micros() as i64)
                    .map(|ts| Timestamp { ts, unit })
            }
            TimeUnit::Nanosecond => {
                let nanos = utc.timestamp().checked_mul(NANOSECONDS)?;
                nanos
                    .checked_add(utc.timestamp_subsec_nanos() as i64)
                    .map(|ts| Timestamp { ts, unit })
            }
        }
    }

    /// convert [`Timestamp`] to [`NaiveDateTime`]
    pub fn to_naive_date_time(&self) -> Option<NaiveDateTime> {
        match self.unit {
            TimeUnit::Second => Some(DateTime::from_timestamp(self.ts, 0)?.naive_utc()),
            TimeUnit::Millisecond => Some(DateTime::from_timestamp_millis(self.ts)?.naive_utc()),
            TimeUnit::Microsecond => Some(DateTime::from_timestamp_micros(self.ts)?.naive_utc()),
            TimeUnit::Nanosecond => Some(DateTime::from_timestamp_nanos(self.ts).naive_utc()),
        }
    }

    /// convert [`Timestamp`] to [`Time32`] in second
    pub fn to_time32_second(&self) -> Option<Time32> {
        let datetime = self.to_naive_date_time()?;
        Some(Time32::from_naive_time_second(datetime.time()))
    }

    /// convert [`Timestamp`] to [`Time32`] in millisecond
    pub fn to_time32_millisecond(&self) -> Option<Time32> {
        let datetime = self.to_naive_date_time()?;
        Some(Time32::from_naive_time_millisecond(datetime.time()))
    }

    /// convert [`Timestamp`] to [`Time64`] in microsecond
    pub fn to_time64_microsecond(&self) -> Option<Time64> {
        let datetime = self.to_naive_date_time()?;
        Some(Time64::from_naive_time_microsecond(datetime.time()))
    }

    /// convert [`Timestamp`] to [`Time64`] in nanosecond
    pub fn to_time64_nanosecond(&self) -> Option<Time64> {
        let datetime = self.to_naive_date_time()?;
        Some(Time64::from_naive_time_nanosecond(datetime.time()))
    }

    /// convert [`Timestamp`] to [`Date32`] in days
    pub fn to_date32(&self) -> Date32 {
        let days = match self.unit {
            TimeUnit::Second => self.ts / SECONDS_IN_DAY,
            TimeUnit::Millisecond => self.ts / SECONDS_IN_DAY / MILLISECONDS,
            TimeUnit::Microsecond => self.ts / SECONDS_IN_DAY / MICROSECONDS,
            TimeUnit::Nanosecond => self.ts / SECONDS_IN_DAY / NANOSECONDS,
        };
        Date32::new(days as i32)
    }

    /// convert [`Timestamp`] to [`Date64`] in millisecond
    pub fn to_date64(&self) -> Date64 {
        let millis = match self.unit {
            TimeUnit::Second => self.ts * MILLISECONDS,
            TimeUnit::Millisecond => self.ts,
            TimeUnit::Microsecond => self.ts / MILLISECONDS,
            TimeUnit::Nanosecond => self.ts / MICROSECONDS,
        };
        Date64::new(millis)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, SeekFrom};

    use chrono::Utc;
    use fusio_log::{Decode, Encode};
    use tokio::io::AsyncSeekExt;

    use super::*;

    #[test]
    fn test_timestamp_eq() {
        let val = Timestamp::new_millis(123);
        let val2 = Timestamp::new_millis(123);
        let val3 = Timestamp::new_seconds(123);
        let val4 = Timestamp::new_micros(123000);
        let val5 = Timestamp::new_micros(123001);
        assert_eq!(val, val2);
        assert_ne!(val, val3);
        assert_eq!(val, val4);
        assert_ne!(val, val5);
    }

    #[tokio::test]
    async fn test_timestamp_encode_decode() {
        let ts = Timestamp::new_millis(1717507203412);
        let mut bytes = Vec::new();
        let mut buf = Cursor::new(&mut bytes);
        ts.encode(&mut buf).await.unwrap();

        buf.seek(SeekFrom::Start(0)).await.unwrap();
        let ts2 = Timestamp::decode(&mut buf).await.unwrap();

        assert_eq!(ts, ts2);
    }

    #[test]
    fn test_timestamp_to_naive_date_time() {
        let datetime = Utc::now();
        let expected = datetime.naive_utc();

        // test nanoseconds
        let nanoseconds = datetime.timestamp_nanos_opt().unwrap();
        let ts = Timestamp::new_nanos(nanoseconds);
        assert_eq!(ts.to_naive_date_time(), Some(expected));

        // test microseconds
        let microseconds = datetime.timestamp_micros();
        let ts = Timestamp::new_micros(microseconds);
        assert_eq!(
            ts.to_naive_date_time(),
            Some(
                DateTime::from_timestamp_micros(microseconds)
                    .unwrap()
                    .naive_utc()
            ),
        );

        // test milliseconds
        let milliseconds = datetime.timestamp_millis();
        let ts = Timestamp::new_millis(milliseconds);
        assert_eq!(
            ts.to_naive_date_time(),
            Some(
                DateTime::from_timestamp_millis(milliseconds)
                    .unwrap()
                    .naive_utc()
            ),
        );

        // test seconds
        let seconds = datetime.timestamp();
        let ts = Timestamp::new_seconds(seconds);
        assert_eq!(
            ts.to_naive_date_time(),
            Some(DateTime::from_timestamp(seconds, 0).unwrap().naive_utc()),
        );
    }

    #[test]
    fn test_timestamp_from_naive_date_time() {
        let datetime = Utc::now();
        let utc = datetime.naive_utc();

        {
            let ts = Timestamp::from_naive_date_time(utc, TimeUnit::Nanosecond).unwrap();
            assert_eq!(
                ts.timestamp_nanos(),
                datetime.timestamp_nanos_opt().unwrap()
            );
        }
        {
            let ts = Timestamp::from_naive_date_time(utc, TimeUnit::Microsecond).unwrap();
            assert_eq!(ts.timestamp_micros(), datetime.timestamp_micros());
        }
        {
            let ts = Timestamp::from_naive_date_time(utc, TimeUnit::Millisecond).unwrap();
            assert_eq!(ts.timestamp_millis(), datetime.timestamp_millis());
        }
        {
            let ts = Timestamp::from_naive_date_time(utc, TimeUnit::Second).unwrap();
            assert_eq!(ts.timestamp(), datetime.timestamp());
        }
    }
}
