use std::{
    any::Any,
    hash::{Hash, Hasher},
    sync::Arc,
};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use fusio_log::{Decode, Encode};

use crate::{
    datatype::DataType,
    key::{Key, KeyRef, TimeUnit, Value},
};

/// Number of seconds in a day
pub const SECONDS_IN_DAY: i64 = 86_400;
/// Number of milliseconds in a second
pub const MILLISECONDS: i64 = 1_000;
/// Number of microseconds in a second
pub const MICROSECONDS: i64 = 1_000_000;
/// Number of nanoseconds in a second
pub const NANOSECONDS: i64 = 1_000_000_000;

#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Date(pub(crate) i32);

macro_rules! make_time_type {
    ($struct_name:ident, $time_ty:ty) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $struct_name {
            pub(crate) time: $time_ty,
            pub(crate) unit: TimeUnit,
        }

        impl $struct_name {
            pub fn new(time: $time_ty, unit: TimeUnit) -> Self {
                Self { time, unit }
            }

            pub fn value(&self) -> $time_ty {
                self.time
            }
        }

        impl<'r> KeyRef<'r> for $struct_name {
            type Key = $struct_name;

            fn to_key(self) -> Self::Key {
                self
            }
        }

        impl Decode for $struct_name {
            async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
            where
                R: fusio::SeqRead,
            {
                let time = <$time_ty>::decode(reader).await?;
                let unit = match u8::decode(reader).await? {
                    0 => TimeUnit::Second,
                    1 => TimeUnit::Millisecond,
                    2 => TimeUnit::Microsecond,
                    3 => TimeUnit::Nanosecond,
                    _ => unreachable!(),
                };
                Ok(Self { time, unit })
            }
        }

        impl Encode for $struct_name {
            async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
            where
                W: fusio::Write,
            {
                self.time.encode(writer).await?;
                match self.unit {
                    TimeUnit::Second => 0u8.encode(writer).await?,
                    TimeUnit::Millisecond => 1u8.encode(writer).await?,
                    TimeUnit::Microsecond => 2u8.encode(writer).await?,
                    TimeUnit::Nanosecond => 3u8.encode(writer).await?,
                };
                Ok(())
            }

            fn size(&self) -> usize {
                self.time.size() + 1
            }
        }
        impl PartialEq for $struct_name {
            fn eq(&self, other: &Self) -> bool {
                self.unit.factor() as $time_ty * self.time
                    == other.unit.factor() as $time_ty * other.time
            }
        }

        impl Eq for $struct_name {}

        impl PartialOrd for $struct_name {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for $struct_name {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                (self.unit.factor() as $time_ty * self.time)
                    .cmp(&(other.unit.factor() as $time_ty * other.time))
            }
        }

        impl Hash for $struct_name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                (self.unit.factor() as $time_ty * self.time).hash(state);
            }
        }
    };
}

make_time_type!(Time32, i32);
make_time_type!(Time64, i64);

macro_rules! make_date_type {
    ($struct_name:ident, $date_ty:ty, $array_ty:ty) => {
        #[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash)]
        pub struct $struct_name(pub(crate) $date_ty);

        impl $struct_name {
            pub fn new(date: $date_ty) -> Self {
                Self(date)
            }

            pub fn value(&self) -> $date_ty {
                self.0
            }
        }

        impl Key for $struct_name {
            type Ref<'r> = $struct_name;

            fn as_key_ref(&self) -> Self::Ref<'_> {
                *self
            }

            // fn to_arrow_datum(&self) -> std::sync::Arc<dyn arrow::array::Datum> {
            //     Arc::new(<$array_ty>::new_scalar(self.0))
            // }

            fn as_value(&self) -> &dyn Value {
                self
            }
        }
        impl<'r> KeyRef<'r> for $struct_name {
            type Key = $struct_name;

            fn to_key(self) -> Self::Key {
                self
            }
        }
        impl Value for $struct_name {
            fn data_type(&self) -> DataType {
                DataType::$struct_name
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn size_of(&self) -> usize {
                self.0.size()
            }

            // fn to_arrow_datum(&self) -> std::sync::Arc<dyn arrow::array::Datum> {
            //     Arc::new(<$array_ty>::new_scalar(self.0))
            // }

            fn is_none(&self) -> bool {
                false
            }

            fn is_some(&self) -> bool {
                false
            }

            fn clone_arc(&self) -> super::ValueRef {
                Arc::new(*self)
            }
        }

        impl Value for Option<$struct_name> {
            fn data_type(&self) -> DataType {
                DataType::$struct_name
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn size_of(&self) -> usize {
                match self {
                    Some(v) => 1 + v.size_of(),
                    None => 1,
                }
            }

            fn is_none(&self) -> bool {
                self.is_none()
            }

            fn is_some(&self) -> bool {
                self.is_some()
            }

            fn clone_arc(&self) -> super::ValueRef {
                Arc::new(*self)
            }
        }

        impl Decode for $struct_name {
            async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
            where
                R: fusio::SeqRead,
            {
                let day = <$date_ty>::decode(reader).await?;

                Ok(Self(day))
            }
        }

        impl Encode for $struct_name {
            async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
            where
                W: fusio::Write,
            {
                self.0.encode(writer).await
            }

            fn size(&self) -> usize {
                self.0.size()
            }
        }

        impl From<$struct_name> for $date_ty {
            fn from(date: $struct_name) -> Self {
                date.0
            }
        }

        impl From<$date_ty> for $struct_name {
            fn from(date: $date_ty) -> Self {
                Self(date)
            }
        }

        impl From<&$struct_name> for $date_ty {
            fn from(date: &$struct_name) -> $date_ty {
                date.0
            }
        }
    };
}

make_date_type!(Date32, i32, Date32Array);
make_date_type!(Date64, i64, Date64Array);

impl Key for Time32 {
    type Ref<'r> = Time32;

    fn as_key_ref(&self) -> Self::Ref<'_> {
        *self
    }

    fn as_value(&self) -> &dyn Value {
        self
    }
}

impl Key for Time64 {
    type Ref<'r> = Time64;

    fn as_key_ref(&self) -> Self::Ref<'_> {
        *self
    }

    fn as_value(&self) -> &dyn Value {
        self
    }
}

impl Time32 {
    pub fn new_seconds(time: i32) -> Self {
        Self {
            time,
            unit: TimeUnit::Second,
        }
    }

    pub fn new_millis(time: i32) -> Self {
        Self {
            time,
            unit: TimeUnit::Millisecond,
        }
    }

    /// converts [`Time32`] to [`chrono::NaiveTime`]
    pub fn to_naive_time(&self) -> Option<NaiveTime> {
        match self.unit {
            TimeUnit::Second => NaiveTime::from_num_seconds_from_midnight_opt(self.time as u32, 0),
            TimeUnit::Millisecond => NaiveTime::from_num_seconds_from_midnight_opt(
                (self.time as i64 / MILLISECONDS) as u32,
                (self.time as i64 % MILLISECONDS * MICROSECONDS) as u32,
            ),
            TimeUnit::Microsecond | TimeUnit::Nanosecond => {
                unreachable!("microsecond and nanosecond is not supported")
            }
        }
    }

    /// converts [`chrono::NaiveTime`] to [`Time32`] in second
    pub fn from_naive_time_second(v: NaiveTime) -> Self {
        let time = v.num_seconds_from_midnight() as i32;

        Self {
            time,
            unit: TimeUnit::Second,
        }
    }

    /// converts [`chrono::NaiveTime`] to [`Time32`] in millsecond
    pub fn from_naive_time_millisecond(v: NaiveTime) -> Self {
        let time = (v.num_seconds_from_midnight() as i64 * MILLISECONDS
            + v.nanosecond() as i64 * MILLISECONDS / NANOSECONDS) as i32;
        Self {
            time,
            unit: TimeUnit::Second,
        }
    }
}

impl Value for Time32 {
    fn data_type(&self) -> DataType {
        match &self.unit {
            TimeUnit::Second => DataType::Time32(TimeUnit::Second),
            TimeUnit::Millisecond => DataType::Time32(TimeUnit::Millisecond),
            TimeUnit::Microsecond => unreachable!(),
            TimeUnit::Nanosecond => unreachable!(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn size_of(&self) -> usize {
        4
    }

    fn is_none(&self) -> bool {
        false
    }

    fn is_some(&self) -> bool {
        false
    }

    fn clone_arc(&self) -> super::ValueRef {
        Arc::new(*self)
    }
}

impl Value for Option<Time32> {
    fn data_type(&self) -> DataType {
        match &self {
            Some(v) => v.data_type(),
            None => DataType::Time32(TimeUnit::Second),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn size_of(&self) -> usize {
        match self {
            Some(v) => v.size_of() + 1,
            None => 1,
        }
    }

    fn is_none(&self) -> bool {
        false
    }

    fn is_some(&self) -> bool {
        false
    }

    fn clone_arc(&self) -> super::ValueRef {
        Arc::new(*self)
    }
}

impl Time64 {
    pub fn new_micros(time: i64) -> Self {
        Self {
            time,
            unit: TimeUnit::Microsecond,
        }
    }

    pub fn new_nanos(time: i64) -> Self {
        Self {
            time,
            unit: TimeUnit::Nanosecond,
        }
    }

    /// converts [`Time64`] to [`chrono::NaiveTime`]
    pub fn to_naive_time(&self) -> Option<NaiveTime> {
        match self.unit {
            TimeUnit::Second | TimeUnit::Millisecond => {
                unreachable!("second and millisecond is not supported")
            }
            TimeUnit::Microsecond => NaiveTime::from_num_seconds_from_midnight_opt(
                (self.time / MICROSECONDS) as u32,
                (self.time % MICROSECONDS * MILLISECONDS) as u32,
            ),
            TimeUnit::Nanosecond => NaiveTime::from_num_seconds_from_midnight_opt(
                (self.time / NANOSECONDS) as u32,
                (self.time % NANOSECONDS) as u32,
            ),
        }
    }

    /// converts [`chrono::NaiveTime`] to [`Time32`] in second
    pub fn from_naive_time_microsecond(v: NaiveTime) -> Self {
        let time = v.num_seconds_from_midnight() as i64 * MICROSECONDS
            + v.nanosecond() as i64 * MICROSECONDS / NANOSECONDS;

        Self {
            time,
            unit: TimeUnit::Microsecond,
        }
    }

    /// converts [`chrono::NaiveTime`] to [`Time32`] in millsecond
    pub fn from_naive_time_nanosecond(v: NaiveTime) -> Self {
        let time = v.num_seconds_from_midnight() as i64 * NANOSECONDS + v.nanosecond() as i64;

        Self {
            time,
            unit: TimeUnit::Nanosecond,
        }
    }
}

impl Value for Time64 {
    fn data_type(&self) -> DataType {
        match &self.unit {
            TimeUnit::Second => unreachable!(),
            TimeUnit::Millisecond => unreachable!(),
            TimeUnit::Microsecond => DataType::Time64(TimeUnit::Microsecond),
            TimeUnit::Nanosecond => DataType::Time64(TimeUnit::Nanosecond),
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

    fn clone_arc(&self) -> super::ValueRef {
        Arc::new(*self)
    }
}

impl Value for Option<Time64> {
    fn data_type(&self) -> DataType {
        match self {
            Some(v) => v.data_type(),
            None => DataType::Time64(TimeUnit::Second),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn size_of(&self) -> usize {
        match self {
            Some(v) => 1 + v.size_of(),
            None => 1,
        }
    }

    fn is_none(&self) -> bool {
        self.is_none()
    }

    fn is_some(&self) -> bool {
        self.is_some()
    }

    fn clone_arc(&self) -> super::ValueRef {
        Arc::new(*self)
    }
}

impl Date32 {
    /// converts [`Date32`] to [`chrono::NaiveDate`] with January 1, 1970 being day 1.
    pub fn to_naive_date(&self) -> Option<NaiveDate> {
        Some(
            DateTime::from_timestamp(self.0 as i64 * SECONDS_IN_DAY, 0)?
                .naive_utc()
                .date(),
        )
    }

    /// converts [`Date32`] to [`chrono::NaiveDateTime`] with January 1, 1970 being day 1.
    pub fn to_naive_datetime(&self) -> Option<NaiveDateTime> {
        Some(DateTime::from_timestamp(self.0 as i64 * SECONDS_IN_DAY, 0)?.naive_utc())
    }
}

impl Date64 {
    pub fn to_naive_date(&self) -> Option<NaiveDate> {
        Some(
            DateTime::from_timestamp(self.0 / MILLISECONDS, 0)?
                .naive_utc()
                .date(),
        )
    }

    /// converts [`Date64`] to [`chrono::NaiveDateTime`]
    pub fn to_naive_datetime(&self) -> Option<NaiveDateTime> {
        Some(DateTime::from_timestamp_millis(self.0)?.naive_utc())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Datelike, Duration, Utc};

    use super::*;

    #[test]
    fn test_time_to_naive_time() {
        let datetime = Utc::now();
        let expected = datetime.time();
        let time64 = Time64::from_naive_time_nanosecond(expected);
        assert_eq!(time64.to_naive_time(), Some(expected));
    }

    #[test]
    fn test_date_to_naive_date() {
        let datetime = Utc::now();
        {
            let days = datetime.num_days_from_ce();
            let expected =
                NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(days as i64);
            let date32 = Date32::new(days);
            assert_eq!(date32.to_naive_date(), Some(expected));
        }

        {
            let millseconds = datetime.timestamp_millis();
            let date64 = Date64::new(millseconds);

            let expected =
                NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::milliseconds(millseconds);
            assert_eq!(date64.to_naive_date(), Some(expected));
        }
    }

    #[test]
    fn test_date_to_naive_datetime() {
        let datetime = Utc::now();
        {
            let days = datetime.num_days_from_ce();
            let expected =
                NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(days as i64);
            let date32 = Date32::new(days);
            assert_eq!(date32.to_naive_datetime(), expected.and_hms_opt(0, 0, 0));
        }

        {
            let millseconds = datetime.timestamp_millis();
            let date64 = Date64::new(millseconds);

            let expected =
                NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::milliseconds(millseconds);
            let res_milli = millseconds.rem_euclid(86_400_000) as u32;
            let hour = res_milli / 3600000;
            let min = res_milli.rem_euclid(3600000) / 60_000;
            let sec = res_milli.rem_euclid(60_000).div_euclid(1000);
            let milli = res_milli % 1000;

            assert_eq!(
                date64.to_naive_datetime(),
                expected.and_hms_milli_opt(hour, min, sec, milli)
            );
        }
    }
}
