use super::{Date32, Date64, Time32, Time64, Timestamp, Value, F32, F64};

mod sealed {
    pub trait Sealed {}
}

pub trait AsValue: sealed::Sealed {
    fn as_boolean_opt(&self) -> &Option<bool>;

    fn as_boolean(&self) -> &bool;

    fn as_string(&self) -> &String;

    fn as_string_opt(&self) -> &Option<String>;

    fn as_i8(&self) -> &i8;

    fn as_i8_opt(&self) -> &Option<i8>;

    fn as_i16(&self) -> &i16;

    fn as_i16_opt(&self) -> &Option<i16>;

    fn as_i32(&self) -> &i32;

    fn as_i32_opt(&self) -> &Option<i32>;

    fn as_i64(&self) -> &i64;

    fn as_i64_opt(&self) -> &Option<i64>;

    fn as_u8(&self) -> &u8;

    fn as_u8_opt(&self) -> &Option<u8>;

    fn as_u16(&self) -> &u16;

    fn as_u16_opt(&self) -> &Option<u16>;

    fn as_u32(&self) -> &u32;

    fn as_u32_opt(&self) -> &Option<u32>;

    fn as_u64(&self) -> &u64;

    fn as_u64_opt(&self) -> &Option<u64>;

    fn as_f32(&self) -> &F32;

    fn as_f32_opt(&self) -> &Option<F32>;

    fn as_f64(&self) -> &F64;

    fn as_f64_opt(&self) -> &Option<F64>;

    fn as_bytes(&self) -> &Vec<u8>;

    fn as_bytes_opt(&self) -> &Option<Vec<u8>>;

    fn as_date32(&self) -> &Date32;

    fn as_date32_opt(&self) -> &Option<Date32>;

    fn as_date64(&self) -> &Date64;

    fn as_date64_opt(&self) -> &Option<Date64>;

    fn as_time32(&self) -> &Time32;

    fn as_time32_opt(&self) -> &Option<Time32>;

    fn as_time64(&self) -> &Time64;

    fn as_time64_opt(&self) -> &Option<Time64>;

    fn as_timestamp(&self) -> &Timestamp;

    fn as_timestamp_opt(&self) -> &Option<Timestamp>;
}

impl sealed::Sealed for dyn Value + '_ {}

impl AsValue for dyn Value {
    fn as_boolean_opt(&self) -> &Option<bool> {
        self.as_any().downcast_ref::<Option<bool>>().unwrap()
    }

    fn as_boolean(&self) -> &bool {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_string_opt(&self) -> &Option<String> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_string(&self) -> &String {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_i8(&self) -> &i8 {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_i8_opt(&self) -> &Option<i8> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_i16(&self) -> &i16 {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_i16_opt(&self) -> &Option<i16> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_i32(&self) -> &i32 {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_i32_opt(&self) -> &Option<i32> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_i64(&self) -> &i64 {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_i64_opt(&self) -> &Option<i64> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_u8(&self) -> &u8 {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_u8_opt(&self) -> &Option<u8> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_u16(&self) -> &u16 {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_u16_opt(&self) -> &Option<u16> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_u32(&self) -> &u32 {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_u32_opt(&self) -> &Option<u32> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_u64(&self) -> &u64 {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_u64_opt(&self) -> &Option<u64> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_f32(&self) -> &F32 {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_f32_opt(&self) -> &Option<F32> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_f64(&self) -> &F64 {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_f64_opt(&self) -> &Option<F64> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_bytes(&self) -> &Vec<u8> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_bytes_opt(&self) -> &Option<Vec<u8>> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_date32(&self) -> &Date32 {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_date32_opt(&self) -> &Option<Date32> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_date64(&self) -> &Date64 {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_date64_opt(&self) -> &Option<Date64> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_time32(&self) -> &Time32 {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_time32_opt(&self) -> &Option<Time32> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_time64(&self) -> &Time64 {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_time64_opt(&self) -> &Option<Time64> {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_timestamp(&self) -> &Timestamp {
        self.as_any().downcast_ref().unwrap()
    }

    fn as_timestamp_opt(&self) -> &Option<Timestamp> {
        self.as_any().downcast_ref().unwrap()
    }
}
