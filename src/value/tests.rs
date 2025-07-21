#[cfg(test)]
mod value_tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::TimeUnit as ArrowTimeUnit,
    };

    use super::super::*;

    #[test]
    fn test_value_basic_types() {
        assert_eq!(Value::Null.data_type(), DataType::Null);
        assert_eq!(Value::Boolean(true).data_type(), DataType::Boolean);
        assert_eq!(Value::Int32(42).data_type(), DataType::Int32);
        assert_eq!(Value::Int64(42).data_type(), DataType::Int64);
        assert_eq!(
            Value::Float32(std::f32::consts::PI).data_type(),
            DataType::Float32
        );
        assert_eq!(
            Value::Float64(std::f64::consts::PI).data_type(),
            DataType::Float64
        );
        assert_eq!(Value::String("hello".into()).data_type(), DataType::Utf8);
        assert_eq!(Value::Binary(vec![1, 2, 3]).data_type(), DataType::Binary);
    }

    #[test]
    fn test_value_null_checks() {
        assert!(Value::Null.is_null());
        assert!(!Value::Boolean(false).is_null());
        assert!(!Value::Int32(0).is_null());
    }

    #[test]
    fn test_value_conversions() {
        assert!(Value::Boolean(true).as_bool().unwrap());
        assert_eq!(Value::Int8(42).as_i64().unwrap(), 42i64);
        assert_eq!(Value::Int32(42).as_i64().unwrap(), 42i64);
        assert!(
            (Value::Float32(std::f32::consts::PI).as_f64().unwrap() - std::f64::consts::PI).abs()
                < 0.0001
        );
        assert_eq!(Value::String("hello".into()).as_string().unwrap(), "hello");
        assert_eq!(
            Value::Binary(vec![1, 2, 3]).as_binary().unwrap(),
            &[1, 2, 3]
        );
    }

    #[test]
    fn test_value_conversion_errors() {
        assert!(Value::Int32(42).as_bool().is_err());
        assert!(Value::String("hello".into()).as_i64().is_err());
        assert!(Value::Boolean(true).as_string().is_err());
    }

    #[test]
    fn test_value_partial_ord() {
        assert!(Value::Null < Value::Int32(0));
        assert!(Value::Int32(1) < Value::Int32(2));
        assert!(Value::String("a".into()) < Value::String("b".into()));
        assert_eq!(
            Value::Float64(1.0).partial_cmp(&Value::Float64(1.0)),
            Some(std::cmp::Ordering::Equal)
        );
    }

    #[test]
    fn test_value_display() {
        assert_eq!(format!("{}", Value::Null), "NULL");
        assert_eq!(format!("{}", Value::Boolean(true)), "true");
        assert_eq!(format!("{}", Value::Int32(42)), "42");
        assert_eq!(format!("{}", Value::String("hello".into())), "hello");
        assert_eq!(
            format!("{}", Value::List(vec![Value::Int32(1), Value::Int32(2)])),
            "[1, 2]"
        );
    }

    #[test]
    fn test_time_unit_conversion() {
        assert_eq!(ArrowTimeUnit::from(TimeUnit::Second), ArrowTimeUnit::Second);
        assert_eq!(
            ArrowTimeUnit::from(TimeUnit::Millisecond),
            ArrowTimeUnit::Millisecond
        );
        assert_eq!(
            TimeUnit::from(ArrowTimeUnit::Microsecond),
            TimeUnit::Microsecond
        );
        assert_eq!(
            TimeUnit::from(ArrowTimeUnit::Nanosecond),
            TimeUnit::Nanosecond
        );
    }

    #[test]
    fn test_arrow_conversion() {
        let values = vec![1, 2, 3, 4, 5];
        let array = Int32Array::from(values);
        let array_ref = Arc::new(array) as arrow::array::ArrayRef;

        let value = Value::from_array_ref(&array_ref, 2).unwrap();
        assert_eq!(value, Value::Int32(3));
    }

    #[test]
    fn test_arrow_string_conversion() {
        let values = vec!["hello", "world", "test"];
        let array = StringArray::from(values);
        let array_ref = Arc::new(array) as arrow::array::ArrayRef;

        let value = Value::from_array_ref(&array_ref, 1).unwrap();
        assert_eq!(value, Value::String("world".into()));
    }

    #[test]
    fn test_values_to_arrow_array() {
        let values = vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)];

        let array = crate::value::conversion::values_to_arrow_array(&values).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.data_type(), &DataType::Int32);
    }

    #[test]
    fn test_struct_value() {
        let value = Value::Struct(vec![
            ("field1".into(), Value::Int32(42)),
            ("field2".into(), Value::String("test".into())),
        ]);

        if let DataType::Struct(fields) = value.data_type() {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name(), "field1");
            assert_eq!(fields[1].name(), "field2");
        } else {
            panic!("Expected Struct data type");
        }
    }

    #[test]
    fn test_list_value() {
        let value = Value::List(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]);

        if let DataType::List(field) = value.data_type() {
            assert_eq!(field.data_type(), &DataType::Int32);
        } else {
            panic!("Expected List data type");
        }
    }
}
