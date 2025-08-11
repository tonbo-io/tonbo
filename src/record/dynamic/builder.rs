use std::sync::Arc;

use arrow::{
    array::{
        make_builder, ArrayBuilder, ArrayRef, BinaryBuilder, BinaryDictionaryBuilder,
        BooleanBuilder, Date32Builder, Date64Builder, FixedSizeBinaryBuilder,
        FixedSizeBinaryDictionaryBuilder, Float32Builder, Float64Builder, Int16Builder,
        Int32Builder, Int64Builder, Int8Builder, LargeBinaryBuilder, LargeBinaryDictionaryBuilder,
        LargeStringBuilder, LargeStringDictionaryBuilder, ListBuilder, PrimitiveDictionaryBuilder,
        StringBuilder, StringDictionaryBuilder, Time32MillisecondBuilder, Time32SecondBuilder,
        Time64MicrosecondBuilder, Time64NanosecondBuilder, TimestampMicrosecondBuilder,
        TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
        UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
    },
    datatypes::{
        DataType, Field, FieldRef, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
        Int8Type, TimeUnit, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
};
use fusio_log::Encode;

use super::ValueRef;
use crate::record::{AsValue, DictionaryKeyType, Key};

macro_rules! append_opt {
    ($builder:expr, $builder_ty:ty, $opt:expr) => {{
        let bd = NestedBuilder::as_builder_mut::<$builder_ty>($builder);
        bd.append_option($opt);
    }};
}
macro_rules! append_val {
    ($builder:expr, $builder_ty:ty, $val:expr) => {{
        let bd = NestedBuilder::as_builder_mut::<$builder_ty>($builder);
        bd.append_value($val).unwrap();
    }};
}

// Array builder for nested types
pub struct NestedBuilder {
    builder: Box<dyn ArrayBuilder>,
    field: FieldRef,
    bytes_written: usize,
}

impl NestedBuilder {
    /// Create a new [`NestedBuilder``] with the specified field and capacity
    pub fn with_capacity(field: FieldRef, capacity: usize) -> Self {
        // FIXME: make_builder only support limited dictionary type
        let builder = make_builder(field.data_type(), capacity);
        Self {
            builder,
            field,
            bytes_written: 0,
        }
    }

    /// Cast the builder to a specific type
    fn as_builder_mut<T>(builder: &mut dyn ArrayBuilder) -> &mut T
    where
        T: ArrayBuilder,
    {
        builder.as_any_mut().downcast_mut::<T>().unwrap()
    }

    /// Append a value to the builder
    fn append_value_inner(
        builder: &mut Box<dyn ArrayBuilder>,
        data_type: &DataType,
        value: ValueRef,
    ) {
        match data_type {
            DataType::Boolean => append_opt!(builder, BooleanBuilder, value.as_bool_opt().copied()),
            DataType::Int8 => append_opt!(builder, Int8Builder, value.as_i8_opt().copied()),
            DataType::Int16 => append_opt!(builder, Int16Builder, value.as_i16_opt().copied()),
            DataType::Int32 => append_opt!(builder, Int32Builder, value.as_i32_opt().copied()),
            DataType::Int64 => append_opt!(builder, Int64Builder, value.as_i64_opt().copied()),
            DataType::UInt8 => append_opt!(builder, UInt8Builder, value.as_u8_opt().copied()),
            DataType::UInt16 => append_opt!(builder, UInt16Builder, value.as_u16_opt().copied()),
            DataType::UInt32 => append_opt!(builder, UInt32Builder, value.as_u32_opt().copied()),
            DataType::UInt64 => append_opt!(builder, UInt64Builder, value.as_u64_opt().copied()),
            DataType::Float32 => append_opt!(builder, Float32Builder, value.as_f32_opt().copied()),
            DataType::Float64 => append_opt!(builder, Float64Builder, value.as_f64_opt().copied()),
            DataType::Timestamp(time_unit, _) => {
                match time_unit {
                    TimeUnit::Second => {
                        append_opt!(builder, TimestampSecondBuilder, value.as_i64_opt().copied())
                    }
                    TimeUnit::Millisecond => append_opt!(
                        builder,
                        TimestampMillisecondBuilder,
                        value.as_i64_opt().copied()
                    ),
                    TimeUnit::Microsecond => append_opt!(
                        builder,
                        TimestampMicrosecondBuilder,
                        value.as_i64_opt().copied()
                    ),
                    TimeUnit::Nanosecond => append_opt!(
                        builder,
                        TimestampNanosecondBuilder,
                        value.as_i64_opt().copied()
                    ),
                };
            }
            DataType::Date32 => append_opt!(builder, Date32Builder, value.as_i32_opt().copied()),
            DataType::Date64 => append_opt!(builder, Date64Builder, value.as_i64_opt().copied()),
            DataType::Time32(time_unit) => match time_unit {
                TimeUnit::Second => {
                    append_opt!(builder, Time32SecondBuilder, value.as_i32_opt().copied())
                }
                TimeUnit::Millisecond => append_opt!(
                    builder,
                    Time32MillisecondBuilder,
                    value.as_i32_opt().copied()
                ),
                _ => unreachable!(),
            },
            DataType::Time64(time_unit) => match time_unit {
                TimeUnit::Microsecond => append_opt!(
                    builder,
                    Time64MicrosecondBuilder,
                    value.as_i64_opt().copied()
                ),
                TimeUnit::Nanosecond => append_opt!(
                    builder,
                    Time64NanosecondBuilder,
                    value.as_i64_opt().copied()
                ),
                _ => unreachable!(),
            },
            DataType::Binary => append_opt!(builder, BinaryBuilder, value.as_bytes_opt()),
            DataType::FixedSizeBinary(_) => {
                append_val!(builder, FixedSizeBinaryBuilder, value.as_bytes())
            }
            DataType::LargeBinary => append_opt!(builder, LargeBinaryBuilder, value.as_bytes_opt()),
            DataType::Utf8 => append_opt!(
                builder,
                StringBuilder,
                value.as_string_opt().map(|v| v.to_string())
            ),
            DataType::LargeUtf8 => append_opt!(
                builder,
                LargeStringBuilder,
                value.as_string_opt().map(|v| v.to_string())
            ),
            DataType::List(field) => {
                let bd = Self::as_builder_mut::<ListBuilder<Box<dyn ArrayBuilder>>>(builder);

                match value {
                    ValueRef::Null => bd.append_null(),
                    ValueRef::List(_, vec) => {
                        vec.into_iter().for_each(|v| {
                            Self::append_value_inner(bd.values(), field.data_type(), v.as_key_ref())
                        });
                        bd.append(true);
                    }
                    _ => unreachable!(),
                }
            }
            DataType::Dictionary(key_type, value_type) => {
                macro_rules! append_dict {
                    ($builder:expr, $key_dt:ty, $val_ty:expr, $opt:expr) => {{
                        match $val_ty.as_ref() {
                            DataType::Int8 => {
                                append_opt!($builder, PrimitiveDictionaryBuilder<$key_dt, Int8Type>, $opt.as_i8_opt().copied());
                            }
                            DataType::Int16 => {
                                append_opt!($builder, PrimitiveDictionaryBuilder<$key_dt, Int16Type>, $opt.as_i16_opt().copied());
                            }
                            DataType::Int32 => {
                                append_opt!($builder, PrimitiveDictionaryBuilder<$key_dt, Int32Type>, $opt.as_i32_opt().copied());
                            }
                            DataType::Int64 => {
                                append_opt!($builder, PrimitiveDictionaryBuilder<$key_dt, Int64Type>, $opt.as_i64_opt().copied());
                            }
                            DataType::UInt8 => {
                                append_opt!($builder, PrimitiveDictionaryBuilder<$key_dt, UInt8Type>, $opt.as_u8_opt().copied());
                            }
                            DataType::UInt16 => {
                                append_opt!($builder, PrimitiveDictionaryBuilder<$key_dt, UInt16Type>, $opt.as_u16_opt().copied());
                            }
                            DataType::UInt32 => {
                                append_opt!($builder, PrimitiveDictionaryBuilder<$key_dt, UInt32Type>, $opt.as_u32_opt().copied());
                            }
                            DataType::UInt64 => {
                                append_opt!($builder, PrimitiveDictionaryBuilder<$key_dt, UInt64Type>, $opt.as_u64_opt().copied());
                            }
                            DataType::Float32 => {
                                append_opt!($builder, PrimitiveDictionaryBuilder<$key_dt, Float32Type>, $opt.as_f32_opt().copied());
                            }
                            DataType::Float64 => {
                                append_opt!($builder, PrimitiveDictionaryBuilder<$key_dt, Float64Type>, $opt.as_f64_opt().copied());
                            }
                            DataType::Utf8 => {
                                append_opt!(
                                    $builder,
                                    StringDictionaryBuilder<$key_dt>,
                                    $opt.as_string_opt().map(|v| v.to_string())
                                );
                            }
                            DataType::LargeUtf8 => {
                                append_opt!(
                                    $builder,
                                    LargeStringDictionaryBuilder<$key_dt>,
                                    $opt.as_string_opt().map(|v| v.to_string())
                                );
                            }
                            DataType::Binary => {
                                append_opt!(
                                    $builder,
                                    BinaryDictionaryBuilder<$key_dt>,
                                    $opt.as_bytes_opt()
                                );
                            }
                            DataType::LargeBinary => {
                                append_opt!(
                                    $builder,
                                    LargeBinaryDictionaryBuilder<$key_dt>,
                                    $opt.as_bytes_opt()
                                );
                            }
                            DataType::FixedSizeBinary(_) => {
                                let bd = NestedBuilder::as_builder_mut::<FixedSizeBinaryDictionaryBuilder<$key_dt>>(
                                    $builder,
                                );
                                bd.append_value($opt.as_bytes());
                            }
                            t => unreachable!("Dictionary value type {t:?} is not currently supported"),
                        }
                    }};
                }

                match key_type.as_ref() {
                    DataType::Int8 => match value {
                        ValueRef::Null => {
                            append_dict!(builder, Int8Type, value_type, value);
                        }
                        ValueRef::Dictionary(_, value) => {
                            append_dict!(builder, Int8Type, value_type, value)
                        }
                        _ => unreachable!("Unsupported value type: {value:?}"),
                    },
                    DataType::Int16 => match value {
                        ValueRef::Null => {
                            append_dict!(builder, Int16Type, value_type, value);
                        }
                        ValueRef::Dictionary(_, value) => {
                            append_dict!(builder, Int16Type, value_type, value)
                        }
                        _ => unreachable!("Unsupported value type: {value:?}"),
                    },
                    DataType::Int32 => match value {
                        ValueRef::Null => {
                            append_dict!(builder, Int32Type, value_type, value);
                        }
                        ValueRef::Dictionary(_, value) => {
                            append_dict!(builder, Int32Type, value_type, value)
                        }
                        _ => unreachable!("Unsupported value type: {value:?}"),
                    },
                    DataType::Int64 => match value {
                        ValueRef::Null => {
                            append_dict!(builder, Int64Type, value_type, value);
                        }
                        ValueRef::Dictionary(_, value) => {
                            append_dict!(builder, Int64Type, value_type, value)
                        }
                        _ => unreachable!("Unsupported value type: {value:?}"),
                    },
                    DataType::UInt8 => match value {
                        ValueRef::Null => {
                            append_dict!(builder, UInt8Type, value_type, value);
                        }
                        ValueRef::Dictionary(_, value) => {
                            append_dict!(builder, UInt8Type, value_type, value)
                        }
                        _ => unreachable!("Unsupported value type: {value:?}"),
                    },
                    DataType::UInt16 => match value {
                        ValueRef::Null => {
                            append_dict!(builder, UInt16Type, value_type, value);
                        }
                        ValueRef::Dictionary(_, value) => {
                            append_dict!(builder, UInt16Type, value_type, value)
                        }
                        _ => unreachable!("Unsupported value type: {value:?}"),
                    },
                    DataType::UInt32 => match value {
                        ValueRef::Null => {
                            append_dict!(builder, UInt32Type, value_type, value);
                        }
                        ValueRef::Dictionary(_, value) => {
                            append_dict!(builder, UInt32Type, value_type, value)
                        }
                        _ => unreachable!("Unsupported value type: {value:?}"),
                    },
                    DataType::UInt64 => match value {
                        ValueRef::Null => {
                            append_dict!(builder, UInt64Type, value_type, value);
                        }
                        ValueRef::Dictionary(_, value) => {
                            append_dict!(builder, UInt64Type, value_type, value)
                        }
                        _ => unreachable!("Unsupported value type: {value:?}"),
                    },
                    _ => unreachable!("Unsupported key type: {key_type:?}"),
                }
            }
            DataType::Struct(_) => todo!(),
            _ => unimplemented!(),
        }
    }

    /// Append a value to this [`NestedBuilder``]
    /// For performance, this method does not check the validity of the value. But the value must
    /// match the field's data type.
    pub fn append_value(&mut self, value: ValueRef) {
        self.bytes_written += value.size();
        Self::append_value_inner(&mut self.builder, self.field.data_type(), value);
    }

    /// Append a null value
    pub fn append_null(&mut self) {
        self.append_value(ValueRef::Null);
    }

    /// Append a default value to this [`NestedBuilder`]
    pub fn append_default(&mut self) {
        let data_type = self.field.data_type().clone();
        let value = Self::default_value(&data_type);
        self.append_value(value);
    }

    #[allow(unused)]
    /// Get the field associated with this builder
    pub fn field(&self) -> &Field {
        &self.field
    }

    /// Return the number of bytes written to the builder
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }
}

impl NestedBuilder {
    fn default_value(data_type: &DataType) -> ValueRef<'_> {
        match data_type {
            DataType::Null => ValueRef::Null,
            DataType::Boolean => ValueRef::Boolean(bool::default()),
            DataType::Int8 => ValueRef::Int8(i8::default()),
            DataType::Int16 => ValueRef::Int16(i16::default()),
            DataType::Int32 => ValueRef::Int32(i32::default()),
            DataType::Int64 => ValueRef::Int64(i64::default()),
            DataType::UInt8 => ValueRef::UInt8(u8::default()),
            DataType::UInt16 => ValueRef::UInt16(u16::default()),
            DataType::UInt32 => ValueRef::UInt32(u32::default()),
            DataType::UInt64 => ValueRef::UInt64(u64::default()),
            DataType::Float32 => ValueRef::Float32(f32::default()),
            DataType::Float64 => ValueRef::Float64(f64::default()),
            DataType::Timestamp(time_unit, _) => {
                ValueRef::Timestamp(i64::default(), (*time_unit).into())
            }
            DataType::Date32 => ValueRef::Date32(i32::default()),
            DataType::Date64 => ValueRef::Date64(i64::default()),
            DataType::Time32(time_unit) => ValueRef::Time32(i32::default(), (*time_unit).into()),
            DataType::Time64(time_unit) => ValueRef::Time64(i64::default(), (*time_unit).into()),
            DataType::Binary | DataType::LargeBinary => ValueRef::Binary(&[]),
            DataType::FixedSizeBinary(w) => ValueRef::FixedSizeBinary(&[], *w as u32),
            DataType::Utf8 | DataType::LargeUtf8 => ValueRef::String(""),
            DataType::List(field) => ValueRef::List(field.data_type(), vec![]),
            DataType::Dictionary(key_type, value_type) => {
                let value = Self::default_value(value_type.as_ref());
                match key_type.as_ref() {
                    DataType::Int8 => {
                        ValueRef::Dictionary(DictionaryKeyType::Int8, Box::new(value))
                    }
                    DataType::Int16 => {
                        ValueRef::Dictionary(DictionaryKeyType::Int16, Box::new(value))
                    }
                    DataType::Int32 => {
                        ValueRef::Dictionary(DictionaryKeyType::Int32, Box::new(value))
                    }
                    DataType::Int64 => {
                        ValueRef::Dictionary(DictionaryKeyType::Int64, Box::new(value))
                    }
                    DataType::UInt8 => {
                        ValueRef::Dictionary(DictionaryKeyType::UInt8, Box::new(value))
                    }
                    DataType::UInt16 => {
                        ValueRef::Dictionary(DictionaryKeyType::UInt16, Box::new(value))
                    }
                    DataType::UInt32 => {
                        ValueRef::Dictionary(DictionaryKeyType::UInt32, Box::new(value))
                    }
                    DataType::UInt64 => {
                        ValueRef::Dictionary(DictionaryKeyType::UInt64, Box::new(value))
                    }
                    _ => unreachable!("Unsupported key type: {key_type:?}"),
                }
            }
            _ => unreachable!("Unsupported data type: {data_type:?}"),
        }
    }
}

impl ArrayBuilder for NestedBuilder {
    fn len(&self) -> usize {
        self.builder.len()
    }

    fn finish(&mut self) -> ArrayRef {
        self.builder.finish()
    }

    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.builder.finish_cloned())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, AsArray, PrimitiveArray},
        datatypes::{DataType, Field, Int32Type},
    };

    use super::*;
    use crate::record::Value;

    #[test]
    fn test_append_list_value() {
        let city_data_type = DataType::List(Arc::new(Field::new("city", DataType::Int32, false)));
        let cities_data_type = DataType::List(Arc::new(Field::new(
            "cities",
            city_data_type.clone(),
            false,
        )));
        let field = Arc::new(Field::new("countries", cities_data_type.clone(), false));
        let mut builder = NestedBuilder::with_capacity(field, 2);

        builder.append_value(ValueRef::List(
            &cities_data_type,
            vec![
                Arc::new(Value::List(city_data_type.clone(), vec![])),
                Arc::new(Value::List(
                    cities_data_type.clone(),
                    vec![Arc::new(Value::Int32(1))],
                )),
                Arc::new(Value::List(
                    city_data_type.clone(),
                    vec![Arc::new(Value::Int32(2)), Arc::new(Value::Int32(3))],
                )),
                Arc::new(Value::List(
                    city_data_type.clone(),
                    vec![
                        Arc::new(Value::Int32(4)),
                        Arc::new(Value::Int32(5)),
                        Arc::new(Value::Int32(6)),
                    ],
                )),
            ],
        ));

        let array = builder.finish();

        let list_array = array.as_list::<i32>();
        assert_eq!(list_array.len(), 1);
        let i32_list_array = list_array.value(0);
        let i32_lists = i32_list_array.as_list::<i32>();
        assert_eq!(i32_lists.len(), 4);

        assert_eq!(
            i32_lists.value(0).as_primitive::<Int32Type>(),
            &PrimitiveArray::<Int32Type>::from_iter_values(vec![])
        );
        assert_eq!(
            i32_lists.value(1).as_primitive::<Int32Type>(),
            &PrimitiveArray::<Int32Type>::from_iter_values(vec![1])
        );
        assert_eq!(
            i32_lists.value(2).as_primitive::<Int32Type>(),
            &PrimitiveArray::<Int32Type>::from_iter_values(vec![2, 3])
        );
        assert_eq!(
            i32_lists.value(3).as_primitive::<Int32Type>(),
            &PrimitiveArray::<Int32Type>::from_iter_values(vec![4, 5, 6])
        );
    }

    #[test]
    fn test_append_list_null_value() {
        let city_data_type = DataType::List(Arc::new(Field::new("city", DataType::Binary, true)));
        let cities_data_type =
            DataType::List(Arc::new(Field::new("cities", city_data_type.clone(), true)));

        let field = Arc::new(Field::new("countries", cities_data_type.clone(), true));
        let mut builder = NestedBuilder::with_capacity(field, 2);

        builder.append_value(ValueRef::Null);
        builder.append_value(ValueRef::List(
            &cities_data_type,
            vec![Arc::new(Value::Null), Arc::new(Value::Null)],
        ));
        builder.append_value(ValueRef::List(
            &cities_data_type,
            vec![
                Arc::new(Value::Null),
                Arc::new(Value::List(
                    city_data_type.clone(),
                    vec![Arc::new(Value::Null)],
                )),
                Arc::new(Value::List(
                    city_data_type.clone(),
                    vec![Arc::new(Value::Null), Arc::new(Value::Null)],
                )),
            ],
        ));

        let array = builder.finish();
        let list_array = array.as_list::<i32>();

        let first_list_array = list_array.value(0);
        assert!(first_list_array.is_empty());

        let second_list_array = list_array.value(1);
        let second_cities_list_array = second_list_array.as_list::<i32>();
        assert_eq!(second_cities_list_array.len(), 2);
        assert!(second_cities_list_array.value(0).is_empty());
        assert!(second_cities_list_array.value(1).is_empty());

        let third_list_array = list_array.value(2);
        let third_cities_list_array = third_list_array.as_list::<i32>();
        assert_eq!(third_cities_list_array.len(), 3);
        assert!(third_cities_list_array.value(0).is_empty());
        let cities_binary_array = third_cities_list_array.value(1);
        assert_eq!(cities_binary_array.len(), 1);
        assert!(cities_binary_array.is_null(0));
        let cities_binary_array = third_cities_list_array.value(2);
        assert_eq!(cities_binary_array.len(), 2);
        assert!(cities_binary_array.is_null(0));
        assert!(cities_binary_array.is_null(1));
    }

    #[test]
    fn test_append_nullable_list_value() {
        let city_data_type = DataType::List(Arc::new(Field::new("city", DataType::Utf8, true)));
        let cities_data_type =
            DataType::List(Arc::new(Field::new("cities", city_data_type.clone(), true)));
        let field = Arc::new(Field::new("countries", cities_data_type.clone(), true));
        let mut builder = NestedBuilder::with_capacity(field, 2);

        builder.append_value(ValueRef::Null);
        builder.append_value(ValueRef::List(
            &cities_data_type,
            vec![
                Arc::new(Value::List(
                    city_data_type.clone(),
                    vec![Arc::new(Value::Null)],
                )),
                Arc::new(Value::List(
                    city_data_type.clone(),
                    vec![Arc::new(Value::String("NewYork".to_string()))],
                )),
            ],
        ));
        builder.append_value(ValueRef::List(
            &cities_data_type,
            vec![
                Arc::new(Value::List(
                    city_data_type.clone(),
                    vec![Arc::new(Value::String("Shanghai".to_string()))],
                )),
                Arc::new(Value::List(
                    city_data_type.clone(),
                    vec![Arc::new(Value::String("Shenzhen".to_string()))],
                )),
                Arc::new(Value::List(
                    city_data_type.clone(),
                    vec![
                        Arc::new(Value::String("Wuhan".to_string())),
                        Arc::new(Value::Null),
                        Arc::new(Value::String("Yichang".to_string())),
                    ],
                )),
            ],
        ));

        let array = builder.finish();

        let list_array = array.as_list::<i32>();
        assert_eq!(list_array.len(), 3);

        let inner_list_array = list_array.value(0);
        assert_eq!(inner_list_array.len(), 0);

        let inner_list_array = list_array.value(1);
        assert_eq!(inner_list_array.len(), 2);
        let array = inner_list_array.as_list::<i32>();

        let list = array.value(0);
        assert_eq!(list.len(), 1);
        assert!(list.is_null(0));

        let list = array.value(1);
        assert_eq!(list.len(), 1);
        let city_array = list.as_string::<i32>();
        assert_eq!(city_array.value(0), "NewYork");

        let inner_list_array = list_array.value(2);
        assert_eq!(inner_list_array.len(), 3);
        let array = inner_list_array.as_list::<i32>();

        let list = array.value(0);
        assert_eq!(list.len(), 1);
        let city_array = list.as_string::<i32>();
        assert_eq!(city_array.value(0), "Shanghai");

        let list = array.value(1);
        assert_eq!(list.len(), 1);
        let city_array = list.as_string::<i32>();
        assert_eq!(city_array.value(0), "Shenzhen");

        let list = array.value(2);
        assert_eq!(list.len(), 3);
        let city_array = list.as_string::<i32>();
        assert_eq!(city_array.value(0), "Wuhan");
        assert!(city_array.is_null(1));
        assert_eq!(city_array.value(2), "Yichang");
    }

    #[test]
    fn test_dict_append_value() {
        let city_data_type =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        let field = Arc::new(Field::new("countries", city_data_type.clone(), false));
        let mut builder = NestedBuilder::with_capacity(field, 2);

        builder.append_value(ValueRef::Dictionary(
            crate::record::DictionaryKeyType::Int8,
            Box::new(ValueRef::String("America")),
        ));
        builder.append_value(ValueRef::Dictionary(
            crate::record::DictionaryKeyType::Int8,
            Box::new(ValueRef::String("Canada")),
        ));
        builder.append_value(ValueRef::Dictionary(
            crate::record::DictionaryKeyType::Int8,
            Box::new(ValueRef::String("America")),
        ));
        builder.append_value(ValueRef::Dictionary(
            crate::record::DictionaryKeyType::Int8,
            Box::new(ValueRef::String("America")),
        ));
        let array = builder.finish();
        let dict_array = array.as_dictionary::<Int8Type>();
        assert_eq!(dict_array.len(), 4);
        let values = dict_array.values().as_string::<i32>();
        assert_eq!(values.len(), 2);
        assert_eq!(values.value(0), "America");
        assert_eq!(values.value(1), "Canada");

        let keys = dict_array.keys();
        assert_eq!(keys.values(), &[0, 1, 0, 0]);
    }

    #[test]
    fn test_dict_append_null_value() {
        let city_data_type =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Binary));
        let field = Arc::new(Field::new("countries", city_data_type.clone(), false));
        let mut builder = NestedBuilder::with_capacity(field, 2);

        builder.append_value(ValueRef::Null);
        builder.append_value(ValueRef::Dictionary(
            crate::record::DictionaryKeyType::Int8,
            Box::new(ValueRef::Null),
        ));

        let array = builder.finish();
        assert_eq!(array.len(), 2);
        assert!(array.is_null(0));
        assert!(array.is_null(1));
    }

    #[test]
    fn test_dict_append_default_value() {
        let city_data_type =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::LargeUtf8));
        let field = Arc::new(Field::new("countries", city_data_type.clone(), false));
        let mut builder = NestedBuilder::with_capacity(field, 2);

        builder.append_default();
        builder.append_default();

        let array = builder.finish();
        let dict_array = array.as_dictionary::<Int8Type>();
        assert_eq!(dict_array.len(), 2);
        let values = dict_array.values().as_string::<i64>();
        assert_eq!(values.len(), 1);
        assert_eq!(values.value(0), "");

        let keys = dict_array.keys();
        assert_eq!(keys.values(), &[0, 0]);
    }
}
