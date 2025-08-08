use std::sync::Arc;

use arrow::{
    array::{
        make_builder, ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder,
        Date64Builder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder,
        Int32Builder, Int64Builder, Int8Builder, LargeBinaryBuilder, LargeStringBuilder,
        ListBuilder, StringBuilder, Time32MillisecondBuilder, Time32SecondBuilder,
        Time64MicrosecondBuilder, Time64NanosecondBuilder, TimestampMicrosecondBuilder,
        TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
        UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
    },
    datatypes::{DataType, Field, FieldRef, TimeUnit},
};
use fusio_log::Encode;

use super::ValueRef;
use crate::record::{AsValue, Key};

/// Array builder for nested types
pub struct NestedBuilder {
    builder: Box<dyn ArrayBuilder>,
    field: FieldRef,
    bytes_written: usize,
}

impl NestedBuilder {
    /// Create a new [`NestedBuilder``] with the specified field and capacity
    pub fn with_capacity(field: FieldRef, capacity: usize) -> Self {
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
            DataType::Boolean => {
                let bd = Self::as_builder_mut::<BooleanBuilder>(builder);
                bd.append_option(value.as_bool_opt().copied());
            }
            DataType::Int8 => {
                let bd = Self::as_builder_mut::<Int8Builder>(builder);
                bd.append_option(value.as_i8_opt().copied());
            }
            DataType::Int16 => {
                let bd = Self::as_builder_mut::<Int16Builder>(builder);
                bd.append_option(value.as_i16_opt().copied());
            }
            DataType::Int32 => {
                let bd = Self::as_builder_mut::<Int32Builder>(builder);
                bd.append_option(value.as_i32_opt().copied());
            }
            DataType::Int64 => {
                let bd = Self::as_builder_mut::<Int64Builder>(builder);
                bd.append_option(value.as_i64_opt().copied());
            }
            DataType::UInt8 => {
                let bd = Self::as_builder_mut::<UInt8Builder>(builder);
                bd.append_option(value.as_u8_opt().copied());
            }
            DataType::UInt16 => {
                let bd = Self::as_builder_mut::<UInt16Builder>(builder);
                bd.append_option(value.as_u16_opt().copied());
            }
            DataType::UInt32 => {
                let bd = Self::as_builder_mut::<UInt32Builder>(builder);
                bd.append_option(value.as_u32_opt().copied());
            }
            DataType::UInt64 => {
                let bd = Self::as_builder_mut::<UInt64Builder>(builder);
                bd.append_option(value.as_u64_opt().copied());
            }
            DataType::Float32 => {
                let bd = Self::as_builder_mut::<Float32Builder>(builder);
                bd.append_option(value.as_f32_opt().copied());
            }
            DataType::Float64 => {
                let bd = Self::as_builder_mut::<Float64Builder>(builder);
                bd.append_option(value.as_f64_opt().copied());
            }
            DataType::Timestamp(time_unit, _) => {
                match time_unit {
                    TimeUnit::Second => {
                        let bd = Self::as_builder_mut::<TimestampSecondBuilder>(builder);
                        bd.append_option(value.as_i64_opt().copied());
                    }
                    TimeUnit::Millisecond => {
                        let bd = Self::as_builder_mut::<TimestampMillisecondBuilder>(builder);
                        bd.append_option(value.as_i64_opt().copied());
                    }
                    TimeUnit::Microsecond => {
                        let bd = Self::as_builder_mut::<TimestampMicrosecondBuilder>(builder);
                        bd.append_option(value.as_i64_opt().copied());
                    }
                    TimeUnit::Nanosecond => {
                        let bd = Self::as_builder_mut::<TimestampNanosecondBuilder>(builder);
                        bd.append_option(value.as_i64_opt().copied());
                    }
                };
            }
            DataType::Date32 => {
                let bd = Self::as_builder_mut::<Date32Builder>(builder);
                bd.append_option(value.as_i32_opt().copied());
            }
            DataType::Date64 => {
                let bd = Self::as_builder_mut::<Date64Builder>(builder);
                bd.append_option(value.as_i64_opt().copied());
            }
            DataType::Time32(time_unit) => match time_unit {
                TimeUnit::Second => {
                    let bd = Self::as_builder_mut::<Time32SecondBuilder>(builder);
                    bd.append_option(value.as_i32_opt().copied());
                }
                TimeUnit::Millisecond => {
                    let bd = Self::as_builder_mut::<Time32MillisecondBuilder>(builder);
                    bd.append_option(value.as_i32_opt().copied());
                }
                _ => unreachable!(),
            },
            DataType::Time64(time_unit) => match time_unit {
                TimeUnit::Microsecond => {
                    let bd = Self::as_builder_mut::<Time64MicrosecondBuilder>(builder);
                    bd.append_option(value.as_i64_opt().copied());
                }
                TimeUnit::Nanosecond => {
                    let bd = Self::as_builder_mut::<Time64NanosecondBuilder>(builder);
                    bd.append_option(value.as_i64_opt().copied());
                }
                _ => unreachable!(),
            },
            DataType::Binary => {
                let bd = Self::as_builder_mut::<BinaryBuilder>(builder);
                bd.append_option(value.as_bytes_opt());
            }
            DataType::FixedSizeBinary(_) => {
                let bd = Self::as_builder_mut::<FixedSizeBinaryBuilder>(builder);
                bd.append_value(value.as_bytes()).unwrap();
            }
            DataType::LargeBinary => {
                let bd = Self::as_builder_mut::<LargeBinaryBuilder>(builder);
                bd.append_option(value.as_bytes_opt());
            }
            DataType::Utf8 => {
                let bd = Self::as_builder_mut::<StringBuilder>(builder);
                bd.append_option(value.as_string_opt().map(|v| v.to_string()));
            }
            DataType::LargeUtf8 => {
                let bd = Self::as_builder_mut::<LargeStringBuilder>(builder);
                bd.append_option(value.as_string_opt().map(|v| v.to_string()));
            }
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
        let value = match &data_type {
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
            DataType::Binary => ValueRef::Binary(&[]),
            DataType::FixedSizeBinary(w) => ValueRef::FixedSizeBinary(&[], *w as u32),
            DataType::Utf8 => ValueRef::String(""),
            DataType::List(field) => ValueRef::List(field.data_type(), vec![]),
            _ => unreachable!(),
        };
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
}
