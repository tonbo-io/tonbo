use std::{any::Any, sync::Arc};

use js_sys::{Object, Reflect, Uint8Array};
use tonbo::{
    cast_arc_value,
    datatype::DataType,
    record::{DynRecord, Value, ValueDesc},
    F32, F64,
};
use wasm_bindgen::{JsCast, JsValue};

fn to_col_value<T: 'static + Send + Sync>(value: T, primary: bool) -> Arc<dyn Any + Send + Sync> {
    match primary {
        true => Arc::new(value) as Arc<dyn Any + Send + Sync>,
        false => Arc::new(Some(value)) as Arc<dyn Any + Send + Sync>,
    }
}

fn none_value(datatype: DataType) -> Arc<dyn Any + Send + Sync> {
    match datatype {
        DataType::UInt8 => Arc::new(Option::<u8>::None),
        DataType::UInt16 => Arc::new(Option::<u16>::None),
        DataType::UInt32 => Arc::new(Option::<u32>::None),
        DataType::UInt64 => Arc::new(Option::<u64>::None),
        DataType::Int8 => Arc::new(Option::<i8>::None),
        DataType::Int16 => Arc::new(Option::<i16>::None),
        DataType::Int32 => Arc::new(Option::<i32>::None),
        DataType::Int64 => Arc::new(Option::<i64>::None),
        DataType::Float32 => Arc::new(Option::<F32>::None),
        DataType::Float64 => Arc::new(Option::<F64>::None),
        DataType::String => Arc::new(Option::<String>::None),
        DataType::Boolean => Arc::new(Option::<bool>::None),
        DataType::Bytes => Arc::new(Option::<Vec<u8>>::None),
        DataType::Timestamp(_) => unimplemented!(),
        _ => unimplemented!(),
    }
}

/// parse js key to tonbo key
///
/// Note: float number will be convert to `F32`/`F64`
pub(crate) fn parse_key(desc: &ValueDesc, key: JsValue, primary: bool) -> Result<Value, JsValue> {
    if key.is_undefined() || key.is_null() {
        match primary || !desc.is_nullable {
            true => return Err(format!("{} can not be null", &desc.name).into()),
            false => {
                return Ok(Value::new(
                    desc.datatype,
                    desc.name.clone(),
                    none_value(desc.datatype),
                    desc.is_nullable,
                ))
            }
        }
    }
    let value: Arc<dyn Any + Send + Sync> = match desc.datatype {
        DataType::UInt8 => to_col_value::<u8>(key.as_f64().unwrap().round() as u8, primary),
        DataType::UInt16 => to_col_value::<u16>(key.as_f64().unwrap().round() as u16, primary),
        DataType::UInt32 => to_col_value::<u32>(key.as_f64().unwrap().round() as u32, primary),
        DataType::UInt64 => to_col_value::<u64>(key.as_f64().unwrap().round() as u64, primary),
        DataType::Int8 => to_col_value::<i8>(key.as_f64().unwrap().round() as i8, primary),
        DataType::Int16 => to_col_value::<i16>(key.as_f64().unwrap().round() as i16, primary),
        DataType::Int32 => to_col_value::<i32>(key.as_f64().unwrap().round() as i32, primary),
        DataType::Float32 => to_col_value(F32::from(key.as_f64().unwrap() as f32), primary),
        DataType::Float64 => to_col_value(F64::from(key.as_f64().unwrap()), primary),
        DataType::Int64 => to_col_value::<i64>(key.as_f64().unwrap().round() as i64, primary),
        DataType::String => to_col_value::<String>(key.as_string().unwrap(), primary),
        DataType::Boolean => to_col_value::<bool>(key.as_bool().unwrap(), primary),
        DataType::Bytes => {
            to_col_value::<Vec<u8>>(key.dyn_into::<Uint8Array>().unwrap().to_vec(), primary)
        }
        DataType::Timestamp(_) => unimplemented!(),
        _ => unimplemented!(),
    };

    Ok(Value::new(
        desc.datatype,
        desc.name.clone(),
        value,
        desc.is_nullable,
    ))
}

pub(crate) fn parse_record(
    record: &Object,
    schema: &Vec<ValueDesc>,
    primary_key_index: usize,
) -> Result<DynRecord, JsValue> {
    let mut cols = Vec::with_capacity(schema.len());
    for (idx, col_desc) in schema.iter().enumerate() {
        let name = col_desc.name.as_str();
        let js_val = Reflect::get(record, &name.into())?;
        cols.push(parse_key(col_desc, js_val, primary_key_index == idx)?);
    }
    Ok(DynRecord::new(cols, primary_key_index))
}

/// convert tonbo `DynRecord` values to `JsValue`
///
pub(crate) fn to_record(cols: &Vec<Value>, primary_key_index: usize) -> JsValue {
    let obj = Object::new();
    for (idx, col) in cols.iter().enumerate() {
        let value = match col.datatype() {
            DataType::UInt8 => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<u8>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<u8>>().unwrap()).into(),
            },
            DataType::UInt16 => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<u16>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<u16>>().unwrap()).into(),
            },
            DataType::UInt32 => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<u32>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<u32>>().unwrap()).into(),
            },
            DataType::UInt64 => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<u64>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<u64>>().unwrap()).into(),
            },
            DataType::Int8 => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<i8>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<i8>>().unwrap()).into(),
            },
            DataType::Int16 => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<i16>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<i16>>().unwrap()).into(),
            },
            DataType::Int32 => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<i32>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<i32>>().unwrap()).into(),
            },
            DataType::Int64 => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<i64>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<i64>>().unwrap()).into(),
            },
            DataType::Float32 => match idx == primary_key_index {
                true => f32::from(cast_arc_value!(col.value, F32)).into(),
                false => cast_arc_value!(col.value, Option<F32>)
                    .map(|v| f32::from(v))
                    .into(),
            },
            DataType::Float64 => match idx == primary_key_index {
                true => f64::from(cast_arc_value!(col.value, F64)).into(),
                false => cast_arc_value!(col.value, Option<F64>)
                    .map(|v| f64::from(v))
                    .into(),
            },
            DataType::String => match idx == primary_key_index {
                true => (col.value.as_ref().downcast_ref::<String>().unwrap()).into(),
                false => (col
                    .value
                    .as_ref()
                    .downcast_ref::<Option<String>>()
                    .unwrap()
                    .clone())
                .into(),
            },
            DataType::Boolean => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<bool>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<bool>>().unwrap()).into(),
            },
            DataType::Bytes => match idx == primary_key_index {
                true => Uint8Array::from(
                    col.value
                        .as_ref()
                        .downcast_ref::<Vec<u8>>()
                        .as_ref()
                        .unwrap()
                        .as_slice(),
                )
                .into(),
                false => col
                    .value
                    .as_ref()
                    .downcast_ref::<Option<Vec<u8>>>()
                    .unwrap()
                    .as_ref()
                    .map(|v| Uint8Array::from(v.as_slice()).into())
                    .unwrap_or(JsValue::NULL),
            },
            DataType::Timestamp(_) => unimplemented!(),
            _ => unimplemented!(),
        };

        Reflect::set(&obj, &col.desc.name.as_str().into(), &value).unwrap();
    }
    obj.into()
}

#[cfg(test)]
mod tests {

    use tonbo::{cast_arc_value, datatype::DataType, record::ValueDesc, F64};
    use wasm_bindgen::JsValue;
    use wasm_bindgen_test::wasm_bindgen_test;

    use crate::utils::parse_key;

    #[wasm_bindgen_test]
    fn test_parse_key() {
        {
            let desc = ValueDesc::new("id".to_string(), DataType::UInt64, false);
            let key_col = parse_key(&desc, JsValue::from(19), true).unwrap();
            assert_eq!(key_col.datatype(), DataType::UInt64);
            assert_eq!(key_col.value.as_ref().downcast_ref::<u64>(), Some(&19_u64));
        }
        {
            let desc = ValueDesc::new("id".to_string(), DataType::UInt64, false);
            let result = parse_key(&desc, JsValue::NULL, true);
            assert!(result.is_err());
        }
        {
            let desc = ValueDesc::new("name".to_string(), DataType::String, false);
            let key_col = parse_key(&desc, JsValue::from("Hello tonbo"), false).unwrap();
            assert_eq!(key_col.datatype(), DataType::String);
            assert_eq!(
                key_col.value.as_ref().downcast_ref::<Option<String>>(),
                Some(&Some("Hello tonbo".to_string()))
            );
        }
        {
            let desc = ValueDesc::new("data".to_string(), DataType::Bytes, false);
            let key_col = parse_key(&desc, JsValue::from(b"Hello tonbo".to_vec()), false).unwrap();
            assert_eq!(key_col.datatype(), DataType::Bytes);
            assert_eq!(
                key_col.value.as_ref().downcast_ref::<Option<Vec<u8>>>(),
                Some(&Some(b"Hello tonbo".to_vec()))
            );
        }
        {
            let desc = ValueDesc::new("price".to_string(), DataType::Float64, false);
            let key_col = parse_key(&desc, JsValue::from(19), true).unwrap();
            assert_eq!(key_col.datatype(), DataType::Float64);
            assert_eq!(*cast_arc_value!(key_col.value, F64), F64::from(19f64));
        }
    }
}
