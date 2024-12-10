use std::{any::Any, sync::Arc};

use js_sys::{Object, Reflect, Uint8Array};
use tonbo::record::{Column, ColumnDesc, Datatype, DynRecord};
use wasm_bindgen::{JsCast, JsValue};

fn to_col_value<T: 'static + Send + Sync>(value: T, primary: bool) -> Arc<dyn Any + Send + Sync> {
    match primary {
        true => Arc::new(value) as Arc<dyn Any + Send + Sync>,
        false => Arc::new(Some(value)) as Arc<dyn Any + Send + Sync>,
    }
}

fn none_value(datatype: Datatype) -> Arc<dyn Any + Send + Sync> {
    match datatype {
        Datatype::UInt8 => Arc::new(Option::<u8>::None),
        Datatype::UInt16 => Arc::new(Option::<u16>::None),
        Datatype::UInt32 => Arc::new(Option::<u32>::None),
        Datatype::UInt64 => Arc::new(Option::<u64>::None),
        Datatype::Int8 => Arc::new(Option::<i8>::None),
        Datatype::Int16 => Arc::new(Option::<i16>::None),
        Datatype::Int32 => Arc::new(Option::<i32>::None),
        Datatype::Int64 => Arc::new(Option::<i64>::None),
        Datatype::String => Arc::new(Option::<String>::None),
        Datatype::Boolean => Arc::new(Option::<bool>::None),
        Datatype::Bytes => Arc::new(Option::<Vec<u8>>::None),
    }
}

pub(crate) fn parse_key(desc: &ColumnDesc, key: JsValue, primary: bool) -> Result<Column, JsValue> {
    if key.is_undefined() || key.is_null() {
        match primary || !desc.is_nullable {
            true => return Err(format!("{} can not be null", &desc.name).into()),
            false => {
                return Ok(Column::new(
                    desc.datatype,
                    desc.name.clone(),
                    none_value(desc.datatype),
                    desc.is_nullable,
                ))
            }
        }
    }
    let value: Arc<dyn Any + Send + Sync> = match desc.datatype {
        Datatype::UInt8 => to_col_value::<u8>(key.as_f64().unwrap().round() as u8, primary),
        Datatype::UInt16 => to_col_value::<u16>(key.as_f64().unwrap().round() as u16, primary),
        Datatype::UInt32 => to_col_value::<u32>(key.as_f64().unwrap().round() as u32, primary),
        Datatype::UInt64 => to_col_value::<u64>(key.as_f64().unwrap().round() as u64, primary),
        Datatype::Int8 => to_col_value::<i8>(key.as_f64().unwrap().round() as i8, primary),
        Datatype::Int16 => to_col_value::<i16>(key.as_f64().unwrap().round() as i16, primary),
        Datatype::Int32 => to_col_value::<i32>(key.as_f64().unwrap().round() as i32, primary),
        Datatype::Int64 => to_col_value::<i64>(key.as_f64().unwrap().round() as i64, primary),
        Datatype::String => to_col_value::<String>(key.as_string().unwrap(), primary),
        Datatype::Boolean => to_col_value::<bool>(key.as_bool().unwrap(), primary),
        Datatype::Bytes => {
            to_col_value::<Vec<u8>>(key.dyn_into::<Uint8Array>().unwrap().to_vec(), primary)
        }
    };

    Ok(Column::new(
        desc.datatype,
        desc.name.clone(),
        value,
        desc.is_nullable,
    ))
}

pub(crate) fn parse_record(
    record: &Object,
    schema: &Vec<ColumnDesc>,
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

pub(crate) fn to_record(cols: &Vec<Column>, primary_key_index: usize) -> JsValue {
    let obj = Object::new();
    for (idx, col) in cols.iter().enumerate() {
        let value = match col.datatype {
            Datatype::UInt8 => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<u8>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<u8>>().unwrap()).into(),
            },
            Datatype::UInt16 => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<u16>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<u16>>().unwrap()).into(),
            },
            Datatype::UInt32 => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<u32>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<u32>>().unwrap()).into(),
            },
            Datatype::UInt64 => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<u64>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<u64>>().unwrap()).into(),
            },
            Datatype::Int8 => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<i8>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<i8>>().unwrap()).into(),
            },
            Datatype::Int16 => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<i16>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<i16>>().unwrap()).into(),
            },
            Datatype::Int32 => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<i32>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<i32>>().unwrap()).into(),
            },
            Datatype::Int64 => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<i64>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<i64>>().unwrap()).into(),
            },
            Datatype::String => match idx == primary_key_index {
                true => (col.value.as_ref().downcast_ref::<String>().unwrap()).into(),
                false => (col
                    .value
                    .as_ref()
                    .downcast_ref::<Option<String>>()
                    .unwrap()
                    .clone())
                .into(),
            },
            Datatype::Boolean => match idx == primary_key_index {
                true => (*col.value.as_ref().downcast_ref::<bool>().unwrap()).into(),
                false => (*col.value.as_ref().downcast_ref::<Option<bool>>().unwrap()).into(),
            },
            Datatype::Bytes => match idx == primary_key_index {
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
        };

        Reflect::set(&obj, &col.name.as_str().into(), &value).unwrap();
    }
    obj.into()
}

#[cfg(test)]
mod tests {

    use tonbo::record::{ColumnDesc, Datatype};
    use wasm_bindgen::JsValue;
    use wasm_bindgen_test::wasm_bindgen_test;

    use crate::utils::parse_key;

    #[wasm_bindgen_test]
    fn test_parse_key() {
        {
            let desc = ColumnDesc::new("id".to_string(), Datatype::UInt64, false);
            let key_col = parse_key(&desc, JsValue::from(19), true).unwrap();
            assert_eq!(key_col.datatype, Datatype::UInt64);
            assert_eq!(key_col.value.as_ref().downcast_ref::<u64>(), Some(&19_u64));
        }
        {
            let desc = ColumnDesc::new("id".to_string(), Datatype::UInt64, false);
            let result = parse_key(&desc, JsValue::NULL, true);
            assert!(result.is_err());
        }
        {
            let desc = ColumnDesc::new("name".to_string(), Datatype::String, false);
            let key_col = parse_key(&desc, JsValue::from("Hello tonbo"), false).unwrap();
            assert_eq!(key_col.datatype, Datatype::String);
            assert_eq!(
                key_col.value.as_ref().downcast_ref::<Option<String>>(),
                Some(&Some("Hello tonbo".to_string()))
            );
        }
        {
            let desc = ColumnDesc::new("data".to_string(), Datatype::Bytes, false);
            let key_col = parse_key(&desc, JsValue::from(b"Hello tonbo".to_vec()), false).unwrap();
            assert_eq!(key_col.datatype, Datatype::Bytes);
            assert_eq!(
                key_col.value.as_ref().downcast_ref::<Option<Vec<u8>>>(),
                Some(&Some(b"Hello tonbo".to_vec()))
            );
        }
    }
}
