use js_sys::{Object, Reflect, Uint8Array};
use tonbo::{
    arrow::datatypes::DataType,
    record::{DynRecord, DynamicField, Value, ValueRef},
};
use wasm_bindgen::{JsCast, JsValue};

/// parse js key to tonbo key
pub(crate) fn parse_key(
    desc: &DynamicField,
    key: JsValue,
    primary: bool,
) -> Result<Value, JsValue> {
    if key.is_undefined() || key.is_null() {
        match primary || !desc.is_nullable {
            true => return Err(format!("{} can not be null", &desc.name).into()),
            false => return Ok(Value::Null),
        }
    }
    let value = match desc.data_type {
        DataType::UInt8 => Value::UInt8(key.as_f64().unwrap().round() as u8),
        DataType::UInt16 => Value::UInt16(key.as_f64().unwrap().round() as u16),
        DataType::UInt32 => Value::UInt32(key.as_f64().unwrap().round() as u32),
        DataType::UInt64 => Value::UInt64(key.as_f64().unwrap().round() as u64),
        DataType::Int8 => Value::Int8(key.as_f64().unwrap().round() as i8),
        DataType::Int16 => Value::Int16(key.as_f64().unwrap().round() as i16),
        DataType::Int32 => Value::Int32(key.as_f64().unwrap().round() as i32),
        DataType::Int64 => Value::Int64(key.as_f64().unwrap().round() as i64),
        DataType::Float32 => Value::Float32(key.as_f64().unwrap() as f32),
        DataType::Float64 => Value::Float64(key.as_f64().unwrap()),
        DataType::Utf8 => Value::String(key.as_string().unwrap()),
        DataType::Boolean => Value::Boolean(key.as_bool().unwrap()),
        DataType::Binary => Value::Binary(key.dyn_into::<Uint8Array>().unwrap().to_vec()),
        DataType::Timestamp(_, _) => unimplemented!(),
        _ => unimplemented!(),
    };

    Ok(value)
}

pub(crate) fn parse_record(
    record: &Object,
    schema: &Vec<DynamicField>,
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

/// convert a collection of [`Value`] to `JsValue`
pub(crate) fn to_record(schema: &[DynamicField], cols: &[Value]) -> JsValue {
    let obj = Object::new();
    for (col, field) in cols.iter().zip(schema.iter()) {
        let value = match col {
            Value::Null => todo!(),
            Value::Boolean(v) => (*v).into(),
            Value::Int8(v) => (*v).into(),
            Value::Int16(v) => (*v).into(),
            Value::Int32(v) => (*v).into(),
            Value::Int64(v) => (*v).into(),
            Value::UInt8(v) => (*v).into(),
            Value::UInt16(v) => (*v).into(),
            Value::UInt32(v) => (*v).into(),
            Value::UInt64(v) => (*v).into(),
            Value::Float32(v) => (*v).into(),
            Value::Float64(v) => (*v).into(),
            Value::String(v) => v.to_owned().into(),
            Value::Binary(v) => v.to_vec().into(),
            Value::FixedSizeBinary(v, w) => v.to_vec().into(),
            Value::Date32(_)
            | Value::Date64(_)
            | Value::List(_, _)
            | Value::Time32(_, _)
            | Value::Time64(_, _)
            | Value::Timestamp(_, _) => unimplemented!(),
        };
        Reflect::set(&obj, &field.name.as_str().into(), &value).unwrap();
    }
    obj.into()
}

/// convert a collection of [`ValueRef`] to `JsValue`
pub(crate) fn to_record_ref(schema: &[DynamicField], cols: &[ValueRef]) -> JsValue {
    let obj = Object::new();
    for (col, field) in cols.iter().zip(schema.iter()) {
        let value = match col {
            ValueRef::Null => todo!(),
            ValueRef::Boolean(v) => (*v).into(),
            ValueRef::Int8(v) => (*v).into(),
            ValueRef::Int16(v) => (*v).into(),
            ValueRef::Int32(v) => (*v).into(),
            ValueRef::Int64(v) => (*v).into(),
            ValueRef::UInt8(v) => (*v).into(),
            ValueRef::UInt16(v) => (*v).into(),
            ValueRef::UInt32(v) => (*v).into(),
            ValueRef::UInt64(v) => (*v).into(),
            ValueRef::Float32(v) => (*v).into(),
            ValueRef::Float64(v) => (*v).into(),
            ValueRef::String(v) => v.to_owned().into(),
            ValueRef::Binary(v) => v.to_vec().into(),
            ValueRef::FixedSizeBinary(v, _) => v.to_vec().into(),
            ValueRef::Date32(_)
            | ValueRef::Date64(_)
            | ValueRef::List(_, _)
            | ValueRef::Time32(_, _)
            | ValueRef::Time64(_, _)
            | ValueRef::Timestamp(_, _) => unimplemented!(),
        };
        Reflect::set(&obj, &field.name.as_str().into(), &value).unwrap();
    }
    obj.into()
}

#[cfg(test)]
mod tests {

    use tonbo::{
        arrow::datatypes::DataType,
        record::{AsValue, DynamicField, F64},
    };
    use wasm_bindgen::JsValue;
    use wasm_bindgen_test::wasm_bindgen_test;

    use crate::utils::parse_key;

    #[wasm_bindgen_test]
    fn test_parse_key() {
        {
            let desc = DynamicField::new("id".to_string(), DataType::UInt64, false);
            let key_col = parse_key(&desc, JsValue::from(19), true).unwrap();
            assert_eq!(key_col.as_u64_opt(), Some(&19_u64));
        }
        {
            let desc = DynamicField::new("id".to_string(), DataType::UInt64, false);
            let result = parse_key(&desc, JsValue::NULL, true);
            assert!(result.is_err());
        }
        {
            let desc = DynamicField::new("name".to_string(), DataType::Utf8, false);
            let key_col = parse_key(&desc, JsValue::from("Hello tonbo"), false).unwrap();
            assert_eq!(key_col.as_string_opt(), Some("Hello tonbo"));
        }
        {
            let desc = DynamicField::new("data".to_string(), DataType::Binary, false);
            let key_col = parse_key(&desc, JsValue::from(b"Hello tonbo".to_vec()), false).unwrap();
            assert_eq!(key_col.as_bytes_opt(), Some(b"Hello tonbo".as_slice()));
        }
        {
            let desc = DynamicField::new("price".to_string(), DataType::Float64, false);
            let key_col = parse_key(&desc, JsValue::from(19), true).unwrap();
            assert_eq!(key_col.as_f64_opt(), Some(&19f64));
        }
    }
}
