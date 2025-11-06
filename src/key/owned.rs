//! Owned key representation backed by typed-arrow dynamic rows.

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow_schema::{DataType, Field, Fields};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error as DeError, SeqAccess, Visitor},
    ser::{Error as SerError, SerializeSeq},
};
use thiserror::Error;
use typed_arrow_dyn::{DynCell, DynRowOwned, DynRowRaw, DynViewError};

use super::row::{KeyRow, dyn_rows_cmp, dyn_rows_equal, dyn_rows_hash};

/// Owned key stored as a dynamic row detached from batch lifetimes.
#[derive(Clone, Debug)]
pub struct KeyOwned {
    row: DynRowOwned,
}

impl KeyOwned {
    /// Construct an owned key from a dynamic owned row.
    pub fn new(row: DynRowOwned) -> Self {
        Self { row }
    }

    /// Reconstruct an owned key from a borrowed key row.
    pub fn from_key_row(row: &KeyRow) -> Result<Self, KeyOwnedError> {
        let owned = DynRowOwned::from_raw(row.as_dyn())?;
        Ok(Self::new(owned))
    }

    /// Number of components carried by this key.
    pub fn len(&self) -> usize {
        self.row.len()
    }

    /// Returns `true` when the key contains zero components.
    pub fn is_empty(&self) -> bool {
        self.row.is_empty()
    }

    /// Borrow the owned dynamic row backing this key.
    pub fn as_row(&self) -> &DynRowOwned {
        &self.row
    }

    /// Consume the key, yielding the owned dynamic row.
    pub fn into_row(self) -> DynRowOwned {
        self.row
    }

    /// Convert the owned representation into a lifetime-erased raw row.
    pub fn as_raw(&self) -> Result<DynRowRaw, KeyOwnedError> {
        self.row.as_raw().map_err(KeyOwnedError::from)
    }

    /// Return the key interpreted as UTF-8 when it consists of a single string component.
    pub fn as_utf8(&self) -> Option<&str> {
        match self.row.cells().first()? {
            Some(DynCell::Str(value)) if self.row.len() == 1 => Some(value.as_str()),
            _ => None,
        }
    }

    /// Return the key interpreted as raw bytes when it consists of a single binary component.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self.row.cells().first()? {
            Some(DynCell::Bin(value)) if self.row.len() == 1 => Some(value.as_slice()),
            _ => None,
        }
    }

    /// Build a composite key by concatenating the components of the provided keys.
    pub fn tuple(parts: Vec<Self>) -> Self {
        let mut field_refs = Vec::new();
        let mut cells = Vec::new();
        for part in parts.into_iter() {
            let (part_fields, part_cells) = part.into_row().into_parts();
            field_refs.extend(part_fields.iter().cloned());
            cells.extend(part_cells);
        }
        let fields = Fields::from(field_refs);
        let row = DynRowOwned::try_new(fields, cells).expect("tuple key components");
        Self::new(row)
    }

    fn from_single_cell(cell: DynCell) -> Self {
        let data_type = cell_data_type(&cell).expect("scalar key component must be supported");
        let field = Arc::new(Field::new("k0", data_type, false));
        let fields = Fields::from(vec![field]);
        let cells = vec![Some(cell)];
        KeyOwned::new(
            DynRowOwned::try_new(fields, cells).expect("single component key construction"),
        )
    }
}

impl PartialEq for KeyOwned {
    fn eq(&self, other: &Self) -> bool {
        let left = self.as_raw().expect("owned key must remain convertible");
        let right = other.as_raw().expect("owned key must remain convertible");
        dyn_rows_equal(&left, &right)
    }
}

impl Eq for KeyOwned {}

impl PartialOrd for KeyOwned {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyOwned {
    fn cmp(&self, other: &Self) -> Ordering {
        let left = self.as_raw().expect("owned key must remain convertible");
        let right = other.as_raw().expect("owned key must remain convertible");
        dyn_rows_cmp(&left, &right)
    }
}

impl Hash for KeyOwned {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let raw = self.as_raw().expect("owned key must remain convertible");
        dyn_rows_hash(&raw, state);
    }
}

impl From<&KeyRow> for KeyOwned {
    fn from(row: &KeyRow) -> Self {
        KeyOwned::from_key_row(row).expect("key row contains supported components")
    }
}

impl From<KeyRow> for KeyOwned {
    fn from(row: KeyRow) -> Self {
        KeyOwned::from(&row)
    }
}

// Generate single-component constructors and `From` impls for supported scalar key values.
macro_rules! key_owned_from_scalar {
    ($( $fn_name:ident => $ty:ty, $make_cell:expr );+ $(;)?) => {
        $(
            impl KeyOwned {
                fn $fn_name(value: $ty) -> Self {
                    let cell = ($make_cell)(value);
                    Self::from_single_cell(cell)
                }
            }

            impl From<$ty> for KeyOwned {
                fn from(value: $ty) -> Self {
                    KeyOwned::$fn_name(value)
                }
            }
        )+
    };
}

key_owned_from_scalar!(
    from_bool => bool, DynCell::Bool;
    from_i32 => i32, DynCell::I32;
    from_i64 => i64, DynCell::I64;
    from_u32 => u32, DynCell::U32;
    from_u64 => u64, DynCell::U64;
    from_f32 => f32, DynCell::F32;
    from_f64 => f64, DynCell::F64;
    from_str => &str, |v: &str| DynCell::Str(v.to_owned());
    from_string => String, |v: String| DynCell::Str(v);
    from_bytes => Vec<u8>, |v: Vec<u8>| DynCell::Bin(v);
    from_bytes_slice => &[u8], |v: &[u8]| DynCell::Bin(v.to_vec());
);

impl Serialize for KeyOwned {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.row.len()))?;
        for (idx, cell) in self.row.cells().iter().enumerate() {
            let value = cell
                .as_ref()
                .ok_or_else(|| SerError::custom(format!("key component {idx} was null")))?;
            cell_data_type(value).map_err(|err| SerError::custom(err.to_string()))?;
            seq.serialize_element(value)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for KeyOwned {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let cells = deserializer.deserialize_seq(DynCellSeqVisitor)?;
        let mut field_refs = Vec::with_capacity(cells.len());
        let mut owned_cells = Vec::with_capacity(cells.len());
        for (idx, cell) in cells.into_iter().enumerate() {
            let data_type =
                cell_data_type(&cell).map_err(|err| DeError::custom(err.to_string()))?;
            field_refs.push(Arc::new(Field::new(format!("k{idx}"), data_type, false)));
            owned_cells.push(Some(cell));
        }
        let fields = Fields::from(field_refs);
        let row = DynRowOwned::try_new(fields, owned_cells)
            .map_err(|err| DeError::custom(err.to_string()))?;
        Ok(KeyOwned::new(row))
    }
}

/// Errors raised while building or converting owned keys.
#[derive(Debug, Error)]
pub enum KeyOwnedError {
    /// Encountered an unsupported key component.
    #[error("unsupported key component: {0}")]
    Unsupported(&'static str),
    /// Dynamic view construction failed while materialising a raw row.
    #[error("dynamic key conversion failed: {0}")]
    Dyn(#[from] DynViewError),
}

fn cell_data_type(cell: &DynCell) -> Result<DataType, KeyOwnedError> {
    match cell {
        DynCell::Bool(_) => Ok(DataType::Boolean),
        DynCell::I8(_) => Ok(DataType::Int8),
        DynCell::I16(_) => Ok(DataType::Int16),
        DynCell::I32(_) => Ok(DataType::Int32),
        DynCell::I64(_) => Ok(DataType::Int64),
        DynCell::U8(_) => Ok(DataType::UInt8),
        DynCell::U16(_) => Ok(DataType::UInt16),
        DynCell::U32(_) => Ok(DataType::UInt32),
        DynCell::U64(_) => Ok(DataType::UInt64),
        DynCell::F32(_) => Ok(DataType::Float32),
        DynCell::F64(_) => Ok(DataType::Float64),
        DynCell::Str(_) => Ok(DataType::Utf8),
        DynCell::Bin(_) => Ok(DataType::Binary),
        DynCell::Null => Err(KeyOwnedError::Unsupported("null key component")),
        DynCell::Struct(_) => Err(KeyOwnedError::Unsupported("struct key component")),
        DynCell::List(_) => Err(KeyOwnedError::Unsupported("list key component")),
        DynCell::FixedSizeList(_) => {
            Err(KeyOwnedError::Unsupported("fixed-size list key component"))
        }
        DynCell::Map(_) => Err(KeyOwnedError::Unsupported("map key component")),
        DynCell::Union { .. } => Err(KeyOwnedError::Unsupported("union key component")),
    }
}

struct DynCellSeqVisitor;

impl<'de> Visitor<'de> for DynCellSeqVisitor {
    type Value = Vec<DynCell>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a sequence of dynamic key cells")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut cells = Vec::new();
        while let Some(cell) = seq.next_element::<DynCell>()? {
            cells.push(cell);
        }
        Ok(cells)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_round_trip_preserves_composite_keys() {
        let key = KeyOwned::tuple(vec![KeyOwned::from("alpha"), KeyOwned::from(42i64)]);
        let json = serde_json::to_string(&key).expect("serialize key");
        let restored: KeyOwned = serde_json::from_str(&json).expect("deserialize key");
        assert_eq!(restored, key);

        let row = KeyRow::from_owned(&restored).expect("convert restored key to row");
        assert_eq!(row.len(), 2);
    }
}
