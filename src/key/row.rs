use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    ptr::NonNull,
    slice,
};

use thiserror::Error;
use typed_arrow_dyn::{DynCellRaw, DynRowRaw, DynViewError};

use super::{KeyOwned, owned::KeyOwnedError};

/// Errors that can occur while building a [`KeyRow`].
#[derive(Debug, Error)]
pub enum KeyRowError {
    /// Encountered a null cell while constructing the key.
    #[error("key column {index} contained null")]
    NullComponent {
        /// Column index containing the unexpected null.
        index: usize,
    },
    /// Failed to convert an owned key into the dynamic representation.
    #[error("failed to convert owned key: {0}")]
    Owned(#[from] KeyOwnedError),
    /// Dynamic view construction failed.
    #[error("failed to build dynamic key view: {0}")]
    DynView(#[from] DynViewError),
}

/// Wrapper around [`DynRowRaw`] that provides Tonbo-specific comparison semantics.
#[derive(Clone, Debug)]
pub struct KeyRow {
    raw: DynRowRaw,
}

impl KeyRow {
    /// Build a key row from the raw dynamic representation.
    pub fn from_dyn(raw: DynRowRaw) -> Result<Self, KeyRowError> {
        for (idx, cell) in raw.cells().iter().enumerate() {
            if cell.is_none() {
                return Err(KeyRowError::NullComponent { index: idx });
            }
        }
        Ok(Self { raw })
    }

    /// Build a key row borrowing from an owned key.
    ///
    /// # Errors
    /// Returns [`KeyRowError`] when the owned key contains an unsupported component type.
    pub fn from_owned(key: &KeyOwned) -> Result<Self, KeyRowError> {
        let raw = key.as_raw()?;
        Self::from_dyn(raw)
    }

    /// Borrow the underlying dynamic row.
    pub fn as_dyn(&self) -> &DynRowRaw {
        &self.raw
    }

    /// Consume the wrapper and return the owned dynamic row.
    pub fn into_dyn(self) -> DynRowRaw {
        self.raw
    }

    /// Number of components carried by this key.
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    /// Whether the key contains no components.
    pub fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }

    /// Convert the borrowed key into its owned counterpart.
    ///
    /// # Panics
    /// Panics if the dynamic row contained an unsupported data type. This should never happen
    /// because the extractor rejects unsupported key columns.
    pub fn to_owned(&self) -> KeyOwned {
        KeyOwned::from_key_row(self).expect("extractor guarantees supported key types")
    }

    pub(crate) fn heap_size(&self) -> usize {
        dyn_row_heap_size(self.as_dyn())
    }

    /// Debug helper: return a representation of the key bytes for logging.
    pub fn debug_bytes(&self) -> Vec<String> {
        self.raw
            .cells()
            .iter()
            .map(|cell| match cell {
                Some(DynCellRaw::Str { ptr, len }) => {
                    let bytes = unsafe { slice::from_raw_parts(ptr.as_ptr(), *len) };
                    format!("Str({:?})", String::from_utf8_lossy(bytes))
                }
                Some(DynCellRaw::Bin { ptr, len }) => {
                    let bytes = unsafe { slice::from_raw_parts(ptr.as_ptr(), *len) };
                    format!("Bin({:?})", bytes)
                }
                Some(DynCellRaw::I32(v)) => format!("I32({v})"),
                Some(DynCellRaw::I64(v)) => format!("I64({v})"),
                Some(DynCellRaw::U64(v)) => format!("U64({v})"),
                Some(other) => format!("{other:?}"),
                None => "None".to_string(),
            })
            .collect()
    }
}

impl PartialEq for KeyRow {
    fn eq(&self, other: &Self) -> bool {
        dyn_rows_equal(self.as_dyn(), other.as_dyn())
    }
}

impl Eq for KeyRow {}

impl PartialOrd for KeyRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyRow {
    fn cmp(&self, other: &Self) -> Ordering {
        dyn_rows_cmp(self.as_dyn(), other.as_dyn())
    }
}

impl Hash for KeyRow {
    fn hash<H: Hasher>(&self, state: &mut H) {
        dyn_rows_hash(self.as_dyn(), state);
    }
}

// Cross-type comparison with KeyOwned
impl PartialEq<KeyOwned> for KeyRow {
    fn eq(&self, other: &KeyOwned) -> bool {
        let right = other.as_raw().expect("owned key must remain convertible");
        dyn_rows_equal(self.as_dyn(), &right)
    }
}

impl PartialOrd<KeyOwned> for KeyRow {
    fn partial_cmp(&self, other: &KeyOwned) -> Option<Ordering> {
        let right = other.as_raw().expect("owned key must remain convertible");
        Some(dyn_rows_cmp(self.as_dyn(), &right))
    }
}

pub(crate) fn dyn_rows_equal(lhs: &DynRowRaw, rhs: &DynRowRaw) -> bool {
    if lhs.len() != rhs.len() {
        return false;
    }
    lhs.cells()
        .iter()
        .zip(rhs.cells().iter())
        .all(|(a, b)| match (a, b) {
            (Some(left), Some(right)) => dyn_cells_equal(left, right),
            _ => false,
        })
}

pub(crate) fn dyn_rows_cmp(lhs: &DynRowRaw, rhs: &DynRowRaw) -> Ordering {
    for (a, b) in lhs.cells().iter().zip(rhs.cells()) {
        match (a, b) {
            (Some(left), Some(right)) => {
                let ord = dyn_cells_cmp(left, right);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            (Some(_), None) => return Ordering::Greater,
            (None, Some(_)) => return Ordering::Less,
            (None, None) => continue,
        }
    }
    lhs.len().cmp(&rhs.len())
}

pub(crate) fn dyn_rows_hash<H: Hasher>(row: &DynRowRaw, state: &mut H) {
    for cell in row.cells().iter().flatten() {
        dyn_cells_hash(cell, state);
    }
}

pub(crate) fn dyn_row_heap_size(row: &DynRowRaw) -> usize {
    row.cells()
        .iter()
        .filter_map(|cell| cell.as_ref())
        .map(dyn_cell_heap_size)
        .sum()
}

fn dyn_cells_equal(lhs: &DynCellRaw, rhs: &DynCellRaw) -> bool {
    use DynCellRaw::*;
    match (lhs, rhs) {
        (Bool(a), Bool(b)) => a == b,
        (I32(a), I32(b)) => a == b,
        (I64(a), I64(b)) => a == b,
        (U32(a), U32(b)) => a == b,
        (U64(a), U64(b)) => a == b,
        (F32(a), F32(b)) => floats_equal_f32(*a, *b),
        (F64(a), F64(b)) => floats_equal_f64(*a, *b),
        (Str { ptr: ap, len: al }, Str { ptr: bp, len: bl }) => unsafe {
            bytes_equal(*ap, *al, *bp, *bl)
        },
        (Bin { ptr: ap, len: al }, Bin { ptr: bp, len: bl }) => unsafe {
            bytes_equal(*ap, *al, *bp, *bl)
        },
        (Struct(a), Struct(b)) => {
            let view_a = unsafe { a.as_view() };
            let view_b = unsafe { b.as_view() };
            if view_a.len() != view_b.len() {
                return false;
            }
            for idx in 0..view_a.len() {
                let Some(cell_a) = view_a.get(idx).expect("struct field access succeeded") else {
                    return false;
                };
                let Some(cell_b) = view_b.get(idx).expect("struct field access succeeded") else {
                    return false;
                };
                if !dyn_cells_equal(&cell_a.into_raw(), &cell_b.into_raw()) {
                    return false;
                }
            }
            true
        }
        _ => false,
    }
}

fn dyn_cells_cmp(lhs: &DynCellRaw, rhs: &DynCellRaw) -> Ordering {
    use DynCellRaw::*;
    match (lhs, rhs) {
        (Bool(a), Bool(b)) => a.cmp(b),
        (I32(a), I32(b)) => a.cmp(b),
        (I64(a), I64(b)) => a.cmp(b),
        (U32(a), U32(b)) => a.cmp(b),
        (U64(a), U64(b)) => a.cmp(b),
        (F32(a), F32(b)) => floats_cmp_f32(*a, *b),
        (F64(a), F64(b)) => floats_cmp_f64(*a, *b),
        (Str { ptr: ap, len: al }, Str { ptr: bp, len: bl }) => unsafe {
            bytes_cmp(*ap, *al, *bp, *bl)
        },
        (Bin { ptr: ap, len: al }, Bin { ptr: bp, len: bl }) => unsafe {
            bytes_cmp(*ap, *al, *bp, *bl)
        },
        (Struct(a), Struct(b)) => {
            let view_a = unsafe { a.as_view() };
            let view_b = unsafe { b.as_view() };
            for idx in 0..view_a.len().min(view_b.len()) {
                let Some(cell_a) = view_a.get(idx).expect("struct field access succeeded") else {
                    return Ordering::Less;
                };
                let Some(cell_b) = view_b.get(idx).expect("struct field access succeeded") else {
                    return Ordering::Greater;
                };
                let ord = dyn_cells_cmp(&cell_a.into_raw(), &cell_b.into_raw());
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            view_a.len().cmp(&view_b.len())
        }
        _ => cell_tag(lhs).cmp(&cell_tag(rhs)),
    }
}

fn dyn_cells_hash<H: Hasher>(cell: &DynCellRaw, state: &mut H) {
    use DynCellRaw::*;
    cell_tag(cell).hash(state);
    match cell {
        Bool(v) => v.hash(state),
        I32(v) => v.hash(state),
        I64(v) => v.hash(state),
        U32(v) => v.hash(state),
        U64(v) => v.hash(state),
        F32(v) => hash_float_f32(*v, state),
        F64(v) => hash_float_f64(*v, state),
        Str { ptr, len } | Bin { ptr, len } => unsafe { hash_bytes(*ptr, *len, state) },
        Struct(raw) => {
            let view = unsafe { raw.as_view() };
            view.len().hash(state);
            for idx in 0..view.len() {
                let value = view.get(idx).expect("struct field access succeeded");
                let cell = value.expect("struct key field contained null");
                dyn_cells_hash(&cell.into_raw(), state);
            }
        }
        _ => {}
    }
}

fn dyn_cell_heap_size(cell: &DynCellRaw) -> usize {
    use DynCellRaw::*;
    match cell {
        Str { len, .. } | Bin { len, .. } => *len,
        Struct(raw) => unsafe {
            let view = raw.as_view();
            let mut size = 0;
            for idx in 0..view.len() {
                let value = view.get(idx).expect("struct field access succeeded");
                let cell = value.expect("struct key field contained null");
                size += dyn_cell_heap_size(&cell.into_raw());
            }
            size
        },
        _ => 0,
    }
}

fn floats_equal_f32(lhs: f32, rhs: f32) -> bool {
    (lhs.is_nan() && rhs.is_nan()) || lhs == rhs
}

fn floats_equal_f64(lhs: f64, rhs: f64) -> bool {
    (lhs.is_nan() && rhs.is_nan()) || lhs == rhs
}

fn floats_cmp_f32(lhs: f32, rhs: f32) -> Ordering {
    lhs.partial_cmp(&rhs).unwrap_or(Ordering::Equal)
}

fn floats_cmp_f64(lhs: f64, rhs: f64) -> Ordering {
    lhs.partial_cmp(&rhs).unwrap_or(Ordering::Equal)
}

fn hash_float_f32<H: Hasher>(value: f32, state: &mut H) {
    if value.is_nan() {
        0xFFu8.hash(state);
    } else {
        value.to_bits().hash(state);
    }
}

fn hash_float_f64<H: Hasher>(value: f64, state: &mut H) {
    if value.is_nan() {
        0xFFu8.hash(state);
    } else {
        value.to_bits().hash(state);
    }
}

unsafe fn bytes_equal(a_ptr: NonNull<u8>, a_len: usize, b_ptr: NonNull<u8>, b_len: usize) -> bool {
    let lhs = unsafe { slice::from_raw_parts(a_ptr.as_ptr(), a_len) };
    let rhs = unsafe { slice::from_raw_parts(b_ptr.as_ptr(), b_len) };
    lhs == rhs
}

unsafe fn bytes_cmp(
    a_ptr: NonNull<u8>,
    a_len: usize,
    b_ptr: NonNull<u8>,
    b_len: usize,
) -> Ordering {
    let lhs = unsafe { slice::from_raw_parts(a_ptr.as_ptr(), a_len) };
    let rhs = unsafe { slice::from_raw_parts(b_ptr.as_ptr(), b_len) };
    lhs.cmp(rhs)
}

unsafe fn hash_bytes<H: Hasher>(ptr: NonNull<u8>, len: usize, state: &mut H) {
    let bytes = unsafe { slice::from_raw_parts(ptr.as_ptr(), len) };
    len.hash(state);
    state.write(bytes);
}

fn cell_tag(cell: &DynCellRaw) -> u8 {
    use DynCellRaw::*;
    match cell {
        Str { .. } => 0,
        Bin { .. } => 1,
        U64(_) => 2,
        U32(_) => 3,
        I64(_) => 4,
        I32(_) => 5,
        F64(_) => 6,
        F32(_) => 7,
        Bool(_) => 8,
        Struct(_) => 9,
        I8(_) | I16(_) => 5,
        U8(_) | U16(_) => 3,
        _ => 255,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::hash_map::DefaultHasher, sync::Arc};

    use arrow_schema::{DataType, Field, Fields};
    use typed_arrow_dyn::DynCellRaw;

    use super::*;

    fn utf8_cell(value: &Arc<String>) -> DynCellRaw {
        let bytes = value.as_bytes();
        let ptr = unsafe { NonNull::new_unchecked(bytes.as_ptr() as *mut u8) };
        DynCellRaw::Str {
            ptr,
            len: bytes.len(),
        }
    }

    fn binary_cell(value: &Arc<Vec<u8>>) -> DynCellRaw {
        let ptr = unsafe { NonNull::new_unchecked(value.as_ptr() as *mut u8) };
        DynCellRaw::Bin {
            ptr,
            len: value.len(),
        }
    }

    fn make_row(cells: Vec<DynCellRaw>) -> KeyRow {
        let fields = Fields::from(
            cells
                .iter()
                .enumerate()
                .map(|(idx, cell)| {
                    let dt = match cell {
                        DynCellRaw::Bool(_) => DataType::Boolean,
                        DynCellRaw::I32(_) | DynCellRaw::I16(_) | DynCellRaw::I8(_) => {
                            DataType::Int32
                        }
                        DynCellRaw::I64(_) => DataType::Int64,
                        DynCellRaw::U32(_) | DynCellRaw::U16(_) | DynCellRaw::U8(_) => {
                            DataType::UInt32
                        }
                        DynCellRaw::U64(_) => DataType::UInt64,
                        DynCellRaw::F32(_) => DataType::Float32,
                        DynCellRaw::F64(_) => DataType::Float64,
                        DynCellRaw::Str { .. } => DataType::Utf8,
                        DynCellRaw::Bin { .. } => DataType::Binary,
                        DynCellRaw::Struct(_) => {
                            DataType::Struct(Fields::from(Vec::<Arc<Field>>::new()))
                        }
                        other => panic!("unsupported test cell {other:?}"),
                    };
                    Arc::new(Field::new(format!("c{idx}"), dt, false))
                })
                .collect::<Vec<_>>(),
        );
        let row = DynRowRaw::from_cells(fields, cells).expect("row");
        KeyRow::from_dyn(row).expect("no nulls")
    }

    #[test]
    fn equal_rows_compare_identically() {
        let s = Arc::new("hello".to_string());
        let row_a = make_row(vec![utf8_cell(&s), DynCellRaw::I64(42)]);
        let row_b = make_row(vec![utf8_cell(&s), DynCellRaw::I64(42)]);
        assert_eq!(row_a, row_b);
        assert_eq!(row_a.cmp(&row_b), Ordering::Equal);
    }

    #[test]
    fn lexicographic_order_for_strings() {
        let a = Arc::new("alpha".to_string());
        let b = Arc::new("beta".to_string());
        let row_a = make_row(vec![utf8_cell(&a)]);
        let row_b = make_row(vec![utf8_cell(&b)]);
        assert!(row_a < row_b);
    }

    #[test]
    fn nan_floats_compare_equal() {
        let row_a = make_row(vec![DynCellRaw::F32(f32::NAN)]);
        let row_b = make_row(vec![DynCellRaw::F32(f32::NAN)]);
        assert_eq!(row_a, row_b);
        assert_eq!(row_a.cmp(&row_b), Ordering::Equal);
    }

    #[test]
    fn binary_hash_matches_bytes() {
        let data = Arc::new(vec![1, 2, 3, 4]);
        let row = make_row(vec![binary_cell(&data)]);
        let mut hasher = DefaultHasher::new();
        row.hash(&mut hasher);
        let hash = hasher.finish();
        assert_ne!(hash, 0);
    }

    #[test]
    fn key_row_round_trips_via_owned() {
        let text = Arc::new("alpha".to_string());
        let row = make_row(vec![utf8_cell(&text), DynCellRaw::I64(99)]);

        let owned = row.to_owned();
        let rebuilt = KeyRow::from_owned(&owned).expect("round trip converts back to row");

        assert_eq!(rebuilt, row);
        assert_eq!(rebuilt.len(), 2);

        let owned_again = rebuilt.to_owned();
        assert_eq!(owned_again, owned);
    }
}
