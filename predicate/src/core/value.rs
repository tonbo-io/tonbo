use std::cmp::Ordering;

use typed_arrow_dyn::{DynCell, DynCellRaw, DynCellRef};

/// Literal values accepted by predicate operands, backed by `DynCell`.
#[derive(Clone, Debug)]
pub struct ScalarValue {
    cell: DynCell,
}

impl ScalarValue {
    /// Represents SQL/Arrow `NULL`.
    #[must_use]
    pub fn null() -> Self {
        Self {
            cell: DynCell::Null,
        }
    }

    pub(crate) fn from_dyn(cell: DynCell) -> Self {
        Self { cell }
    }

    /// Returns true when the literal is `NULL`.
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self.cell, DynCell::Null)
    }

    /// Returns a borrowed view over this scalar value.
    #[must_use]
    pub fn as_ref(&self) -> ScalarValueRef<'_> {
        let ref_cell = self
            .cell
            .as_ref()
            .expect("ScalarValue should only hold scalar DynCell variants");
        ScalarValueRef::from_dyn(ref_cell)
    }

    /// Compares this scalar with another, returning the ordering when both sides are comparable.
    pub fn compare(&self, other: &Self) -> Option<Ordering> {
        self.as_ref().compare(&other.as_ref())
    }

    /// Access the underlying dynamic cell.
    #[must_use]
    pub fn as_dyn(&self) -> &DynCell {
        &self.cell
    }

    /// Consume this scalar and return the underlying dynamic cell.
    pub fn into_dyn(self) -> DynCell {
        self.cell
    }
}

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        let left = self.as_ref();
        let right = other.as_ref();
        match (left.is_null(), right.is_null()) {
            (true, true) => true,
            _ => left
                .compare(&right)
                .map(|ord| ord == Ordering::Equal)
                .unwrap_or_else(|| left.eq(&right.as_dyn())),
        }
    }
}

impl From<bool> for ScalarValue {
    fn from(value: bool) -> Self {
        ScalarValue::from_dyn(DynCell::Bool(value))
    }
}

impl From<i64> for ScalarValue {
    fn from(value: i64) -> Self {
        ScalarValue::from_dyn(DynCell::I64(value))
    }
}

impl From<u64> for ScalarValue {
    fn from(value: u64) -> Self {
        ScalarValue::from_dyn(DynCell::U64(value))
    }
}

impl From<f64> for ScalarValue {
    fn from(value: f64) -> Self {
        ScalarValue::from_dyn(DynCell::F64(value))
    }
}

impl From<String> for ScalarValue {
    fn from(value: String) -> Self {
        ScalarValue::from_dyn(DynCell::Str(value))
    }
}

impl From<&str> for ScalarValue {
    fn from(value: &str) -> Self {
        ScalarValue::from_dyn(DynCell::Str(value.to_owned()))
    }
}

impl From<Vec<u8>> for ScalarValue {
    fn from(value: Vec<u8>) -> Self {
        ScalarValue::from_dyn(DynCell::Bin(value))
    }
}

impl From<&[u8]> for ScalarValue {
    fn from(value: &[u8]) -> Self {
        ScalarValue::from_dyn(DynCell::Bin(value.to_vec()))
    }
}

/// Borrowed view over a scalar value backed by `DynCellRef`.
#[derive(Clone, Debug)]
pub struct ScalarValueRef<'a> {
    cell: DynCellRef<'a>,
}

impl PartialEq<DynCellRef<'_>> for ScalarValueRef<'_> {
    fn eq(&self, other: &DynCellRef<'_>) -> bool {
        self.cells_equal(&ScalarValueRef::from_dyn(other.clone()))
    }
}

impl PartialOrd for ScalarValueRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.compare(other)
    }
}

fn signed_int_to_i128(raw: &DynCellRaw) -> Option<i128> {
    match raw {
        DynCellRaw::I8(v) => Some(i128::from(*v)),
        DynCellRaw::I16(v) => Some(i128::from(*v)),
        DynCellRaw::I32(v) => Some(i128::from(*v)),
        DynCellRaw::I64(v) => Some(i128::from(*v)),
        _ => None,
    }
}

fn unsigned_int_to_u128(raw: &DynCellRaw) -> Option<u128> {
    match raw {
        DynCellRaw::U8(v) => Some(u128::from(*v)),
        DynCellRaw::U16(v) => Some(u128::from(*v)),
        DynCellRaw::U32(v) => Some(u128::from(*v)),
        DynCellRaw::U64(v) => Some(u128::from(*v)),
        _ => None,
    }
}

impl<'a> ScalarValueRef<'a> {
    fn cells_equal_option(lhs: Option<DynCellRef<'_>>, rhs: Option<DynCellRef<'_>>) -> bool {
        match (lhs, rhs) {
            (None, None) => true,
            (Some(l), Some(r)) => {
                let lref = ScalarValueRef::from_dyn(l);
                let rref = ScalarValueRef::from_dyn(r);
                lref.cells_equal(&rref)
            }
            _ => false,
        }
    }

    /// Deep Arrow-semantic equality against another scalar reference.
    fn cells_equal(&self, rhs: &ScalarValueRef<'_>) -> bool {
        use DynCellRaw::*;
        match (self.cell.as_raw(), rhs.cell.as_raw()) {
            (Null, Null) => true,
            (Bool(a), Bool(b)) => a == b,
            (I64(a), I64(b)) => a == b,
            (U64(a), U64(b)) => a == b,
            (F64(a), F64(b)) => a.to_bits() == b.to_bits(),
            (Str { ptr: ap, len: al }, Str { ptr: bp, len: bl }) => unsafe {
                std::slice::from_raw_parts(ap.as_ptr() as *const u8, *al)
                    == std::slice::from_raw_parts(bp.as_ptr() as *const u8, *bl)
            },
            (Bin { ptr: ap, len: al }, Bin { ptr: bp, len: bl }) => unsafe {
                std::slice::from_raw_parts(ap.as_ptr() as *const u8, *al)
                    == std::slice::from_raw_parts(bp.as_ptr() as *const u8, *bl)
            },
            _ => {
                if let (Some(ls), Some(rs)) = (self.cell.as_struct(), rhs.cell.as_struct()) {
                    if ls.len() != rs.len() {
                        return false;
                    }
                    for idx in 0..ls.len() {
                        let l = ls.get(idx).ok().flatten();
                        let r = rs.get(idx).ok().flatten();
                        if !ScalarValueRef::cells_equal_option(l, r) {
                            return false;
                        }
                    }
                    return true;
                }
                if let (Some(ll), Some(rl)) = (self.cell.as_list(), rhs.cell.as_list()) {
                    if ll.len() != rl.len() {
                        return false;
                    }
                    for idx in 0..ll.len() {
                        let l = ll.get(idx).ok().flatten();
                        let r = rl.get(idx).ok().flatten();
                        if !ScalarValueRef::cells_equal_option(l, r) {
                            return false;
                        }
                    }
                    return true;
                }
                if let (Some(lf), Some(rf)) = (
                    self.cell.as_fixed_size_list(),
                    rhs.cell.as_fixed_size_list(),
                ) {
                    if lf.len() != rf.len() {
                        return false;
                    }
                    for idx in 0..lf.len() {
                        let l = lf.get(idx).ok().flatten();
                        let r = rf.get(idx).ok().flatten();
                        if !ScalarValueRef::cells_equal_option(l, r) {
                            return false;
                        }
                    }
                    return true;
                }
                if let (Some(lm), Some(rm)) = (self.cell.as_map(), rhs.cell.as_map()) {
                    if lm.len() != rm.len() {
                        return false;
                    }
                    for idx in 0..lm.len() {
                        let l = lm.get(idx).ok();
                        let r = rm.get(idx).ok();
                        let (lk, lv) = match l {
                            Some(pair) => pair,
                            None => return false,
                        };
                        let (rk, rv) = match r {
                            Some(pair) => pair,
                            None => return false,
                        };
                        if !ScalarValueRef::cells_equal_option(Some(lk), Some(rk))
                            || !ScalarValueRef::cells_equal_option(lv, rv)
                        {
                            return false;
                        }
                    }
                    return true;
                }
                if let (Some(lu), Some(ru)) = (self.cell.as_union(), rhs.cell.as_union()) {
                    if lu.type_id() != ru.type_id() {
                        return false;
                    }
                    let lval = lu.value().ok().flatten();
                    let rval = ru.value().ok().flatten();
                    return ScalarValueRef::cells_equal_option(lval, rval);
                }
                false
            }
        }
    }

    /// Returns true when the literal is the `Null` variant.
    #[must_use]
    pub fn is_null(&self) -> bool {
        self.cell.is_null()
    }

    /// Compares this scalar with another, returning the ordering when both sides are comparable.
    pub fn compare(&self, other: &ScalarValueRef<'_>) -> Option<Ordering> {
        use DynCellRaw::*;
        match (self.cell.as_raw(), other.cell.as_raw()) {
            (Null, _) | (_, Null) => None,
            (Bool(lhs), Bool(rhs)) => Some(lhs.cmp(rhs)),
            (I8(lhs), I8(rhs)) => Some(lhs.cmp(rhs)),
            (I16(lhs), I16(rhs)) => Some(lhs.cmp(rhs)),
            (I32(lhs), I32(rhs)) => Some(lhs.cmp(rhs)),
            (I64(lhs), I64(rhs)) => Some(lhs.cmp(rhs)),
            (U8(lhs), U8(rhs)) => Some(lhs.cmp(rhs)),
            (U16(lhs), U16(rhs)) => Some(lhs.cmp(rhs)),
            (U32(lhs), U32(rhs)) => Some(lhs.cmp(rhs)),
            (U64(lhs), U64(rhs)) => Some(lhs.cmp(rhs)),
            (F32(lhs), F32(rhs)) => lhs.partial_cmp(rhs),
            (F64(lhs), F64(rhs)) => lhs.partial_cmp(rhs),
            (Str { ptr: lp, len: ll }, Str { ptr: rp, len: rl }) => {
                let l = unsafe { std::slice::from_raw_parts(lp.as_ptr() as *const u8, *ll) };
                let r = unsafe { std::slice::from_raw_parts(rp.as_ptr() as *const u8, *rl) };
                Some(l.cmp(r))
            }
            (Bin { ptr: lp, len: ll }, Bin { ptr: rp, len: rl }) => {
                let l = unsafe { std::slice::from_raw_parts(lp.as_ptr() as *const u8, *ll) };
                let r = unsafe { std::slice::from_raw_parts(rp.as_ptr() as *const u8, *rl) };
                Some(l.cmp(r))
            }
            _ => {
                // Allow mixed-width numeric comparisons when both sides are ints of the same sign.
                if let (Some(lhs), Some(rhs)) = (
                    signed_int_to_i128(self.cell.as_raw()),
                    signed_int_to_i128(other.cell.as_raw()),
                ) {
                    return Some(lhs.cmp(&rhs));
                }
                if let (Some(lhs), Some(rhs)) = (
                    unsigned_int_to_u128(self.cell.as_raw()),
                    unsigned_int_to_u128(other.cell.as_raw()),
                ) {
                    return Some(lhs.cmp(&rhs));
                }
                self.cells_equal(other).then_some(Ordering::Equal)
            }
        }
    }

    /// Extract as `bool` when possible.
    pub fn as_bool(&self) -> Option<bool> {
        self.cell.as_bool()
    }

    /// Extract as signed integer across supported widths.
    pub fn as_int_i128(&self) -> Option<i128> {
        match self.cell.as_raw() {
            DynCellRaw::I8(v) => Some(i128::from(*v)),
            DynCellRaw::I16(v) => Some(i128::from(*v)),
            DynCellRaw::I32(v) => Some(i128::from(*v)),
            DynCellRaw::I64(v) => Some(i128::from(*v)),
            _ => None,
        }
    }

    /// Extract as unsigned integer across supported widths.
    pub fn as_uint_u128(&self) -> Option<u128> {
        match self.cell.as_raw() {
            DynCellRaw::U8(v) => Some(u128::from(*v)),
            DynCellRaw::U16(v) => Some(u128::from(*v)),
            DynCellRaw::U32(v) => Some(u128::from(*v)),
            DynCellRaw::U64(v) => Some(u128::from(*v)),
            _ => None,
        }
    }

    /// Extract as 64-bit floating point.
    pub fn as_f64(&self) -> Option<f64> {
        match self.cell.as_raw() {
            DynCellRaw::F32(value) => Some(f64::from(*value)),
            DynCellRaw::F64(value) => Some(*value),
            _ => None,
        }
    }

    /// Extract as string slice.
    pub fn as_utf8(&self) -> Option<&'a str> {
        self.cell.as_str()
    }

    /// Extract as binary slice.
    pub fn as_binary(&self) -> Option<&'a [u8]> {
        self.cell.as_bin()
    }

    /// Access the underlying dynamic cell reference.
    #[must_use]
    pub fn as_dyn(&self) -> DynCellRef<'a> {
        self.cell.clone()
    }

    /// Construct from a dynamic cell reference.
    pub fn from_dyn(cell: DynCellRef<'a>) -> Self {
        Self { cell }
    }
}

impl<'a> PartialEq for ScalarValueRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self.is_null(), other.is_null()) {
            (true, true) => true,
            _ => self
                .compare(other)
                .map(|ord| ord == Ordering::Equal)
                .unwrap_or(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use typed_arrow_dyn::DynCell;

    use super::*;

    fn scalar_ref<'a>(cell: &'a DynCell) -> ScalarValueRef<'a> {
        ScalarValueRef::from_dyn(cell.as_ref().expect("scalar cell"))
    }

    #[test]
    fn scalar_value_null_and_dyn_access() {
        let null = ScalarValue::null();
        assert!(null.is_null());
        assert!(null.as_ref().is_null());
        assert_eq!(null.compare(&ScalarValue::from(1i64)), None);
        assert!(matches!(null.as_dyn(), DynCell::Null));
        assert!(matches!(null.clone().into_dyn(), DynCell::Null));
    }

    #[test]
    fn scalar_value_conversions_and_equality() {
        let bool_val = ScalarValue::from(true);
        assert_eq!(bool_val.as_ref().as_bool(), Some(true));

        let signed = ScalarValue::from(-5i64);
        assert_eq!(signed.as_ref().as_int_i128(), Some(-5));

        let unsigned = ScalarValue::from(42u64);
        assert_eq!(unsigned.as_ref().as_uint_u128(), Some(42));

        let float_val = ScalarValue::from(1.25f64);
        assert_eq!(float_val.as_ref().as_f64(), Some(1.25));

        let text = ScalarValue::from("hi");
        assert_eq!(text.as_ref().as_utf8(), Some("hi"));

        let bytes = ScalarValue::from(&[1u8, 2][..]);
        assert_eq!(bytes.as_ref().as_binary(), Some([1u8, 2].as_ref()));

        let nan = ScalarValue::from(f64::NAN);
        assert_eq!(nan.compare(&nan), None);
        assert_eq!(nan, ScalarValue::from(f64::NAN));

        let dyn_cell = DynCell::Str("hi".to_string());
        assert_eq!(text.as_ref(), dyn_cell.as_ref().expect("scalar cell"));
    }

    #[test]
    fn signed_and_unsigned_helpers_handle_widths() {
        assert_eq!(signed_int_to_i128(&DynCellRaw::I8(-1)), Some(-1));
        assert_eq!(signed_int_to_i128(&DynCellRaw::I16(2)), Some(2));
        assert_eq!(signed_int_to_i128(&DynCellRaw::I32(-3)), Some(-3));
        assert_eq!(signed_int_to_i128(&DynCellRaw::I64(4)), Some(4));
        assert_eq!(signed_int_to_i128(&DynCellRaw::U8(5)), None);

        assert_eq!(unsigned_int_to_u128(&DynCellRaw::U8(1)), Some(1));
        assert_eq!(unsigned_int_to_u128(&DynCellRaw::U16(2)), Some(2));
        assert_eq!(unsigned_int_to_u128(&DynCellRaw::U32(3)), Some(3));
        assert_eq!(unsigned_int_to_u128(&DynCellRaw::U64(4)), Some(4));
        assert_eq!(unsigned_int_to_u128(&DynCellRaw::I16(5)), None);
    }

    #[test]
    fn scalar_value_ref_compare_and_accessors() {
        let bool_cell = DynCell::Bool(false);
        let bool_ref = scalar_ref(&bool_cell);
        assert_eq!(bool_ref.as_bool(), Some(false));
        assert_eq!(bool_ref.compare(&bool_ref), Some(Ordering::Equal));

        let int_cell = DynCell::I32(-7);
        let int_ref = scalar_ref(&int_cell);
        assert_eq!(int_ref.as_int_i128(), Some(-7));
        assert_eq!(int_ref.compare(&int_ref), Some(Ordering::Equal));

        let uint_cell = DynCell::U32(7);
        let uint_ref = scalar_ref(&uint_cell);
        assert_eq!(uint_ref.as_uint_u128(), Some(7));
        assert_eq!(uint_ref.compare(&uint_ref), Some(Ordering::Equal));

        let float32_cell = DynCell::F32(1.5);
        let float32_ref = scalar_ref(&float32_cell);
        assert_eq!(float32_ref.as_f64(), Some(1.5));
        assert_eq!(
            float32_ref.compare(&scalar_ref(&DynCell::F32(2.0))),
            Some(Ordering::Less)
        );

        let float64_cell = DynCell::F64(2.5);
        let float64_ref = scalar_ref(&float64_cell);
        assert_eq!(float64_ref.as_f64(), Some(2.5));
        assert_eq!(
            float64_ref.compare(&scalar_ref(&DynCell::F64(3.0))),
            Some(Ordering::Less)
        );

        let text_cell = DynCell::Str("aa".to_string());
        let text_ref = scalar_ref(&text_cell);
        assert_eq!(text_ref.as_utf8(), Some("aa"));
        assert_eq!(
            text_ref.compare(&scalar_ref(&DynCell::Str("bb".to_string()))),
            Some(Ordering::Less)
        );

        let bin_cell = DynCell::Bin(vec![1u8, 2]);
        let bin_ref = scalar_ref(&bin_cell);
        assert_eq!(bin_ref.as_binary(), Some([1u8, 2].as_ref()));
        assert_eq!(
            bin_ref.compare(&scalar_ref(&DynCell::Bin(vec![1u8, 3]))),
            Some(Ordering::Less)
        );

        let signed_small = DynCell::I16(-1);
        let signed_big = DynCell::I64(1);
        assert_eq!(
            scalar_ref(&signed_small).compare(&scalar_ref(&signed_big)),
            Some(Ordering::Less)
        );

        let unsigned_small = DynCell::U16(9);
        let unsigned_big = DynCell::U64(4);
        assert_eq!(
            scalar_ref(&unsigned_small).compare(&scalar_ref(&unsigned_big)),
            Some(Ordering::Greater)
        );

        assert_eq!(
            scalar_ref(&signed_small).compare(&scalar_ref(&unsigned_small)),
            None
        );

        let null_cell = DynCell::Null;
        assert_eq!(scalar_ref(&null_cell).compare(&bool_ref), None);
    }

    #[test]
    fn scalar_value_ref_cells_equal_for_scalars() {
        let left = DynCell::F64(f64::NAN);
        let right = DynCell::F64(f64::NAN);
        assert!(scalar_ref(&left).cells_equal(&scalar_ref(&right)));

        let text_left = DynCell::Str("hi".to_string());
        let text_right = DynCell::Str("hi".to_string());
        assert!(scalar_ref(&text_left).cells_equal(&scalar_ref(&text_right)));

        let bin_left = DynCell::Bin(vec![7u8, 8]);
        let bin_right = DynCell::Bin(vec![7u8, 8]);
        assert!(scalar_ref(&bin_left).cells_equal(&scalar_ref(&bin_right)));
    }

    #[test]
    fn cells_equal_option_handles_missing() {
        assert!(ScalarValueRef::cells_equal_option(None, None));

        let left = DynCell::I64(1);
        let right = DynCell::I64(2);

        let left_ref = left.as_ref().expect("scalar cell");
        assert!(ScalarValueRef::cells_equal_option(
            Some(left_ref.clone()),
            Some(left_ref)
        ));

        let left_ref = left.as_ref().expect("scalar cell");
        let right_ref = right.as_ref().expect("scalar cell");
        assert!(!ScalarValueRef::cells_equal_option(
            Some(left_ref),
            Some(right_ref)
        ));

        let left_ref = left.as_ref().expect("scalar cell");
        assert!(!ScalarValueRef::cells_equal_option(Some(left_ref), None));
    }
}
