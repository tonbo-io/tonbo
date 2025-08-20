use crate::inmem::immutable::keys::{BinKey, StrKey};

/// A dynamically typed key used for runtime-schema indexing.
///
/// - Ordering groups by variant and compares values within the same variant.
/// - Floating-point equality treats NaN == NaN to satisfy `Eq` for map keys.
/// - External users can construct string/binary keys via `From<&str>`, `From<String>`,
///   `From<&[u8]>`, and `From<Vec<u8>>` without touching crate-internal key types.
#[derive(Clone, Debug)]
pub enum KeyDyn {
    /// UTF-8 string key.
    Str(StrKey),
    /// Arbitrary binary key.
    Bin(BinKey),
    /// 64-bit unsigned integer key.
    U64(u64),
    /// 32-bit unsigned integer key.
    U32(u32),
    /// 64-bit signed integer key.
    I64(i64),
    /// 32-bit signed integer key.
    I32(i32),
    /// 64-bit floating point key.
    F64(f64),
    /// 32-bit floating point key.
    F32(f32),
    /// Boolean key.
    Bool(bool),
    /// Composite tuple key with lexicographic ordering.
    Tuple(Vec<KeyDyn>),
}

impl std::cmp::Ord for KeyDyn {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use KeyDyn::*;
        let tag = |k: &KeyDyn| match k {
            Str(_) => 0,
            Bin(_) => 1,
            U64(_) => 2,
            U32(_) => 3,
            I64(_) => 4,
            I32(_) => 5,
            F64(_) => 6,
            F32(_) => 7,
            Bool(_) => 8,
            Tuple(_) => 9,
        };
        match (self, other) {
            (Str(a), Str(b)) => a.cmp(b),
            (Bin(a), Bin(b)) => a.cmp(b),
            (U64(a), U64(b)) => a.cmp(b),
            (U32(a), U32(b)) => a.cmp(b),
            (I64(a), I64(b)) => a.cmp(b),
            (I32(a), I32(b)) => a.cmp(b),
            (F64(a), F64(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            (F32(a), F32(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            (Bool(a), Bool(b)) => a.cmp(b),
            (Tuple(a), Tuple(b)) => {
                let mut i = 0;
                loop {
                    match (a.get(i), b.get(i)) {
                        (Some(ak), Some(bk)) => {
                            let c = ak.cmp(bk);
                            if c != std::cmp::Ordering::Equal {
                                break c;
                            }
                        }
                        (None, Some(_)) => break std::cmp::Ordering::Less,
                        (Some(_), None) => break std::cmp::Ordering::Greater,
                        (None, None) => break std::cmp::Ordering::Equal,
                    }
                    i += 1;
                }
            }
            (a, b) => tag(a).cmp(&tag(b)),
        }
    }
}

impl std::cmp::PartialOrd for KeyDyn {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::PartialEq for KeyDyn {
    fn eq(&self, other: &Self) -> bool {
        use KeyDyn::*;
        match (self, other) {
            (Str(a), Str(b)) => a == b,
            (Bin(a), Bin(b)) => a == b,
            (U64(a), U64(b)) => a == b,
            (U32(a), U32(b)) => a == b,
            (I64(a), I64(b)) => a == b,
            (I32(a), I32(b)) => a == b,
            (F64(a), F64(b)) => (a.is_nan() && b.is_nan()) || a == b,
            (F32(a), F32(b)) => (a.is_nan() && b.is_nan()) || a == b,
            (Bool(a), Bool(b)) => a == b,
            (Tuple(a), Tuple(b)) => {
                if a.len() != b.len() {
                    return false;
                }
                for (ak, bk) in a.iter().zip(b.iter()) {
                    if ak != bk {
                        return false;
                    }
                }
                true
            }
            _ => false,
        }
    }
}

impl std::cmp::Eq for KeyDyn {}

impl From<StrKey> for KeyDyn {
    fn from(v: StrKey) -> Self {
        KeyDyn::Str(v)
    }
}
impl From<BinKey> for KeyDyn {
    fn from(v: BinKey) -> Self {
        KeyDyn::Bin(v)
    }
}
impl From<&str> for KeyDyn {
    fn from(v: &str) -> Self {
        KeyDyn::Str(StrKey::from_str(v))
    }
}
impl From<String> for KeyDyn {
    fn from(v: String) -> Self {
        KeyDyn::Str(StrKey::from_string_owned(v))
    }
}
impl From<&[u8]> for KeyDyn {
    fn from(v: &[u8]) -> Self {
        KeyDyn::Bin(BinKey::from_bytes(v))
    }
}
impl From<Vec<u8>> for KeyDyn {
    fn from(v: Vec<u8>) -> Self {
        KeyDyn::Bin(BinKey::from_vec_owned(v))
    }
}
impl From<u64> for KeyDyn {
    fn from(v: u64) -> Self {
        KeyDyn::U64(v)
    }
}
impl From<u32> for KeyDyn {
    fn from(v: u32) -> Self {
        KeyDyn::U32(v)
    }
}
impl From<i64> for KeyDyn {
    fn from(v: i64) -> Self {
        KeyDyn::I64(v)
    }
}
impl From<i32> for KeyDyn {
    fn from(v: i32) -> Self {
        KeyDyn::I32(v)
    }
}
impl From<f64> for KeyDyn {
    fn from(v: f64) -> Self {
        KeyDyn::F64(v)
    }
}
impl From<f32> for KeyDyn {
    fn from(v: f32) -> Self {
        KeyDyn::F32(v)
    }
}
impl From<bool> for KeyDyn {
    fn from(v: bool) -> Self {
        KeyDyn::Bool(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nan_equality_and_ordering_tags() {
        let a = KeyDyn::from(f64::NAN);
        let b = KeyDyn::from(f64::NAN);
        assert_eq!(a, b, "NaN treated equal for map keys");

        // Simple cross-variant ordering checks follow tag ordering
        let s = KeyDyn::from("a");
        let bin = KeyDyn::from(&b"x"[..]);
        let u64k = KeyDyn::from(1u64);
        assert!(s < bin);
        assert!(bin < u64k);
        // Tuple sorts after Bool by tag; unrelated but consistent
        let tup = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(1i32)]);
        let b = KeyDyn::from(true);
        assert!(b < tup);
    }

    #[test]
    fn from_conversions() {
        let s1 = KeyDyn::from("hi");
        let s2 = KeyDyn::from("hi".to_string());
        assert_eq!(s1, s2);

        let b1 = KeyDyn::from(&b"xy"[..]);
        let b2 = KeyDyn::from(vec![b'x', b'y']);
        assert_eq!(b1, b2);

        // Tuple lexicographic compare
        let t1 = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(1i64)]);
        let t2 = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(2i64)]);
        assert!(t1 < t2);
    }
}
