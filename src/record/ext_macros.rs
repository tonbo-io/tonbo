//! typed-arrow-derive extension macros for ergonomic schema-time key definitions.
//!
//! These macros hook into typed-arrow's `ext-hooks` to implement our
//! `Record` trait automatically from annotations on the derived
//! `typed_arrow::Record` structs.
//!
//! Usage (single key on a field):
//!
//! - On the struct: `#[record(field_macro = crate::key_field)]`
//! - On the key field: `#[record(ext(key))]`
//!
//! This generates an impl of `crate::record::Record` with a unified `Key`:
//! - numeric/bool keys use the primitive type
//! - string/binary keys use zero-copy owning keys (`StrKey`/`BinKey`)
//!
//! Usage (composite key on the record):
//!
//! - On the struct: `#[record(record_macro = crate::composite_key, ext((user_id: u64), (ts: i64),
//!   (deleted: bool)))]`
//!
//! This generates an impl of `crate::record::Record` with:
//! - `Key` as a tuple mapping `String->StrKey`, `Vec<u8>->BinKey`, others unchanged.

/// Per-field macro: implement `Record` when a field is marked as key.
///
/// Valid key: non-nullable, non-nested. Supports numeric/bool directly,
/// `String` via `StrKey`, and `Vec<u8>` via `BinKey`.
#[macro_export]
macro_rules! key_field {
    // String key (non-null, non-nested)
    (owner = $owner:ty, index = $idx:tt, field = $fname:ident, ty = String, nullable = false, ext = (key $($rest:tt)*)) => {
        impl $crate::record::Record for $owner {
            type Key = $crate::inmem::immutable::keys::StrKey;
            type Payload = $owner;
            #[inline]
            fn key(&self) -> Self::Key { $crate::inmem::immutable::keys::StrKey::from_str(&self.$fname) }
            #[inline]
            fn split(mut self) -> (Self::Key, Self::Payload) {
                let k = $crate::inmem::immutable::keys::StrKey::from_string_owned(::std::mem::take(&mut self.$fname));
                (k, self)
            }
            #[inline]
            fn key_at(
                arrays: &<<Self as typed_arrow::schema::BuildRows>::Builders as typed_arrow::schema::RowBuilder<Self>>::Arrays,
                row: usize,
            ) -> Self::Key {
                $crate::inmem::immutable::keys::StrKey::from_string_array(&arrays.$fname, row)
            }
            #[inline]
            fn join(key: Self::Key, mut payload: Self::Payload) -> Self {
                payload.$fname = key.into_string();
                payload
            }
        }
    };
    // Binary key (`Vec<u8>`) (non-null, non-nested)
    (owner = $owner:ty, index = $idx:tt, field = $fname:ident, ty = Vec < u8 >, nullable = false, ext = (key $($rest:tt)*)) => {
        impl $crate::record::Record for $owner {
            type Key = $crate::inmem::immutable::keys::BinKey;
            type Payload = $owner;
            #[inline]
            fn key(&self) -> Self::Key { $crate::inmem::immutable::keys::BinKey::from_bytes(&self.$fname) }
            #[inline]
            fn split(mut self) -> (Self::Key, Self::Payload) {
                let k = $crate::inmem::immutable::keys::BinKey::from_vec_owned(::std::mem::take(&mut self.$fname));
                (k, self)
            }
            #[inline]
            fn key_at(
                arrays: &<<Self as typed_arrow::schema::BuildRows>::Builders as typed_arrow::schema::RowBuilder<Self>>::Arrays,
                row: usize,
            ) -> Self::Key {
                $crate::inmem::immutable::keys::BinKey::from_binary_array(&arrays.$fname, row)
            }
            #[inline]
            fn join(key: Self::Key, mut payload: Self::Payload) -> Self {
                payload.$fname = key.into_vec();
                payload
            }
        }
    };
    // Reject nullable keys
    (owner = $owner:ty, index = $idx:tt, field = $fname:ident, ty = $ty:ty, nullable = true, ext = (key $($rest:tt)*)) => {
        compile_error!("#[record(ext(key))] field cannot be nullable");
    };
    // Numeric/bool default (non-null, non-nested)
    (owner = $owner:ty, index = $idx:tt, field = $fname:ident, ty = $ty:ty, nullable = false, ext = (key $($rest:tt)*)) => {
        impl $crate::record::Record for $owner {
            type Key = $ty;
            type Payload = $owner;
            #[inline]
            fn key(&self) -> Self::Key { self.$fname }
            #[inline]
            fn split(self) -> (Self::Key, Self::Payload) { (self.$fname, self) }
            #[inline]
            fn key_at(
                arrays: &<<Self as typed_arrow::schema::BuildRows>::Builders as typed_arrow::schema::RowBuilder<Self>>::Arrays,
                row: usize,
            ) -> Self::Key {
                arrays.$fname.value(row)
            }
            #[inline]
            fn join(key: Self::Key, mut payload: Self::Payload) -> Self {
                payload.$fname = key;
                payload
            }
        }
    };
    // Default: no-op for other fields or other ext tags
    (owner = $owner:ty, index = $idx:tt, field = $fname:ident, ty = $ty:ty, nullable = $nul:expr, ext = ($($rest:tt)*)) => {};
}

/// Record macro: composite key using an explicit, typed list of fields.
///
/// Example:
/// `#[record(record_macro = crate::composite_key, ext((user_id: u64), (ts: i64), (deleted:
/// bool)))]`
///
/// Supports `String` and `Vec<u8>` by mapping to `StrKey`/`BinKey` in `Key`.
#[macro_export]
macro_rules! composite_key {
    // Entry with 1+ typed items
    (owner = $owner:ty, len = $len:expr, ext = ( $( ($fname:ident : $fty:ty) ),+ $(,)? )) => {
        impl $crate::record::Record for $owner {
            // Key tuple maps String->StrKey, Vec<u8>->BinKey, else identity
            type Key = (
                $(
                    $crate::__macros__map_slice_ty!($fty)
                ),+
            );
            type Payload = $owner;

            #[inline]
            fn key(&self) -> Self::Key {
                (
                    $(
                        $crate::__macros__make_owning_key!(self, $fname, $fty)
                    ),+
                )
            }

            #[inline]
            fn split(self) -> (Self::Key, Self::Payload) {
                let k = (
                    $(
                        $crate::__macros__take_owning_key!(self, $fname, $fty)
                    ),+
                );
                (k, self)
            }

            #[inline]
            fn key_at(
                arrays: &<<Self as typed_arrow::schema::BuildRows>::Builders as typed_arrow::schema::RowBuilder<Self>>::Arrays,
                row: usize,
            ) -> Self::Key {
                (
                    $(
                        $crate::__macros__make_slice_key!(arrays, row, $fname, $fty)
                    ),+
                )
            }
            #[inline]
            fn join(key: Self::Key, mut payload: Self::Payload) -> Self {
                let ($($fname),+) = key;
                $(
                    $crate::__macros__assign_key_field!(payload, $fname, $fty);
                )+
                payload
            }
        }
    };
    // Entry with ident-only ext: no-op here; type-aware work is done in `composite_record!`.
    (owner = $owner:ty, len = $len:expr, ext = ( $first:ident $(, $rest:ident )* $(,)? )) => {};
    // Empty ext list is invalid
    (owner = $owner:ty, len = $len:expr, ext = ()) => {
        compile_error!("composite_key requires at least one (field: Type) in ext(...)");
    };
}

// Internal helpers for composite_key
#[doc(hidden)]
#[macro_export]
macro_rules! __macros__map_slice_ty {
    (String) => {
        $crate::inmem::immutable::keys::StrKey
    };
    (std :: string :: String) => {
        $crate::inmem::immutable::keys::StrKey
    };
    (alloc :: string :: String) => {
        $crate::inmem::immutable::keys::StrKey
    };
    (& str) => {
        $crate::inmem::immutable::keys::StrKey
    };
    (Vec < u8 >) => {
        $crate::inmem::immutable::keys::BinKey
    };
    (std :: vec :: Vec < u8 >) => {
        $crate::inmem::immutable::keys::BinKey
    };
    (alloc :: vec :: Vec < u8 >) => {
        $crate::inmem::immutable::keys::BinKey
    };
    (& [ u8 ]) => {
        $crate::inmem::immutable::keys::BinKey
    };
    ($t:ty) => {
        $t
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __macros__make_slice_key {
    ( $arrays:ident, $row:ident, $fname:ident, String ) => {
        $crate::inmem::immutable::keys::StrKey::from_string_array(&$arrays.$fname, $row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, std :: string :: String ) => {
        $crate::inmem::immutable::keys::StrKey::from_string_array(&$arrays.$fname, $row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, alloc :: string :: String ) => {
        $crate::inmem::immutable::keys::StrKey::from_string_array(&$arrays.$fname, $row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, & str ) => {
        $crate::inmem::immutable::keys::StrKey::from_string_array(&$arrays.$fname, $row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, Vec < u8 > ) => {
        $crate::inmem::immutable::keys::BinKey::from_binary_array(&$arrays.$fname, $row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, std :: vec :: Vec < u8 > ) => {
        $crate::inmem::immutable::keys::BinKey::from_binary_array(&$arrays.$fname, $row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, alloc :: vec :: Vec < u8 > ) => {
        $crate::inmem::immutable::keys::BinKey::from_binary_array(&$arrays.$fname, $row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, & [ u8 ] ) => {
        $crate::inmem::immutable::keys::BinKey::from_binary_array(&$arrays.$fname, $row)
    };
    // Common integers
    ( $arrays:ident, $row:ident, $fname:ident, u8 ) => {
        $arrays.$fname.value($row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, u16 ) => {
        $arrays.$fname.value($row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, u32 ) => {
        $arrays.$fname.value($row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, u64 ) => {
        $arrays.$fname.value($row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, usize ) => {
        $arrays.$fname.value($row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, i8 ) => {
        $arrays.$fname.value($row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, i16 ) => {
        $arrays.$fname.value($row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, i32 ) => {
        $arrays.$fname.value($row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, i64 ) => {
        $arrays.$fname.value($row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, isize ) => {
        $arrays.$fname.value($row)
    };
    // Floats and bool
    ( $arrays:ident, $row:ident, $fname:ident, f32 ) => {
        $arrays.$fname.value($row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, f64 ) => {
        $arrays.$fname.value($row)
    };
    ( $arrays:ident, $row:ident, $fname:ident, bool ) => {
        $arrays.$fname.value($row)
    };
    // Fallback
    ( $arrays:ident, $row:ident, $fname:ident, $t:ty ) => {
        $arrays.$fname.value($row)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __macros__make_owning_key {
    ( $self:ident, $fname:ident, String ) => {
        $crate::inmem::immutable::keys::StrKey::from_str(&$self.$fname)
    };
    ( $self:ident, $fname:ident, std :: string :: String ) => {
        $crate::inmem::immutable::keys::StrKey::from_str(&$self.$fname)
    };
    ( $self:ident, $fname:ident, alloc :: string :: String ) => {
        $crate::inmem::immutable::keys::StrKey::from_str(&$self.$fname)
    };
    ( $self:ident, $fname:ident, & str ) => {
        $crate::inmem::immutable::keys::StrKey::from_str($self.$fname)
    };
    ( $self:ident, $fname:ident, Vec < u8 > ) => {
        $crate::inmem::immutable::keys::BinKey::from_bytes(&$self.$fname)
    };
    ( $self:ident, $fname:ident, std :: vec :: Vec < u8 > ) => {
        $crate::inmem::immutable::keys::BinKey::from_bytes(&$self.$fname)
    };
    ( $self:ident, $fname:ident, alloc :: vec :: Vec < u8 > ) => {
        $crate::inmem::immutable::keys::BinKey::from_bytes(&$self.$fname)
    };
    ( $self:ident, $fname:ident, & [ u8 ] ) => {
        $crate::inmem::immutable::keys::BinKey::from_bytes($self.$fname)
    };
    ( $self:ident, $fname:ident, $t:ty ) => {
        $self.$fname.clone()
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __macros__take_owning_key {
    ( $self:ident, $fname:ident, String ) => {
        $crate::inmem::immutable::keys::StrKey::from_string_owned(::std::mem::take(
            &mut $self.$fname,
        ))
    };
    ( $self:ident, $fname:ident, std :: string :: String ) => {
        $crate::inmem::immutable::keys::StrKey::from_string_owned(::std::mem::take(
            &mut $self.$fname,
        ))
    };
    ( $self:ident, $fname:ident, alloc :: string :: String ) => {
        $crate::inmem::immutable::keys::StrKey::from_string_owned(::std::mem::take(
            &mut $self.$fname,
        ))
    };
    ( $self:ident, $fname:ident, & str ) => {
        $crate::inmem::immutable::keys::StrKey::from_str($self.$fname)
    };
    ( $self:ident, $fname:ident, Vec < u8 > ) => {
        $crate::inmem::immutable::keys::BinKey::from_vec_owned(::std::mem::take(&mut $self.$fname))
    };
    ( $self:ident, $fname:ident, std :: vec :: Vec < u8 > ) => {
        $crate::inmem::immutable::keys::BinKey::from_vec_owned(::std::mem::take(&mut $self.$fname))
    };
    ( $self:ident, $fname:ident, alloc :: vec :: Vec < u8 > ) => {
        $crate::inmem::immutable::keys::BinKey::from_vec_owned(::std::mem::take(&mut $self.$fname))
    };
    ( $self:ident, $fname:ident, & [ u8 ] ) => {
        $crate::inmem::immutable::keys::BinKey::from_bytes($self.$fname)
    };
    ( $self:ident, $fname:ident, $t:ty ) => {
        $self.$fname.clone()
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __macros__assign_key_field {
    ( $payload:ident, $k:ident, String ) => {
        $payload.$k = $k.into_string();
    };
    ( $payload:ident, $k:ident, std :: string :: String ) => {
        $payload.$k = $k.into_string();
    };
    ( $payload:ident, $k:ident, alloc :: string :: String ) => {
        $payload.$k = $k.into_string();
    };
    ( $payload:ident, $k:ident, & str ) => {
        $payload.$k = $k.to_owned();
    };
    ( $payload:ident, $k:ident, Vec < u8 > ) => {
        $payload.$k = $k.into_vec();
    };
    ( $payload:ident, $k:ident, std :: vec :: Vec < u8 > ) => {
        $payload.$k = $k.into_vec();
    };
    ( $payload:ident, $k:ident, alloc :: vec :: Vec < u8 > ) => {
        $payload.$k = $k.into_vec();
    };
    ( $payload:ident, $k:ident, & [ u8 ] ) => {
        $payload.$k = $k.to_vec();
    };
    ( $payload:ident, $k:ident, $t:ty ) => {
        $payload.$k = $k;
    };
}

/// Helper macro: implement `Record` for a type with a composite key using a
/// typed field list. This bypasses typed-arrow's `record_macro` grammar limits
/// and can be used alongside `#[derive(typed_arrow::Record)]`.
///
/// Example:
///
/// impl_composite_record!(TwoKeys, (user_id: u64), (ts: i64), (deleted: bool));
#[macro_export]
macro_rules! impl_composite_record {
    ( $owner:ty, $( ($fname:ident : $fty:ty) ),+ $(,)? ) => {
        $crate::composite_record!($owner, $( ($fname : $fty) ),+ );
    }
}

/// Record-fields macro: generate a full `Record` impl from a typed list of
/// (field, type) pairs. Intended to be called by typed-arrow derive via
/// `record_fields_macro = crate::composite_record` alongside
/// `record_macro = crate::composite_key` with `ext(field1, field2, ...)`.
#[macro_export]
macro_rules! composite_record {
    ( $owner:ty, $( ($fname:ident : $fty:ty) ),+ $(,)? ) => {
        $crate::composite_key!(owner = $owner, len = 0usize, ext = ( $( ($fname : $fty) ),+ ));
    }
}

#[cfg(test)]
mod tests {
    use crate::{inmem::immutable::memtable::segment_from_rows, record::Record};

    // Single numeric key via field macro
    #[derive(typed_arrow::Record)]
    #[record(field_macro = crate::key_field)]
    struct OneKey {
        #[record(ext(key))]
        id: u32,
    }

    // Ensure impl is generated and works with ImmutableMemTable lookup
    #[test]
    fn key_field_numeric_immutable_offsets() {
        let rows = vec![OneKey { id: 2 }, OneKey { id: 1 }, OneKey { id: 3 }];
        let imm = segment_from_rows::<OneKey, _>(rows);
        assert_eq!(imm.get_offset(&1), Some(1));
        assert_eq!(imm.get_offset(&2), Some(0));
        assert_eq!(imm.get_offset(&3), Some(2));
    }

    // Single string key via field macro -> StrKey slice
    #[derive(typed_arrow::Record, Clone)]
    #[record(field_macro = crate::key_field)]
    struct User {
        #[record(ext(key))]
        id: String,
        score: i32,
    }

    #[test]
    fn key_field_string_immutable_index() {
        let rows = vec![
            User {
                id: "a".into(),
                score: 1,
            },
            User {
                id: "b".into(),
                score: 2,
            },
            User {
                id: "b".into(),
                score: 3,
            },
        ];
        let imm = segment_from_rows::<User, _>(rows);
        assert_eq!(imm.get_offset("a"), Some(0));
        assert_eq!(imm.get_offset("b"), Some(2));
    }

    // Single binary key via field macro -> BinKey slice
    #[derive(typed_arrow::Record, Clone)]
    #[record(field_macro = crate::key_field)]
    struct Blob {
        #[record(ext(key))]
        id: Vec<u8>,
        size: u32,
    }

    #[test]
    fn key_field_binary_immutable_index() {
        let rows = vec![
            Blob {
                id: b"a".to_vec(),
                size: 1,
            },
            Blob {
                id: b"b".to_vec(),
                size: 2,
            },
            Blob {
                id: b"b".to_vec(),
                size: 3,
            },
        ];
        let imm = segment_from_rows::<Blob, _>(rows);
        assert_eq!(imm.get_offset(&b"a"[..]), Some(0));
        assert_eq!(imm.get_offset(&b"b"[..]), Some(2));
    }

    // Inline composite-key using record_fields_macro + record_macro.
    #[derive(typed_arrow::Record, Debug, Clone)]
    #[record(record_fields_macro = crate::composite_record, record_macro = crate::composite_key, ext(user_id, ts, deleted))]
    struct TwoKeysInline {
        user_id: u64,
        ts: i64,
        deleted: bool,
    }

    #[test]
    fn composite_inline_typed_offsets() {
        let rows = vec![
            TwoKeysInline {
                user_id: 1,
                ts: 10,
                deleted: false,
            },
            TwoKeysInline {
                user_id: 1,
                ts: 10,
                deleted: true,
            },
            TwoKeysInline {
                user_id: 1,
                ts: 20,
                deleted: false,
            },
            TwoKeysInline {
                user_id: 2,
                ts: 5,
                deleted: false,
            },
        ];
        let imm = crate::inmem::immutable::memtable::segment_from_rows::<TwoKeysInline, _>(rows);
        let k1 = TwoKeysInline {
            user_id: 1,
            ts: 10,
            deleted: false,
        }
        .key();
        let k2 = TwoKeysInline {
            user_id: 1,
            ts: 10,
            deleted: true,
        }
        .key();
        let k3 = TwoKeysInline {
            user_id: 1,
            ts: 20,
            deleted: false,
        }
        .key();
        let k4 = TwoKeysInline {
            user_id: 2,
            ts: 5,
            deleted: false,
        }
        .key();
        assert_eq!(imm.get_offset(&k1), Some(0));
        assert_eq!(imm.get_offset(&k2), Some(1));
        assert_eq!(imm.get_offset(&k3), Some(2));
        assert_eq!(imm.get_offset(&k4), Some(3));
    }
}
