# typed-arrow

<p align="left">
  <a href="https://github.com/tonbo-io/typed-arrow" target="_blank">
    <a href="https://crates.io/crates/typed-arrow/"><img src="https://img.shields.io/crates/v/typed-arrow.svg"></a>
    <a href="https://github.com/tonbo-io/tonbo/blob/main/LICENSE"><img src="https://img.shields.io/crates/l/tonbo"></a>
    <a href="https://discord.gg/j27XVFVmJM"><img src="https://img.shields.io/discord/1270294987355197460?logo=discord"></a>
  </a>
</p>

typed-arrow provides a strongly typed, fully compile-time way to declare Arrow schemas in Rust. It maps Rust types directly to arrow-rs typed builders/arrays and `arrow_schema::DataType` — without any runtime `DataType` switching — enabling zero runtime cost, monomorphized column construction and ergonomic ORM-like APIs.

## Why compile-time Arrow?

- Performance: monomorphized builders/arrays with zero dynamic dispatch; avoids runtime `DataType` matching.
- Safety: column types, names, and nullability live in the type system; mismatches fail at compile time.
- Interop: uses `arrow-array`/`arrow-schema` types directly; no bespoke runtime layer to learn.

## Quick Start

```rust
use typed_arrow::{prelude::*, schema::SchemaMeta};
use typed_arrow::{Dictionary, TimestampTz, Millisecond, Utc, List};

#[derive(typed_arrow::Record)]
struct Address { city: String, zip: Option<i32> }

#[derive(typed_arrow::Record)]
struct Person {
    id: i64,
    address: Option<Address>,
    tags: Option<List<Option<i32>>>,          // List column with nullable items
    code: Option<Dictionary<i32, String>>,    // Dictionary<i32, Utf8>
    joined: TimestampTz<Millisecond, Utc>,    // Timestamp(ms) with timezone (UTC)
}

fn main() {
    // Build from owned rows
    let rows = vec![
        Person {
            id: 1,
            address: Some(Address { city: "NYC".into(), zip: None }),
            tags: Some(List::new(vec![Some(1), None, Some(3)])),
            code: Some(Dictionary::new("gold".into())),
            joined: TimestampTz::<Millisecond, Utc>::new(1_700_000_000_000),
        },
        Person {
            id: 2,
            address: None,
            tags: None,
            code: None,
            joined: TimestampTz::<Millisecond, Utc>::new(1_700_000_100_000),
        },
    ];

    let mut b = <Person as BuildRows>::new_builders(rows.len());
    b.append_rows(rows);
    let arrays = b.finish();

    // Compile-time schema + RecordBatch
    let batch = arrays.into_record_batch();
    assert_eq!(batch.schema().fields().len(), <Person as Record>::LEN);
    println!("rows={}, field0={}", batch.num_rows(), batch.schema().field(0).name());
}
```

Add to your `Cargo.toml` (derives enabled by default):

```toml
[dependencies]
typed-arrow = { version = "0.x" }
```

When working in this repository/workspace:

```toml
[dependencies]
typed-arrow = { path = "." }
```

## Examples

Run the included examples to see end-to-end usage:

- `01_primitives` — derive `Record`, inspect `DataType`, build primitives
- `02_lists` — `List<T>` and `List<Option<T>>`
- `03_dictionary` — `Dictionary<K, String>`
- `04_timestamps` — `Timestamp<U>` units
- `04b_timestamps_tz` — `TimestampTz<U, Z>` with `Utc` and custom markers
- `05_structs` — nested structs → `StructArray`
- `06_rows_flat` — row-based building for flat records
- `07_rows_nested` — row-based building with nested struct fields
- `08_record_batch` — compile-time schema + `RecordBatch`
- `09_duration_interval` — Duration and Interval types
- `10_union` — Dense Union as a Record column (with attributes)
- `11_map` — Map (incl. `Option<V>` values) + as a Record column
- `12_ext_hooks` — Extend `#[derive(Record)]` with visitor injection and macro callbacks

Run:

```bash
cargo run --example 08_record_batch
```

## Core Concepts

- `Record`: implemented by the derive macro for structs with named fields.
- `ColAt<I>`: per-column associated items `Rust`, `ColumnBuilder`, `ColumnArray`, `NULLABLE`, `NAME`, and `data_type()`.
- `ArrowBinding`: compile-time mapping from a Rust value type to its Arrow builder, array, and `DataType`.
- `BuildRows`: derive generates `<Type>Builders` and `<Type>Arrays` with `append_row(s)` and `finish`.
- `SchemaMeta`: derive provides `fields()` and `schema()`; arrays structs provide `into_record_batch()`.
- `AppendStruct` and `StructMeta`: enable nested struct fields and `StructArray` building.

### Metadata (Compile-time)

- Schema-level: annotate with `#[schema_metadata(k = "owner", v = "data")]`.
- Field-level: annotate with `#[metadata(k = "pii", v = "email")]`.
- You can repeat attributes to add multiple pairs; later duplicates win.

### Nested Type Wrappers

- Struct fields: struct-typed fields map to Arrow `Struct` columns by default. Make the parent field nullable with `Option<Nested>`; child nullability is independent.
- Lists: `List<T>` (items non-null) and `List<Option<T>>` (items nullable). Use `Option<List<_>>` for list-level nulls.
- LargeList: `LargeList<T>` and `LargeList<Option<T>>` for 64-bit offsets; wrap with `Option<_>` for column nulls.
- FixedSizeList: `FixedSizeList<T, N>` (items non-null) and `FixedSizeListNullable<T, N>` (items nullable). Wrap with `Option<_>` for list-level nulls.
- Map: `Map<K, V, const SORTED: bool = false>` where keys are non-null; use `Map<K, Option<V>>` to allow nullable values. Column nullability via `Option<Map<...>>`. `SORTED` sets `keys_sorted` in the Arrow `DataType`.
- OrderedMap: `OrderedMap<K, V>` uses `BTreeMap<K, V>` and declares `keys_sorted = true`.
- Dictionary: `Dictionary<K, V>` with integral keys `K ∈ { i8, i16, i32, i64, u8, u16, u32, u64 }` and values:
  - `String`/`LargeUtf8` (Utf8/LargeUtf8)
  - `Vec<u8>`/`LargeBinary` (Binary/LargeBinary)
  - `[u8; N]` (FixedSizeBinary)
  - primitives `i*`, `u*`, `f32`, `f64`
  Column nullability via `Option<Dictionary<..>>`.
- Timestamps: `Timestamp<U>` (unit-only) and `TimestampTz<U, Z>` (unit + timezone). Units: `Second`, `Millisecond`, `Microsecond`, `Nanosecond`. Use `Utc` or define your own `Z: TimeZoneSpec`.
- Decimals: `Decimal128<P, S>` and `Decimal256<P, S>` (precision `P`, scale `S` as const generics).
- Unions: `#[derive(Union)]` for enums with `#[union(mode = "dense"|"sparse")]`, per-variant `#[union(tag = N)]`, `#[union(field = "name")]`, and optional null carrier `#[union(null)]` or container-level `null_variant = "Var"`.

## Arrow DataType Coverage

Supported (arrow-rs v56):

- Primitives: Int8/16/32/64, UInt8/16/32/64, Float16/32/64, Boolean
- Strings/Binary: Utf8, LargeUtf8, Binary, LargeBinary, FixedSizeBinary (via `[u8; N]`)
- Temporal: Timestamp (with/without TZ; s/ms/us/ns), Date32/64, Time32(s/ms), Time64(us/ns), Duration(s/ms/us/ns), Interval(YearMonth/DayTime/MonthDayNano)
- Decimal: Decimal128, Decimal256 (const generic precision/scale)
- Nested:
  - List (including nullable items), LargeList, FixedSizeList (nullable/non-null items)
  - Struct,
  - Map (Vec<(K,V)>; use `Option<V>` for nullable values), OrderedMap (BTreeMap<K,V>) with `keys_sorted = true`
  - Union: Dense and Sparse (via `#[derive(Union)]` on enums)
  - Dictionary: keys = all integral types; values = Utf8 (String), LargeUtf8, Binary (Vec<u8>), LargeBinary, FixedSizeBinary (`[u8; N]`), primitives (i*, u*, f32, f64)

Missing:

- BinaryView, Utf8View
- Utf8View
- ListView, LargeListView
- RunEndEncoded

## Extensibility

- Derive extension hooks allow user-level customization without changing the core derive:
  - Inject compile-time visitors: `#[record(visit(MyVisitor))]`
  - Call your macros per field/record: `#[record(field_macro = my_ext::per_field, record_macro = my_ext::per_record)]`
  - Tag fields/records with free-form markers: `#[record(ext(key))]`
- See `docs/extensibility.md` and the runnable example `examples/12_ext_hooks.rs`.
