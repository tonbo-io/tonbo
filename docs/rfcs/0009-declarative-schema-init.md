# RFC 0009: Declarative Schema Initialisation

- Status: Draft
- Authors: Tonbo team
- Created: 2025-11-02
- Area: API, Ingest, Metadata

## Summary

Tonbo currently requires callers to construct a `DynModeConfig` with an explicit `KeyProjection` implementation in order to declare the primary key of a table. Although that flexibility is useful internally, it forces application code to reach into Tonbo-specific extractor APIs rather than expressing intent through portable Arrow schema metadata or a builder-style configuration surface. This RFC proposes two complementary ways to tell Tonbo which columns form the primary key while keeping user schemas Arrow-first: (1) schema metadata markers (`tonbo.key`, `tonbo.keys`) that the database reads during initialisation and (2) a `SchemaBuilder` API that wraps a standard Arrow schema and produces the corresponding configuration.

## Problem Statement

- **Tonbo-specific wiring** – today users of the Rust API must import `tonbo::extractor::projection_for_field` to get a `KeyProjection`, a detail that belongs in the database internals.
- **Metadata under-utilisation** – Arrow schemas already support arbitrary key/value metadata, but Tonbo treats it as optional best-effort sugar instead of the canonical declaration path.
- **Builder ergonomics** – some users prefer code-level configuration to metadata strings; the lack of a builder leaves them assembling `DynModeConfig` by hand.
- **Future composite support** – upcoming work on cascading composite keys and schema evolution will be easier if we centralise key declaration logic instead of spreading it across user code.

## Goals

1. Make metadata-driven configuration (`tonbo.key`, `tonbo.keys`) the primary way to declare primary keys while remaining compatible with existing schemas.
2. Provide a typed builder for callers who prefer configuring schemas in Rust code.
3. Hide `projection_for_field` (and related extractor constructors) from the public API once alternative pathways exist.
4. Preserve the current validation rules (`DynModeConfig::new`) so unsupported Arrow types are rejected with clear errors.

## Non-goals

- Defining schema evolution semantics or migration tooling. This RFC only covers initial declaration.
- Extending key support to new Arrow types beyond what `KeyProjection` already handles.
- Rewriting existing RFCs about zero-copy key handling (`0008`) or storage layout (`0004`); the proposal simply layers a better configuration surface on top.

## Current State

- `DynModeConfig::from_metadata` reads `tonbo.key` markers today, but metadata is optional and most samples/tests still call `projection_for_field` directly.
- `projection_for_field` is re-exported from `tonbo::extractor`, making it part of the user-facing API surface even though it's primarily an internal helper.
- There is no builder or utility to mark composite keys programmatically; clients compose `CompositeProjection` by hand if they need it.

## Proposed Design

### Metadata-first declaration

We standardise the existing metadata contract and document it clearly: (a) single-column keys set `tonbo.key = "true"` on the field; (b) composite keys with explicit ordering assign integer ordinals such as `field.metadata["tonbo.key"] = "0"`, `"1"`, ensuring cascading composite keys retain the intended precedence; (c) schema-level declarations set `schema.metadata["tonbo.keys"]` to either a comma-separated list or JSON-ish array (`["col1","col2"]`) as a fallback when field metadata is absent. The loader logic (refined inside `DynModeConfig::from_metadata`) becomes authoritative: when a user calls `DB::new` without an explicit config, Tonbo reads the schema metadata, constructs the `CompositeProjection` as needed, and rejects the schema if no key markers are present.

### `SchemaBuilder`

We add a Rust-native builder to cover code-driven setup. `SchemaBuilder::from(schema)` stores the schema and a mutable list of key parts; `.primary_key(field)` is syntactic sugar for `.composite_key([field])`; `.composite_key<I>(iterable)` accepts any iterable of strings, replaces the current key set, and preserves the caller-provided order verbatim; `.add_key_part(field)` allows incremental configuration; `.with_metadata()` optionally writes the resulting key markers back into the schema for downstream tooling. The builder validates fields eagerly and, on `build()`, calls `DynModeConfig::new(schema, extractor)` to produce the same config structure used elsewhere.

### API adjustments

- Change `projection_for_field` to `pub(crate)` and re-home any internal call sites. Tests can reach it through `crate::` paths while integration tests use metadata or the builder.
- Add `DynModeConfig::from_schema(schema: SchemaRef)` that prefers metadata and falls back to returning a descriptive error if no key markers exist. `DB::new` will use this helper.
- Expand documentation (crate-level and RFC) describing supported metadata values and builder usage, including ordering rules for cascading composite keys.

## Implementation Plan

1. Builder scaffolding – implement `SchemaBuilder`, supporting methods, unit tests, and documentation.
2. Metadata enforcement – update `DB::new` and samples to call `DynModeConfig::from_schema`, producing actionable errors when key metadata is missing.
3. Visibility tightening – make `projection_for_field` crate-private; update integration tests to rely on the new builder/helper APIs.
4. Documentation – refresh `docs/overview.md` and user guides to describe the preferred schema declaration flow and ordering semantics.

## Compatibility

- Existing code that already sets `tonbo.key` / `tonbo.keys` continues to work.
- Users who created configs via `DynModeConfig::new(schema, extractor)` can keep doing so; the RFC does not remove the escape hatch, only discourages direct extractor usage.
- Examples/tests that previously called `projection_for_field` will need minor updates to use the builder or metadata helpers.

## Alternatives Considered

- Status quo – keep exporting `projection_for_field`. Rejected because it leaks internal abstractions and obstructs future schema evolution.
- Mandatory metadata only – require metadata injection and remove all programmatic options. Rejected because some callers prefer compile-time configuration or cannot mutate shared schemas.
- New DSL – invent a Tonbo-specific schema description. Rejected because Arrow schemas already provide the necessary structure and metadata channels.

## Open Questions

1. Should the builder automatically back-populate metadata onto the schema—including explicit ordinals for composite keys—or should that remain an explicit opt-in?
2. How should we surface errors when metadata describes unsupported data types (e.g., marking a list column as a key)? Do we need more descriptive diagnostics than today’s `WrongType`?
3. Do we introduce similar builders for other mode-specific settings (e.g., partitioning, retention) as part of the same API, or keep this builder narrowly focused on key declaration?
