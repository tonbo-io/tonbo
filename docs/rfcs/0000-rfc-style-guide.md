# RFC: RFC Style Guide

- Status: Accepted
- Authors: Tonbo team
- Created: 2025-12-03
- Area: Process

## Summary

Define the structure and style for Tonbo RFCs. RFCs document design intent and semantics, not implementation details.

## Motivation

- Ensure consistent RFC structure across the project
- Make RFCs easy to navigate and review
- Keep design documents focused on *what* and *why*, not *how*

## Goals

- Establish a standard RFC template
- Define required and optional sections
- Set expectations for content style

## Non-Goals

- Prescribing implementation approaches
- Defining code review or approval processes

## Design

### Header Metadata

Every RFC starts with:

```
# RFC: <Title>

- Status: Draft | Accepted | Implementing | Superseded
- Authors: <team or individuals>
- Created: <YYYY-MM-DD>
- Updated: <YYYY-MM-DD> (if revised)
- Area: <affected components>
```

### Required Sections

| Section | Purpose |
|---------|---------|
| Summary | One paragraph describing the design |
| Motivation | Why this design is needed |
| Goals | What the design achieves |
| Non-Goals | Explicit scope boundaries |
| Design | The core design with subsections as needed |

### Optional Sections

| Section | Purpose |
|---------|---------|
| Alternatives Considered | Other approaches and why they were rejected |
| Comparison with Other Systems | How similar systems (Iceberg, RocksDB, etc.) solve the same problem and trade-offs of our approach |
| Future Work | Known limitations and planned extensions |

### Style Principles

- **Precursory**: Write or update the RFC before starting implementation; use it to gather feedback and align direction
- **Skimmable**: Structure with clear headings; readers should locate relevant information quickly
- **Current**: Incorrect documentation is worse than missing documentation; update RFCs when implementation diverges
- **Consistent**: Use consistent terminology across RFCs; align with `docs/overview.md` vocabulary
- **Semantic**: Describe *what* and *why*, not *how*; focus on contracts, not code
- Use tables and diagrams to clarify complex relationships
