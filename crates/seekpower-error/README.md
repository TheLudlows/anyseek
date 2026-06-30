# seekpower-error

Foundation crate for SeekPower. Every other crate in the workspace depends on
this one, and every public function in the workspace returns
`seekpower_error::Result<T>`.

This crate exposes a single error type, `SeekPowerError`, and a `Result<T>`
alias.

## Error variants

`SeekPowerError` is a flat `enum` with no `From` impls from foreign error
types. Each variant carries the information a caller needs to diagnose the
failure; foreign errors must be stringified by the caller.

| Variant | Meaning |
| --- | --- |
| `IndexNotFound(String)` | The requested index name does not exist in the storage layer. |
| `InvalidQuery(String)` | The query string is malformed or violates query-parser rules. |
| `StorageError(String)` | Underlying I/O or key-value store (e.g. SlateDB) failed. |
| `SchemaError(String)` | A schema definition is invalid or inconsistent. |
| `EncodingError(String)` | A serialization, deserialization, or compression codec failed. |
| `DimensionMismatch { expected, actual }` | A vector had a length different from the schema's declared dimensions. |
| `Corrupted(String)` | The on-disk index is inconsistent and cannot be safely read. |

## Usage

```rust
use seekpower_error::{Result, SeekPowerError};

fn open_index(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(SeekPowerError::InvalidQuery(
            "index name must not be empty".into(),
        ));
    }
    Ok(())
}
```

The `Result<T>` alias is just `std::result::Result<T, SeekPowerError>` and is
the return type that every SeekPower crate uses.

## Dependency rules

This crate has no dependencies on any other `seekpower-*` crate, and it must
stay that way: it sits at the bottom of the dependency graph so that
`seekpower-schema`, `seekpower-storage`, and everything above can share a
common error vocabulary without forming a cycle.

Concretely:

- `SeekPowerError` does **not** implement `From<std::io::Error>`,
  `From<serde_json::Error>`, `From<slatedb::Error>`, or any other foreign
  `From` conversion.
- Crates that wrap foreign libraries must convert those errors to strings at
  the call site, typically into `SeekPowerError::StorageError(format!(...))`
  or `SeekPowerError::EncodingError(format!(...))`.

This keeps the error type trivially constructable anywhere in the workspace
and prevents the foundation crate from pulling in heavyweight transitive
dependencies.

## Source of truth

The error enum and `Result` alias are defined in a single file:

- `crates/seekpower-error/src/lib.rs`
