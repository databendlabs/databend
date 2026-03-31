# Agent Guidelines for Apache Iceberg Rust

Guidelines for AI coding agents working on the iceberg-rust codebase.

## Build Commands

```bash
make build                                      # Full build
cargo build -p iceberg                          # Build specific crate
cargo build -p iceberg --no-default-features    # Build without defaults
```

## Test Commands

```bash
make test                                       # All tests
make unit-test                                  # Unit tests only (no integration)
make doc-test                                   # Doc tests only

# Single test
cargo test --all-features -p iceberg test_name
cargo test --all-features -p iceberg -- test_name --exact

# Tests in specific module
cargo test --all-features -p iceberg spec::snapshot::tests

# With output
cargo test --all-features -p iceberg test_name -- --nocapture
```

## Lint & Format

```bash
make check           # All checks (format, clippy, toml, machete)
make check-fmt       # Format check only
cargo fmt --all      # Apply formatting

make check-clippy    # Clippy (warnings = errors)
# cargo clippy --all-targets --all-features --workspace -- -D warnings

make check-toml      # TOML format (requires taplo)
cargo machete        # Unused dependencies
```

## Code Style

### File Header
Every file MUST start with the Apache License header.

### Imports (rustfmt.toml enforced)
Order: std -> external crates -> crate. Module-level granularity.

```rust
use std::collections::HashMap;

use async_trait::async_trait;
use serde_derive::{Deserialize, Serialize};

use crate::spec::Schema;
use crate::{Error, ErrorKind, Result};
```

### Naming
- Types: `PascalCase` (`TableMetadata`, `NamespaceIdent`)
- Functions: `snake_case` (`load_table`, `with_context`)
- Constants: `SCREAMING_SNAKE_CASE`
- Modules: `snake_case`

### Error Handling
Use the crate's error system, NOT `anyhow` directly:

```rust
use crate::{Error, ErrorKind, Result};

Error::new(ErrorKind::DataInvalid, "message")
    .with_context("key", "value")
    .with_source(original_error)

// ErrorKinds: Unexpected, DataInvalid, FeatureUnsupported,
// TableAlreadyExists, TableNotFound, NamespaceAlreadyExists,
// NamespaceNotFound, PreconditionFailed

ensure_data_valid!(condition, "format {}", arg);
```

### Derive Macros
```rust
#[derive(Debug, Clone, PartialEq, Eq)]           // Value types
#[derive(Debug, Serialize, Deserialize, Clone)]  // Serializable
#[derive(Debug, TypedBuilder)]                   // Builder pattern
```

### Async Code
```rust
#[async_trait]
pub trait Catalog: Debug + Sync + Send {
    async fn load_table(&self, table: &TableIdent) -> Result<Table>;
}
```

### Documentation
- `#![deny(missing_docs)]` is enabled - public items MUST have docs
- Use `///` for items, `//!` for modules

### Testing
```rust
#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use super::*;

    #[test]
    fn test_something() { }
}
```

## Project Structure

```
crates/
  iceberg/              # Core library
  catalog/
    glue/               # AWS Glue
    hms/                # Hive Metastore
    memory/             # In-memory (testing)
    rest/               # REST catalog
    sql/                # SQL-based
    s3tables/           # S3 Tables
  integrations/
    datafusion/         # DataFusion query engine
    cli/                # CLI
  test_utils/           # Shared test utilities
  integration_tests/    # Integration tests
```

## PR Titles
Follow [Conventional Commits](https://www.conventionalcommits.org):
```
feat(schema): Add last_updated_ms in schema
fix(scan): Handle empty partition specs
docs: Update contributing guide
```

## Key Details

- MSRV: 1.84
- Arrow/Parquet: v56
- DataFusion: v50
- Storage: `opendal`
- Serialization: `serde`/`serde_json` (JSON), `apache-avro` (Avro)
