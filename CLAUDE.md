# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Databend is an open-source, Rust-based cloud data warehouse with near 100% Snowflake compatibility. It features MPP architecture, S3-native storage, and supports structured, semi-structured, and unstructured data processing with vector embeddings and AI capabilities.

## Architecture

The codebase follows a modular workspace structure with clear separation between:

- **Meta Service** (`src/meta/`): Distributed metadata management using Raft consensus
- **Query Service** (`src/query/`): SQL processing engine with vectorized execution
- **Common Libraries** (`src/common/`): Shared utilities for storage, networking, authentication
- **Binaries** (`src/binaries/`): Main executables for databend-query, databend-meta, databend-metactl

Key architectural patterns:
- Compute-storage separation with S3-native design
- Async Rust throughout with tokio runtime
- Arrow-based columnar processing
- Plugin architecture for storage backends and file formats

## Development Commands

### Building
```bash
# Debug build (fast compilation)
make build
# or: cargo build --bin=databend-query --bin=databend-meta --bin=databend-metactl

# Release build (optimized)
make build-release
# or: bash ./scripts/build/build-release.sh

# Native optimized build
make build-native
```

### Testing
```bash
# Unit tests
make unit-test

# Integration tests
make stateless-test        # Stateless integration tests
make sqllogic-test        # SQL logic tests
make metactl-test         # Meta control tests
make meta-kvapi-test      # Meta KV API tests

# Cluster tests
make stateless-cluster-test
make stateless-cluster-test-tls

# All tests
make test
```

### Development Environment
```bash
# Setup development environment (installs tools)
make setup

# Run debug build locally
make run-debug

# Run in management mode
make run-debug-management

# Stop all services
make kill
```

### Code Quality
```bash
# Format code
make fmt

# Lint and format all code
make lint
# Includes: cargo clippy, cargo machete, typos, taplo fmt, ruff format, shfmt

# YAML linting
make lint-yaml

# License checking
make check-license
```

### Cleanup
```bash
make clean  # Clean build artifacts and test data
```

## Core Components

### Meta Service (`src/meta/`)
- **raft-store**: Raft-based distributed consensus
- **kvapi**: Key-value API layer for metadata operations
- **api**: High-level metadata APIs (schema, table, user management)
- **client**: gRPC client for meta service communication
- **protos**: Protocol buffer definitions

### Query Service (`src/query/`)
- **sql**: SQL parser and planner using recursive descent parser
- **expression**: Vectorized expression evaluation engine
- **functions**: Scalar and aggregate function implementations
- **pipeline**: Query execution pipeline (sources → transforms → sinks)
- **storages**: Storage engine integrations (Fuse, Iceberg, Delta, Hive)
- **catalog**: Database/table catalog management

### Storage Systems
- **Fuse** (`src/query/storages/fuse/`): Native columnar storage format
- **External**: Iceberg, Delta Lake, Hive, Parquet integrations
- **Stage** (`src/query/storages/stage/`): External stage management

### Common Libraries (`src/common/`)
- **storage**: S3/cloud storage abstractions using OpenDAL
- **hashtable**: Optimized hash tables for joins and aggregations
- **expression**: Column-oriented data processing
- **exception**: Error handling and backtraces
- **metrics**: Prometheus metrics collection

## Testing Architecture

- **Unit tests**: Located in `tests/` subdirectories within each crate
- **Stateless tests**: `tests/suites/0_stateless/` - SQL script based tests
- **Stateful tests**: `tests/suites/1_stateful/` - Long-running integration tests
- **SQL Logic Tests**: `tests/sqllogictests/` - SQL compatibility verification
- **Enterprise tests**: `tests/suites/5_ee/` - Enterprise feature tests

## Configuration

- Default configs: `distro/configs/`
- Test configs: `scripts/ci/deploy/config/`
- Service configuration uses TOML format
- Environment-based configuration supported

## Performance and Profiling

```bash
# Performance profiling
make profile

# Memory profiling with jemalloc
# Built-in profiling endpoints available in debug builds
```

## Development Tips

- Use `make setup` to install all required development tools
- Rust toolchain version is pinned in `rust-toolchain.toml`
- The project uses custom memory allocator (jemalloc) for performance
- Vector/SIMD optimizations are extensive - check CPU feature compatibility
- S3/cloud storage tests require proper credentials configuration
- Always run `make lint` before committing to catch formatting issues

## Binary Outputs

After building, key binaries are available in `target/debug/` or `target/release/`:
- `databend-query`: Main query engine
- `databend-meta`: Metadata service  
- `databend-metactl`: Meta service administration tool
- `databend-sqllogictests`: SQL logic test runner