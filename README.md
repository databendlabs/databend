# Databend Client

Databend Native Client in Rust

## Components

- **core**: RestAPI rust client [![crates.io](https://img.shields.io/crates/v/databend-client.svg)](https://crates.io/crates/databend-client)

- **driver**: Unified SQL client for RestAPI and FlightSQL [![crates.io](https://img.shields.io/crates/v/databend-driver.svg)](https://crates.io/crates/databend-driver)

- **cli**: Native CLI for Databend [![crates.io](https://img.shields.io/crates/v/bendsql.svg)](https://crates.io/crates/bendsql)


## Installation for BendSQL

With cargo:
```bash
cargo install bendsql
```

With Homebrew:
```bash
brew install databendcloud/homebrew-tap/bendsql
```

With Binary: check for latest release [here](https://github.com/datafuselabs/databend-client/releases)


## Development

### unit tests

```bash
cargo test --lib
```

### integration tests

```bash
make -C tests
```
