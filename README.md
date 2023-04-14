# Databend Client

Databend Native Client in Rust

## Components

- [**core**](core): RestAPI rust client

- [**driver**](driver): Unified SQL client for RestAPI and FlightSQL

- [**cli**](cli): Native CLI for Databend


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

### Cargo fmt, clippy, audit

```bash
make check
```

### Unit tests

```bash
make test
```

### integration tests

** Note: Docker and Docker Compose needed **

```bash
make integration-tests
make integration-tests-flight-sql
```
