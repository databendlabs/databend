# BendSQL

Databend Native Client in Rust

## Components

- [**core**](core): Databend RestAPI rust client

- [**driver**](driver): Databend unified SQL client for RestAPI and FlightSQL

- [**cli**](cli): Databend native CLI


## Installation for BendSQL

* Cargo:
```bash
cargo install bendsql
```

* Homebrew:
```bash
brew install databendcloud/homebrew-tap/bendsql
```

* Binary: check for latest release [here](https://github.com/datafuselabs/databend-client/releases)


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

*Note: Docker and Docker Compose needed*

```bash
make integration
```
