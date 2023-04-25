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

* Apt:
```bash
sudo curl -L -o /usr/share/keyrings/datafuselabs-keyring.gpg https://repo.databend.rs/deb/datafuselabs.gpg
sudo curl -L -o /etc/apt/sources.list.d/datafuselabs.list https://repo.databend.rs/deb/datafuselabs.list

# or using DEB822-STYLE format in Ubuntu-22.04/Debian-12 and later
# sudo curl -L -o /etc/apt/sources.list.d/datafuselabs.sources https://repo.databend.rs/deb/datafuselabs.sources

sudo apt update

sudo apt install bendsql
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
