# BendSQL

Databend Native Client in Rust

## Components

- [**core**](core): Databend RestAPI Rust client

- [**driver**](driver): Databend SQL client for both RestAPI and FlightSQL

- [**cli**](cli): Databend native CLI

## Installation for BendSQL

### Cargo:

[cargo-binstall](https://github.com/cargo-bins/cargo-binstall) is recommended:

```bash
cargo binstall bendsql
```

Or alternatively build from source:

```bash
cargo install bendsql
```

### Homebrew:

```bash
brew install databendcloud/homebrew-tap/bendsql
```

### Apt:

- Using DEB822-STYLE format on Ubuntu-22.04/Debian-12 and later:

```bash
sudo curl -L -o /etc/apt/sources.list.d/datafuselabs.sources https://repo.databend.rs/deb/datafuselabs.sources
```

- Using old format on Ubuntu-20.04/Debian-11 and earlier:

```bash
sudo curl -L -o /usr/share/keyrings/datafuselabs-keyring.gpg https://repo.databend.rs/deb/datafuselabs.gpg
sudo curl -L -o /etc/apt/sources.list.d/datafuselabs.list https://repo.databend.rs/deb/datafuselabs.list
```

Then install bendsql:

```bash
sudo apt update

sudo apt install bendsql
```

### Manually:

Check for latest version on [GitHub Release](https://github.com/datafuselabs/bendsql/releases)

## Usage

```
❯ bendsql --help
Databend Native Command Line Tool

Usage: bendsql [OPTIONS]

Options:
      --help                       Print help information
      --flight                     Using flight sql protocol, ignored when --dsn is set
      --tls                        Enable TLS, ignored when --dsn is set
  -h, --host <HOST>                Databend Server host, Default: 127.0.0.1, ignored when --dsn is set
  -P, --port <PORT>                Databend Server port, Default: 8000, ignored when --dsn is set
  -u, --user <USER>                Default: root, overrides username in DSN
  -p, --password <PASSWORD>        Password, overrides password in DSN [env: BENDSQL_PASSWORD]
  -r, --role <ROLE>                Downgrade role name, overrides role in DSN
  -D, --database <DATABASE>        Database name, overrides database in DSN
      --set <SET>                  Settings, overrides settings in DSN
      --dsn <DSN>                  Data source name [env: BENDSQL_DSN]
  -n, --non-interactive            Force non-interactive mode
      --check                      Check for server status and exit
      --query=<QUERY>              Query to execute
  -d, --data <DATA>                Data to load, @file or @- for stdin
  -f, --format <FORMAT>            Data format to load [default: csv] [possible values: csv, tsv, ndjson, parquet, xml]
      --format-opt <FORMAT_OPT>    Data format options
  -o, --output <OUTPUT>            Output format [possible values: table, csv, tsv, null]
      --quote-style <QUOTE_STYLE>  Output quote style, applies to `csv` and `tsv` output formats [possible values: always, necessary, non-numeric, never]
      --progress                   Show progress for query execution in stderr, only works with output format `table` and `null`.
      --stats                      Show stats after query execution in stderr, only works with non-interactive mode.
      --time[=<TIME>]              Only show execution time without results, will implicitly set output format to `null`. [possible values: local, server]
  -l, --log-level <LOG_LEVEL>      [default: info]
  -V, --version                    Print version
```

## Custom configuration

By default bendsql will read configuration from `~/.bendsql/config.toml` and `~/.config/bendsql/config.toml`
sequentially if exists.

- Example file

```
❯ cat ~/.bendsql/config.toml
[connection]
connect_timeout = "30s"


[settings]
display_pretty_sql = true
progress_color = "green"
expand = "off"
prompt = ":) "

```

- Connection section

| Parameter  | Description                 |
| ---------- | --------------------------- |
| `host`     | Server host to connect.     |
| `port`     | Server port to connect.     |
| `user`     | User name.                  |
| `database` | Which database to connect.  |
| `args`     | Additional connection args. |

- Settings section

| Parameter            | Description                                                                         |
| -------------------- | ----------------------------------------------------------------------------------- |
| `display_pretty_sql` | Whether to display SQL queries in a formatted way.                                  |
| `prompt`             | The prompt to display before asking for input.                                      |
| `progress_color`     | The color to use for the progress bar.                                              |
| `show_progress`      | Whether to show a progress bar when executing queries.                              |
| `show_stats`         | Whether to show statistics after executing queries.                                 |
| `max_display_rows`   | The maximum number of rows to display in table output format.                       |
| `max_width`          | Limit display render box max width, 0 means default to the size of the terminal.    |
| `max_col_width`      | Limit display render each column max width, smaller than 3 means disable the limit. |
| `output_format`      | The output format to use.                                                           |
| `expand`             | Expand table format display, default auto, could be on/off/auto.                    |
| `time`               | Whether to show the time elapsed when executing queries.                            |
| `multi_line`         | Whether to allow multi-line input.                                                  |
| `replace_newline`    | whether replace '\n' with '\\\n'.                                                   |

## Commands in REPL

| Commands       | Description             |
| -------------- | ----------------------- |
| `!exit`        | Exit bendsql            |
| `!quit`        | Exit bendsql            |
| `!configs`     | Show current settings   |
| `!set`         | Set settings            |
| `!source file` | Source file and execute |

## Setting commands in REPL

We can use `!set CMD_NAME VAL` to update the `Settings` above in runtime, example:

```
❯ bendsql

:) !set display_pretty_sql false
:) !set max_display_rows 10
:) !set expand auto
```

## DSN

Format:

```
databend[+flight]://user:[password]@host[:port]/[database][?sslmode=disable][&arg1=value1]
```

Examples:

- `databend://root:@localhost:8000/?sslmode=disable&presign=detect`

- `databend://user1:password1@tnxxxx--default.gw.aws-us-east-2.default.databend.com:443/benchmark?enable_dphyp=1`

- `databend+flight://root:@localhost:8900/database1?connect_timeout=10`

Available Args:

Common:

| Arg               | Description                          |
| ----------------- | ------------------------------------ |
| `tenant`          | Tenant ID, Databend Cloud only.      |
| `warehouse`       | Warehouse name, Databend Cloud only. |
| `sslmode`         | Set to `disable` if not using tls.   |
| `tls_ca_file`     | Custom root CA certificate path.     |
| `connect_timeout` | Connect timeout in seconds           |

RestAPI client:

| Arg                         | Description                                                                                                                                                      |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `wait_time_secs`            | Request wait time for page, default to `1`                                                                                                                       |
| `max_rows_in_buffer`        | Max rows for page buffer                                                                                                                                         |
| `max_rows_per_page`         | Max response rows for a single page                                                                                                                              |
| `page_request_timeout_secs` | Timeout for a single page request, default to `30`                                                                                                               |
| `presign`                   | Whether to enable presign for data loading, available arguments are `auto`/`detect`/`on`/`off`. Default to `auto` which only enable presign for `Databend Cloud` |

FlightSQL client:

| Arg                         | Description                                                               |
| --------------------------- | ------------------------------------------------------------------------- |
| `query_timeout`             | Query timeout seconds                                                     |
| `tcp_nodelay`               | Default to `true`                                                         |
| `tcp_keepalive`             | Tcp keepalive seconds, default to `3600`, set to `0` to disable keepalive |
| `http2_keep_alive_interval` | Keep alive interval in seconds, default to `300`                          |
| `keep_alive_timeout`        | Keep alive timeout in seconds, default to `20`                            |
| `keep_alive_while_idle`     | Default to `true`                                                         |

Query Settings:

see: [Databend Query Settings](https://databend.rs/doc/sql-commands/show/show-settings)

## Development

### Cargo fmt, clippy, deny

```bash
make check
```

### Unit tests

```bash
make test
```

### integration tests

_Note: Docker and Docker Compose needed_

```bash
make integration
```
