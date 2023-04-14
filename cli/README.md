# BendSQL

Databend Native Command Line Tool

[![crates.io](https://img.shields.io/crates/v/bendsql.svg)](https://crates.io/crates/bendsql)
![License](https://img.shields.io/crates/l/bendsql.svg)

## Install

```sh
cargo install bendsql
```

## Usage

```
❯ bendsql --help
Databend Native Command Line Tool

Usage: bendsql [OPTIONS]

Options:
      --help                 Print help information
      --flight               Using flight sql protocol
      --tls                  Enable TLS
  -h, --host <HOST>          Databend Server host, Default: 127.0.0.1
  -P, --port <PORT>          Databend Server port, Default: 8000
  -u, --user <USER>          Default: root
  -p, --password <PASSWORD>  [env: BENDSQL_PASSWORD=]
  -D, --database <DATABASE>  Database name
      --set <SET>            Settings
      --dsn <DSN>            Data source name [env: BENDSQL_DSN=]
  -n, --non-interactive      Force non-interactive mode
  -q, --query <QUERY>        Query to execute
  -d, --data <DATA>          Data to load, @file or @- for stdin
  -f, --format <FORMAT>      Data format to load [default: csv]
  -o, --output <OUTPUT>      Output format [default: table]
      --progress             Show progress for data loading in stderr
  -V, --version              Print version
```

## Examples

### REPL
```sql
❯ bendsql -h arch -u sundy -p abc --port 8900
Welcome to Arrow CLI.
Connecting to http://arch:8900/ as user sundy.

arch :) select avg(number) from numbers(10);

select avg(number) from numbers(10);

+-------------+
| avg(number) |
+-------------+
| 4.5         |
+-------------+

1 rows in set (0.036 sec)

arch :) show tables like 'c%';

show tables like 'c%';

+-------------------+
| tables_in_default |
+-------------------+
| customer          |
+-------------------+

1 rows in set (0.030 sec)

arch :) exit
Bye
```

### StdIn Pipe

```bash
❯ echo "select number from numbers(3)" | bendsql -h arch -u sundy -p abc --port 8900
0
1
2
```

## Features

- basic keywords highlight
- basic auto-completion
- select query support
- TBD
