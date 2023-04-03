# bendsql &emsp; 

## Install 

```sh
cargo install bendsql
```

## Usage

```
> bendsql --help
Usage: bendsql <--user <USER>|--password <PASSWORD>|--host <HOST>|--port <PORT>>
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

