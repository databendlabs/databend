---
id: building-and-running
title: Building and Running
---

This document describes how to build and run [FuseQuery](https://github.com/datafusedev/fuse-query) as a distributed query engine. 

## 1. Run with Docker (Recommended)

```text
docker pull datafusedev/fuse-query
docker run --init --rm -p 3307:3307 datafusedev/fuse-query
...
05:12:36 [ INFO] Options { log_level: "debug", num_cpus: 8, mysql_handler_port: 3307 }
05:12:36 [ INFO] Fuse-Query Cloud Compute Starts...
05:12:36 [ INFO] Usage: mysql -h127.0.0.1 -P3307
```

Or 

## 2. Run from Source

### 2.1 Dependencies

FuseQuery is a Rust project. Clang, Rust are supported. 

To install dependencies on Ubuntu:

```text
apt install git clang
```

On Arch Linux:

```text
pacman -S git clang
```

On Mac via Homebrew:

```text
brew install git clang
```

To install Rust(nightly):

```text
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup toolchain install nightly
```

### 2.2 Running on Linux and macOS


Clone:

```text
git clone https://github.com/datafusedev/fuse-query
```

Running:

```text
cd fuse-query
make run
```

## 3. Connect

 Connect FuseQuery with MySQL client

```text
mysql -h127.0.0.1 -P3307
```

### Avg Demo

```text
mysql> SELECT avg(number) FROM system.numbers(10000);
+-------------+
| Avg(number) |
+-------------+
|      4999.5 |
+-------------+
1 row in set (0.00 sec)

```


### 10 Billion Performance

```text
mysql> SELECT avg(number) FROM system.numbers(10000000000);
+-------------------+
| Avg(number)       |
+-------------------+
| 4999999999.494631 |
+-------------------+
1 row in set (2.02 sec)
```