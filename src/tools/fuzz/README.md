# fuzz

## Installing `cargo-afl`

```
cargo install cargo-afl
```

## Fuzzing

Build the fuzzer:

```shell
cd fuzz
cargo afl build
```

Fuzz with the `fuzz_parse_sql` target:

```shell
cargo afl fuzz -i in -o out target/debug/fuzz_parse_sql
```

For more information, please check <https://rust-fuzz.github.io/book/afl/tutorial.html>
