# fuzz

## Installing `honggfuzz`

```
cargo install honggfuzz
```

Install [dependencies](https://github.com/rust-fuzz/honggfuzz-rs#dependencies) for your system.

## Fuzzing

Choose a target.
These are `[[bin]]` entries in `Cargo.toml`.
List them with `cargo read-manifest | jq '.targets[].name'` from the `fuzz` directory.

Run the fuzzer:

```shell
cd fuzz
cargo hfuzz run <target>
```

After a panic is found, get a stack trace with:

```shell
cargo hfuzz run-debug <target> hfuzz_workspace/<target>/*.fuzz
```

For example, with the `fuzz_parse_sql` target:

```shell
cargo hfuzz run fuzz_parse_sql
cargo hfuzz run-debug fuzz_parse_sql hfuzz_workspace/fuzz_parse_sql/*.fuzz
```

## Notices

Due to the LLVM version, errors like the following may occur:

```text
error: failed to run LLVM passes: unknown pass name 'sancov'

error: could not compile `cfg-if` due to previous error
```

Please checkout [honggfuzz-rs#63](https://github.com/rust-fuzz/honggfuzz-rs/pull/63), which contains a possible fix.
