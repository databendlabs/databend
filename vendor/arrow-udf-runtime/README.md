# Arrow UDF Runtime

[![Crate](https://img.shields.io/crates/v/arrow-udf-runtime.svg)](https://crates.io/crates/arrow-udf-runtime)
[![Docs](https://docs.rs/arrow-udf-runtime/badge.svg)](https://docs.rs/arrow-udf-runtime)

Provide runtimes for JavaScript, Python, WASM or remote UDFs.

## Usage

By default, all runtimes will be included:

```toml
arrow-udf-runtime = "0.7"
```

You can selectively enable specific runtimes using feature flags:

```toml
# Enable only Python runtime
arrow-udf-runtime = { version = "0.7", default-features = false, features = ["python"] }

# Enable JavaScript and WASM runtimes
arrow-udf-runtime = { version = "0.7", default-features = false, features = ["javascript", "wasm"] }

# Enable remote execution runtime
arrow-udf-runtime = { version = "0.7", default-features = false, features = ["remote"] }

# Enable embedded runtime set (includes WASM, Python, and JavaScript)
arrow-udf-runtime = { version = "0.7", default-features = false, features = ["embedded"] }
```

Available runtime features:

- `python`: Python runtime
- `javascript`: JavaScript runtime
- `wasm`: WebAssembly runtime
- `remote`: Remote execution runtime
- `embedded`: Collection of WASM, Python and JavaScript runtimes

For detailed usage instructions of each runtime, please refer to the following directories:

- [Python runtime](src/python)
- [JavaScript runtime](src/javascript)
- [WebAssembly runtime](src/wasm)
- [Remote execution runtime](src/remote)
