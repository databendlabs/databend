# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.1] - 2025-03-19

### Changed

- Change `arrow` version from `>=53,<55` to `54`.

## [0.5.0] - 2025-02-24

### Changed

- Remove `Runtime::find_function_by_inlined_signature` method.
- Change the first argument of `Runtime::call` and `Runtime::call_table_function` to the function handle returned by `Runtime::find_function`.

### Added

- Add `Runtime::find_function` method to find UDF function handle by name, argument types and return type.

## [0.4.1] - 2024-12-24

### Changed

- Replace unmaintained `genawaiter` with forked version `genawaiter2`.

### Fixed

- Rename target `wasm32-wasi` to `wasm32-wasip1`.

## [0.4.0] - 2024-10-10

### Changed

- Upgrade `arrow` version requirement to `=53`.

## [0.3.0] - 2024-09-19

### Fixed

- Pin all arrow version to 52 instead.

### Changed

- Update `wasmtime` to v22.

## [0.2.2] - 2024-04-25

### Added

- Support arrow-udf v0.3.
- Add `Runtime::abi_version`.

### Changed

- Update `wasmtime` to v20.

## [0.2.1] - 2024-04-07

### Changed

- Update `wasmtime` to v19.
- Update `arrow` version to >=50.
- Stdio is no longer inherited. Stdout and stderr will be included in the error message.

## [0.2.0] - 2024-02-29

### Added

- Support arrow-udf v0.2.
- Add more options to `build`.

### Changed

- Update `wasmtime` to v18.
- `build` now use arrow-udf v0.2 by default.

## [0.1.3] - 2024-02-04

### Fixed

- Force to build with stable toolchain.

## [0.1.2] - 2024-02-04

### Added

- Add `build_with` and `BuildOpts` to allow building in offline mode.
- Automatically install `wasm32-wasi` target when building without offline.

## [0.1.1] - 2024-01-31

### Added

- Add `build` feature to build the wasm binary from source.

### Changed

- Update `wasmtime` to v17.

## [0.1.0] - 2024-01-13

### Added

- Initial release. Support basic scalar functions and table functions.
