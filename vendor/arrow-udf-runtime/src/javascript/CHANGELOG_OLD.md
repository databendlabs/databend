# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.1] - 2025-03-19

### Changed

- Change `arrow` version from `>=53,<55` to `54`.

## [0.6.0] - 2025-02-24

### Added

- Add async function support, as a result, all methods of `Runtime` becomes `async`. Return value of `call_table_function` becomes an async `Stream`.
- Add `Runtime::enable_fetch` to enable the async Fetch API extension.
- Add batched function support.

### Changed

- Refactor the parameter as `FunctionOptions` and `AggregateOptions` to keep code clean.

## [0.5.0] - 2024-10-10

### Changed

- Upgrade `arrow` version requirement to `=53`.

## [0.4.0] - 2024-09-19

### Fixed

- Pin all arrow version to 52 instead.

## [0.3.2] - 2024-06-24

### Added

- Support large list type as input.

### Fixed

- Fix deprecated warnings with `arrow` v52.
- Don't panic when data type is not supported.

## [0.3.1] - 2024-05-24

### Added

- Add `Runtime::memory_usage` to get the current memory usage from quickjs.

## [0.3.0] - 2024-05-23

### Added

- Add `Runtime::{set_memory_limit, set_timeout}` to limit memory usage and execution time.
- Add methods to support aggregate functions.
    - `add_aggregate`
    - `create_state`
    - `accumulate`
    - `accumulate_or_retract`
    - `merge`
    - `finish`

### Changed

- Update rquickjs to v0.6.2.

## [0.2.0] - 2024-04-25

### Breaking Changes

- `json` and `decimal` type are no longer mapped to `LargeString` and `LargeBinary` respectively. They are now mapped to [extension types](https://arrow.apache.org/docs/format/Columnar.html#format-metadata-extension-types) with `String` as the storage type.
    - `json`: `ARROW:extension:name` = `arrorudf.json`
    - `decimal`: `ARROW:extension:name` = `arrowudf.decimal`

### Added

- Add support for `LargeString` and `LargeBinary` type.

### Changed

- Update `arrow` version to >=50.

## [0.1.2] - 2024-03-04

### Fixed

- Make `Runtime` Send and Sync again.

## [0.1.1] - 2024-02-19

### Changed

- Improve performance of decimal inputs.
- Update rquickjs to v0.5.0.

## [0.1.0] - 2024-01-31

### Added

- Initial release. Support basic scalar functions and table functions.
