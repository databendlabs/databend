# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.2] - 2025-03-19

### Changed

- Change `arrow` version from `>=53,<55` to `54`.

## [0.4.1] - 2025-02-24

### Fixed

- Fix a bug that causes `RuntimeError('unable to get sys.modules')` when creating the second `SubInterpreter`.

## [0.4.0] - 2024-10-10

### Changed

- Upgrade `arrow` version requirement to `=53`.

## [0.3.0] - 2024-09-19

### Fixed

- Pin all arrow version to 52 instead.

## [0.2.2] - 2024-06-24

### Added

- Support large list type as output.

### Fixed

- Fix deprecated warnings with `arrow` v52.
- Don't panic when data type is not supported.

## [0.2.1] - 2024-05-23

### Fixed

- Fix building documentation on docs.rs.

## [0.2.0] - 2024-05-23

### Added

- Add methods to support aggregate functions.
    - `add_aggregate`
    - `create_state`
    - `accumulate`
    - `accumulate_or_retract`
    - `merge`
    - `finish`

- Add extension type `arrowudf.pickle`.

### Fixed

- Fix the abort issue when using decimal for the second time.

## [0.1.0] - 2024-04-25

### Added

- Initial release. Support basic scalar functions and table functions.
