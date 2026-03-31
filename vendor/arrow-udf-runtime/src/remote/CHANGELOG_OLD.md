# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.2] - 2025-03-19

### Added

- Re-export `arrow_flight` so downstream crates can use it without depending on `arrow-udf-flight`, and avoid version conflicts.

## [0.4.1] - 2025-03-19

### Changed

- Change `arrow` version from `>=53,<55` to `54`.

## [0.4.0] - 2024-10-10

### Changed

- Change `arrow_udf_flight::Client::new` method to accept `FlightServiceClient<Channel>` instead of `Channel`, allowing more customization.
- Upgrade `arrow` version requirement to `=53`.

## [0.3.0] - 2024-09-19

### Fixed

- Fix `arrow` version requirement to `=52`.

## [0.2.0] - 2024-07-03

### Changed

- Update `arrow` version to >=52 and `tonic` to 0.11.

## [0.1.0] - 2024-05-07

### Added

- Initial release.
