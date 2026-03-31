# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.8.0] - 2025-04-10

### Changed

- Remove `SubInterpreter` and all related code from Python UDF runtime.

## [0.7.0] - 2025-03-22

### Changed

- Merge all runtime packages into `arrow-udf-runtime`. You can use features like `javascript` to only enable JavaScript UDF runtime specifically. By default all runtimes are included.
