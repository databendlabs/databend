// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use thiserror::Error;

// TODO: implement From<Result> for `common_exception::Result`.s
pub type Result<T> = std::result::Result<T, Error>;

/// Error is the error type for the dal2 crate.
///
/// ## Style
///
/// The error will be named as `noun-adj`. For example, `ObjectNotExist` or `PermissionDenied`.
/// The error will be formatted as `description: (keyA valueA, keyB valueB, ...)`.
/// As an exception, `Error::Unexpected` is used for all unexpected errors.
///
/// ## TODO
///
/// Maybe it's better to include the operation name in the error message.
#[derive(Error, Debug)]
pub enum Error {
    #[error("backend not supported: (type {0})")]
    BackendNotSupported(String),
    #[error("backend configuration invalid: (key {key}, value {value})")]
    BackendConfigurationInvalid { key: String, value: String },

    #[error("object not exist: (path {0})")]
    ObjectNotExist(String),
    #[error("permission denied: (path {0})")]
    PermissionDenied(String),

    #[error("unexpected: (cause {0})")]
    Unexpected(String),
}
