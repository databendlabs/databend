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
#[derive(Error, Debug)]
pub enum Error {
    #[error("backend configuration invalid: (key {key}, value {value})")]
    BackendConfigurationInvalid { key: String, value: String },

    #[error("object not exist: (path {0})")]
    ObjectNotExist(String),
    #[error("permission denied: (path {0})")]
    PermissionDenied(String),

    #[error("unexpected: (cause {0})")]
    Unexpected(String),
}
