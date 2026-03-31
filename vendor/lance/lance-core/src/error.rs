// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_schema::ArrowError;
use snafu::{Location, Snafu};

type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Allocates error on the heap and then places `e` into it.
#[inline]
pub fn box_error(e: impl std::error::Error + Send + Sync + 'static) -> BoxedError {
    Box::new(e)
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Invalid user input: {source}, {location}"))]
    InvalidInput {
        source: BoxedError,
        location: Location,
    },
    #[snafu(display("Dataset already exists: {uri}, {location}"))]
    DatasetAlreadyExists { uri: String, location: Location },
    #[snafu(display("Append with different schema: {difference}, location: {location}"))]
    SchemaMismatch {
        difference: String,
        location: Location,
    },
    #[snafu(display("Dataset at path {path} was not found: {source}, {location}"))]
    DatasetNotFound {
        path: String,
        source: BoxedError,
        location: Location,
    },
    #[snafu(display("Encountered corrupt file {path}: {source}, {location}"))]
    CorruptFile {
        path: object_store::path::Path,
        source: BoxedError,
        location: Location,
        // TODO: add backtrace?
    },
    #[snafu(display("Not supported: {source}, {location}"))]
    NotSupported {
        source: BoxedError,
        location: Location,
    },
    #[snafu(display("Commit conflict for version {version}: {source}, {location}"))]
    CommitConflict {
        version: u64,
        source: BoxedError,
        location: Location,
    },
    #[snafu(display("Retryable commit conflict for version {version}: {source}, {location}"))]
    RetryableCommitConflict {
        version: u64,
        source: BoxedError,
        location: Location,
    },
    #[snafu(display("Too many concurrent writers. {message}, {location}"))]
    TooMuchWriteContention { message: String, location: Location },
    #[snafu(display("Encountered internal error. Please file a bug report at https://github.com/lance-format/lance/issues. {message}, {location}"))]
    Internal { message: String, location: Location },
    #[snafu(display("A prerequisite task failed: {message}, {location}"))]
    PrerequisiteFailed { message: String, location: Location },
    #[snafu(display("Unprocessable: {message}, {location}"))]
    Unprocessable { message: String, location: Location },
    #[snafu(display("LanceError(Arrow): {message}, {location}"))]
    Arrow { message: String, location: Location },
    #[snafu(display("LanceError(Schema): {message}, {location}"))]
    Schema { message: String, location: Location },
    #[snafu(display("Not found: {uri}, {location}"))]
    NotFound { uri: String, location: Location },
    #[snafu(display("LanceError(IO): {source}, {location}"))]
    IO {
        source: BoxedError,
        location: Location,
    },
    #[snafu(display("LanceError(Index): {message}, {location}"))]
    Index { message: String, location: Location },
    #[snafu(display("Lance index not found: {identity}, {location}"))]
    IndexNotFound {
        identity: String,
        location: Location,
    },
    #[snafu(display("Cannot infer storage location from: {message}"))]
    InvalidTableLocation { message: String },
    /// Stream early stop
    Stop,
    #[snafu(display("Wrapped error: {error}, {location}"))]
    Wrapped {
        error: BoxedError,
        location: Location,
    },
    #[snafu(display("Cloned error: {message}, {location}"))]
    Cloned { message: String, location: Location },
    #[snafu(display("Query Execution error: {message}, {location}"))]
    Execution { message: String, location: Location },
    #[snafu(display("Ref is invalid: {message}"))]
    InvalidRef { message: String },
    #[snafu(display("Ref conflict error: {message}"))]
    RefConflict { message: String },
    #[snafu(display("Ref not found error: {message}"))]
    RefNotFound { message: String },
    #[snafu(display("Cleanup error: {message}"))]
    Cleanup { message: String },
    #[snafu(display("Version not found error: {message}"))]
    VersionNotFound { message: String },
    #[snafu(display("Version conflict error: {message}"))]
    VersionConflict {
        message: String,
        major_version: u16,
        minor_version: u16,
        location: Location,
    },
    #[snafu(display("Namespace error: {source}, {location}"))]
    Namespace {
        source: BoxedError,
        location: Location,
    },
}

impl Error {
    pub fn corrupt_file(
        path: object_store::path::Path,
        message: impl Into<String>,
        location: Location,
    ) -> Self {
        let message: String = message.into();
        Self::CorruptFile {
            path,
            source: message.into(),
            location,
        }
    }

    pub fn invalid_input(message: impl Into<String>, location: Location) -> Self {
        let message: String = message.into();
        Self::InvalidInput {
            source: message.into(),
            location,
        }
    }

    pub fn io(message: impl Into<String>, location: Location) -> Self {
        let message: String = message.into();
        Self::IO {
            source: message.into(),
            location,
        }
    }

    pub fn version_conflict(
        message: impl Into<String>,
        major_version: u16,
        minor_version: u16,
        location: Location,
    ) -> Self {
        let message: String = message.into();
        Self::VersionConflict {
            message,
            major_version,
            minor_version,
            location,
        }
    }
}

pub trait LanceOptionExt<T> {
    /// Unwraps an option, returning an internal error if the option is None.
    ///
    /// Can be used when an option is expected to have a value.
    fn expect_ok(self) -> Result<T>;
}

impl<T> LanceOptionExt<T> for Option<T> {
    #[track_caller]
    fn expect_ok(self) -> Result<T> {
        let location = std::panic::Location::caller().to_snafu_location();
        self.ok_or_else(|| Error::Internal {
            message: "Expected option to have value".to_string(),
            location,
        })
    }
}

pub trait ToSnafuLocation {
    fn to_snafu_location(&'static self) -> snafu::Location;
}

impl ToSnafuLocation for std::panic::Location<'static> {
    fn to_snafu_location(&'static self) -> snafu::Location {
        snafu::Location::new(self.file(), self.line(), self.column())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
pub type ArrowResult<T> = std::result::Result<T, ArrowError>;
#[cfg(feature = "datafusion")]
pub type DataFusionResult<T> = std::result::Result<T, datafusion_common::DataFusionError>;

impl From<ArrowError> for Error {
    #[track_caller]
    fn from(e: ArrowError) -> Self {
        Self::Arrow {
            message: e.to_string(),
            location: std::panic::Location::caller().to_snafu_location(),
        }
    }
}

impl From<&ArrowError> for Error {
    #[track_caller]
    fn from(e: &ArrowError) -> Self {
        Self::Arrow {
            message: e.to_string(),
            location: std::panic::Location::caller().to_snafu_location(),
        }
    }
}

impl From<std::io::Error> for Error {
    #[track_caller]
    fn from(e: std::io::Error) -> Self {
        Self::IO {
            source: box_error(e),
            location: std::panic::Location::caller().to_snafu_location(),
        }
    }
}

impl From<object_store::Error> for Error {
    #[track_caller]
    fn from(e: object_store::Error) -> Self {
        Self::IO {
            source: box_error(e),
            location: std::panic::Location::caller().to_snafu_location(),
        }
    }
}

impl From<prost::DecodeError> for Error {
    #[track_caller]
    fn from(e: prost::DecodeError) -> Self {
        Self::IO {
            source: box_error(e),
            location: std::panic::Location::caller().to_snafu_location(),
        }
    }
}

impl From<prost::EncodeError> for Error {
    #[track_caller]
    fn from(e: prost::EncodeError) -> Self {
        Self::IO {
            source: box_error(e),
            location: std::panic::Location::caller().to_snafu_location(),
        }
    }
}

impl From<prost::UnknownEnumValue> for Error {
    #[track_caller]
    fn from(e: prost::UnknownEnumValue) -> Self {
        Self::IO {
            source: box_error(e),
            location: std::panic::Location::caller().to_snafu_location(),
        }
    }
}

impl From<tokio::task::JoinError> for Error {
    #[track_caller]
    fn from(e: tokio::task::JoinError) -> Self {
        Self::IO {
            source: box_error(e),
            location: std::panic::Location::caller().to_snafu_location(),
        }
    }
}

impl From<object_store::path::Error> for Error {
    #[track_caller]
    fn from(e: object_store::path::Error) -> Self {
        Self::IO {
            source: box_error(e),
            location: std::panic::Location::caller().to_snafu_location(),
        }
    }
}

impl From<url::ParseError> for Error {
    #[track_caller]
    fn from(e: url::ParseError) -> Self {
        Self::IO {
            source: box_error(e),
            location: std::panic::Location::caller().to_snafu_location(),
        }
    }
}

impl From<serde_json::Error> for Error {
    #[track_caller]
    fn from(e: serde_json::Error) -> Self {
        Self::Arrow {
            message: e.to_string(),
            location: std::panic::Location::caller().to_snafu_location(),
        }
    }
}

#[track_caller]
fn arrow_io_error_from_msg(message: String) -> ArrowError {
    ArrowError::IoError(message.clone(), std::io::Error::other(message))
}

impl From<Error> for ArrowError {
    fn from(value: Error) -> Self {
        match value {
            Error::Arrow { message, .. } => arrow_io_error_from_msg(message), // we lose the error type converting to LanceError
            Error::IO { source, .. } => arrow_io_error_from_msg(source.to_string()),
            Error::Schema { message, .. } => Self::SchemaError(message),
            Error::Index { message, .. } => arrow_io_error_from_msg(message),
            Error::Stop => arrow_io_error_from_msg("early stop".to_string()),
            e => arrow_io_error_from_msg(e.to_string()), // Find a more scalable way of doing this
        }
    }
}

#[cfg(feature = "datafusion")]
impl From<datafusion_sql::sqlparser::parser::ParserError> for Error {
    #[track_caller]
    fn from(e: datafusion_sql::sqlparser::parser::ParserError) -> Self {
        Self::IO {
            source: box_error(e),
            location: std::panic::Location::caller().to_snafu_location(),
        }
    }
}

#[cfg(feature = "datafusion")]
impl From<datafusion_sql::sqlparser::tokenizer::TokenizerError> for Error {
    #[track_caller]
    fn from(e: datafusion_sql::sqlparser::tokenizer::TokenizerError) -> Self {
        Self::IO {
            source: box_error(e),
            location: std::panic::Location::caller().to_snafu_location(),
        }
    }
}

#[cfg(feature = "datafusion")]
impl From<Error> for datafusion_common::DataFusionError {
    #[track_caller]
    fn from(e: Error) -> Self {
        Self::Execution(e.to_string())
    }
}

#[cfg(feature = "datafusion")]
impl From<datafusion_common::DataFusionError> for Error {
    #[track_caller]
    fn from(e: datafusion_common::DataFusionError) -> Self {
        let location = std::panic::Location::caller().to_snafu_location();
        match e {
            datafusion_common::DataFusionError::SQL(..)
            | datafusion_common::DataFusionError::Plan(..)
            | datafusion_common::DataFusionError::Configuration(..) => Self::InvalidInput {
                source: box_error(e),
                location,
            },
            datafusion_common::DataFusionError::SchemaError(..) => Self::Schema {
                message: e.to_string(),
                location,
            },
            datafusion_common::DataFusionError::ArrowError(..) => Self::Arrow {
                message: e.to_string(),
                location,
            },
            datafusion_common::DataFusionError::NotImplemented(..) => Self::NotSupported {
                source: box_error(e),
                location,
            },
            datafusion_common::DataFusionError::Execution(..) => Self::Execution {
                message: e.to_string(),
                location,
            },
            _ => Self::IO {
                source: box_error(e),
                location,
            },
        }
    }
}

// This is a bit odd but some object_store functions only accept
// Stream<Result<T, ObjectStoreError>> and so we need to convert
// to ObjectStoreError to call the methods.
impl From<Error> for object_store::Error {
    fn from(err: Error) -> Self {
        Self::Generic {
            store: "N/A",
            source: Box::new(err),
        }
    }
}

#[track_caller]
pub fn get_caller_location() -> &'static std::panic::Location<'static> {
    std::panic::Location::caller()
}

/// Wrap an error in a new error type that implements Clone
///
/// This is useful when two threads/streams share a common fallible source
/// The base error will always have the full error.  Any cloned results will
/// only have Error::Cloned with the to_string of the base error.
pub struct CloneableError(pub Error);

impl Clone for CloneableError {
    #[track_caller]
    fn clone(&self) -> Self {
        Self(Error::Cloned {
            message: self.0.to_string(),
            location: std::panic::Location::caller().to_snafu_location(),
        })
    }
}

#[derive(Clone)]
pub struct CloneableResult<T: Clone>(pub std::result::Result<T, CloneableError>);

impl<T: Clone> From<Result<T>> for CloneableResult<T> {
    fn from(result: Result<T>) -> Self {
        Self(result.map_err(CloneableError))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_caller_location_capture() {
        let current_fn = get_caller_location();
        // make sure ? captures the correct location
        // .into() WILL NOT capture the correct location
        let f: Box<dyn Fn() -> Result<()>> = Box::new(|| {
            Err(object_store::Error::Generic {
                store: "",
                source: "".into(),
            })?;
            Ok(())
        });
        match f().unwrap_err() {
            Error::IO { location, .. } => {
                // +4 is the beginning of object_store::Error::Generic...
                assert_eq!(location.line, current_fn.line() + 4, "{}", location)
            }
            #[allow(unreachable_patterns)]
            _ => panic!("expected ObjectStore error"),
        }
    }
}
