// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::backtrace::{Backtrace, BacktraceStatus};
use std::fmt;
use std::fmt::{Debug, Display, Formatter};

use chrono::{DateTime, TimeZone as _, Utc};

/// Result that is a wrapper of `Result<T, iceberg::Error>`
pub type Result<T> = std::result::Result<T, Error>;

/// ErrorKind is all kinds of Error of iceberg.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The operation was rejected because the system is not in a state required for the operation’s execution.
    PreconditionFailed,

    /// Iceberg don't know what happened here, and no actions other than
    /// just returning it back. For example, iceberg returns an internal
    /// service error.
    Unexpected,

    /// Iceberg data is invalid.
    ///
    /// This error is returned when we try to read a table from iceberg but
    /// failed to parse its metadata or data file correctly.
    ///
    /// The table could be invalid or corrupted.
    DataInvalid,

    /// Iceberg namespace already exists at creation.
    NamespaceAlreadyExists,

    /// Iceberg table already exists at creation.
    TableAlreadyExists,

    /// Iceberg namespace does not exist.
    NamespaceNotFound,

    /// Iceberg table does not exist.
    TableNotFound,

    /// Iceberg feature is not supported.
    ///
    /// This error is returned when given iceberg feature is not supported.
    FeatureUnsupported,

    /// Catalog commit failed due to outdated metadata
    CatalogCommitConflicts,
}

impl ErrorKind {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }
}

impl From<ErrorKind> for &'static str {
    fn from(v: ErrorKind) -> &'static str {
        match v {
            ErrorKind::Unexpected => "Unexpected",
            ErrorKind::DataInvalid => "DataInvalid",
            ErrorKind::FeatureUnsupported => "FeatureUnsupported",
            ErrorKind::TableAlreadyExists => "TableAlreadyExists",
            ErrorKind::TableNotFound => "TableNotFound",
            ErrorKind::NamespaceAlreadyExists => "NamespaceAlreadyExists",
            ErrorKind::NamespaceNotFound => "NamespaceNotFound",
            ErrorKind::PreconditionFailed => "PreconditionFailed",
            ErrorKind::CatalogCommitConflicts => "CatalogCommitConflicts",
        }
    }
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.into_static())
    }
}

/// Error is the error struct returned by all iceberg functions.
///
/// ## Display
///
/// Error can be displayed in two ways:
///
/// - Via `Display`: like `err.to_string()` or `format!("{err}")`
///
/// Error will be printed in a single line:
///
/// ```shell
/// Unexpected, context: { path: /path/to/file, called: send_async } => something wrong happened, source: networking error"
/// ```
///
/// - Via `Debug`: like `format!("{err:?}")`
///
/// Error will be printed in multi lines with more details and backtraces (if captured):
///
/// ```shell
/// Unexpected => something wrong happened
///
/// Context:
///    path: /path/to/file
///    called: send_async
///
/// Source: networking error
///
/// Backtrace:
///    0: iceberg::error::Error::new
///              at ./src/error.rs:197:24
///    1: iceberg::error::tests::generate_error
///              at ./src/error.rs:241:9
///    2: iceberg::error::tests::test_error_debug_with_backtrace::{{closure}}
///              at ./src/error.rs:305:41
///    ...
/// ```
pub struct Error {
    kind: ErrorKind,
    message: String,

    context: Vec<(&'static str, String)>,

    source: Option<anyhow::Error>,
    backtrace: Backtrace,

    retryable: bool,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;

        if !self.context.is_empty() {
            write!(f, ", context: {{ ")?;
            write!(
                f,
                "{}",
                self.context
                    .iter()
                    .map(|(k, v)| format!("{k}: {v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
            write!(f, " }}")?;
        }

        if !self.message.is_empty() {
            write!(f, " => {}", self.message)?;
        }

        if let Some(source) = &self.source {
            write!(f, ", source: {source}")?;
        }

        Ok(())
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // If alternate has been specified, we will print like Debug.
        if f.alternate() {
            let mut de = f.debug_struct("Error");
            de.field("kind", &self.kind);
            de.field("message", &self.message);
            de.field("context", &self.context);
            de.field("source", &self.source);
            de.field("backtrace", &self.backtrace);
            return de.finish();
        }

        write!(f, "{}", self.kind)?;
        if !self.message.is_empty() {
            write!(f, " => {}", self.message)?;
        }
        writeln!(f)?;

        if !self.context.is_empty() {
            writeln!(f)?;
            writeln!(f, "Context:")?;
            for (k, v) in self.context.iter() {
                writeln!(f, "   {k}: {v}")?;
            }
        }
        if let Some(source) = &self.source {
            writeln!(f)?;
            writeln!(f, "Source: {source:#}")?;
        }

        if self.backtrace.status() == BacktraceStatus::Captured {
            writeln!(f)?;
            writeln!(f, "Backtrace:")?;
            writeln!(f, "{}", self.backtrace)?;
        }

        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|v| v.as_ref())
    }
}

impl Error {
    /// Create a new Error with error kind and message.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            context: Vec::default(),

            source: None,
            // `Backtrace::capture()` will check if backtrace has been enabled
            // internally. It's zero cost if backtrace is disabled.
            backtrace: Backtrace::capture(),

            retryable: false,
        }
    }

    /// Set retryable of the error.
    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    /// Add more context in error.
    pub fn with_context(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.context.push((key, value.into()));
        self
    }

    /// Set source for error.
    ///
    /// # Notes
    ///
    /// If the source has been set, we will raise a panic here.
    pub fn with_source(mut self, src: impl Into<anyhow::Error>) -> Self {
        debug_assert!(self.source.is_none(), "the source error has been set");

        self.source = Some(src.into());
        self
    }

    /// Set the backtrace for error.
    ///
    /// This function is served as testing purpose and not intended to be called
    /// by users.
    #[cfg(test)]
    fn with_backtrace(mut self, backtrace: Backtrace) -> Self {
        self.backtrace = backtrace;
        self
    }

    /// Return error's backtrace.
    ///
    /// Note: the standard way of exposing backtrace is the unstable feature [`error_generic_member_access`](https://github.com/rust-lang/rust/issues/99301).
    /// We don't provide it as it requires nightly rust.
    ///
    /// If you just want to print error with backtrace, use `Debug`, like `format!("{err:?}")`.
    ///
    /// If you use nightly rust, and want to access `iceberg::Error`'s backtrace in the standard way, you can
    /// implement a new type like this:
    ///
    /// ```ignore
    /// // assume you already have `#![feature(error_generic_member_access)]` on the top of your crate
    ///
    /// #[derive(::std::fmt::Debug)]
    /// pub struct IcebergError(iceberg::Error);
    ///
    /// impl std::fmt::Display for IcebergError {
    ///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    ///         self.0.fmt(f)
    ///     }
    /// }
    ///
    /// impl std::error::Error for IcebergError {
    ///     fn provide<'a>(&'a self, request: &mut std::error::Request<'a>) {
    ///         request.provide_ref::<std::backtrace::Backtrace>(self.0.backtrace());
    ///     }
    ///
    ///     fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    ///         self.0.source()
    ///     }
    /// }
    /// ```
    ///
    /// Additionally, you can add a clippy lint to prevent usage of the original `iceberg::Error` type.
    /// ```toml
    /// disallowed-types = [
    ///     { path = "iceberg::Error", reason = "Please use `my_crate::IcebergError` instead." },
    /// ]
    /// ```
    pub fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }

    /// Return error's kind.
    ///
    /// Users can use this method to check error's kind and take actions.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Return error's retryable status
    pub fn retryable(&self) -> bool {
        self.retryable
    }

    /// Return error's message.
    #[inline]
    pub fn message(&self) -> &str {
        self.message.as_str()
    }
}

macro_rules! define_from_err {
    ($source: path, $error_kind: path, $msg: expr) => {
        impl std::convert::From<$source> for crate::error::Error {
            fn from(v: $source) -> Self {
                Self::new($error_kind, $msg).with_source(v)
            }
        }
    };
}

define_from_err!(
    std::str::Utf8Error,
    ErrorKind::Unexpected,
    "handling invalid utf-8 characters"
);

define_from_err!(
    core::num::ParseIntError,
    ErrorKind::Unexpected,
    "parsing integer from string"
);

define_from_err!(
    std::array::TryFromSliceError,
    ErrorKind::DataInvalid,
    "failed to convert byte slice to array"
);

define_from_err!(
    std::num::TryFromIntError,
    ErrorKind::DataInvalid,
    "failed to convert integer"
);

define_from_err!(
    chrono::ParseError,
    ErrorKind::DataInvalid,
    "Failed to parse string to date or time"
);

define_from_err!(
    uuid::Error,
    ErrorKind::DataInvalid,
    "Failed to convert between uuid und iceberg value"
);

define_from_err!(
    apache_avro::Error,
    ErrorKind::DataInvalid,
    "Failure in conversion with avro"
);

define_from_err!(
    opendal::Error,
    ErrorKind::Unexpected,
    "Failure in doing io operation"
);

define_from_err!(
    url::ParseError,
    ErrorKind::DataInvalid,
    "Failed to parse url"
);

define_from_err!(
    reqwest::Error,
    ErrorKind::Unexpected,
    "Failed to execute http request"
);

define_from_err!(
    serde_json::Error,
    ErrorKind::DataInvalid,
    "Failed to parse json string"
);

define_from_err!(
    rust_decimal::Error,
    ErrorKind::DataInvalid,
    "Failed to convert decimal literal to rust decimal"
);

define_from_err!(
    parquet::errors::ParquetError,
    ErrorKind::Unexpected,
    "Failed to read a Parquet file"
);

define_from_err!(
    futures::channel::mpsc::SendError,
    ErrorKind::Unexpected,
    "Failed to send a message to a channel"
);

define_from_err!(
    arrow_schema::ArrowError,
    ErrorKind::Unexpected,
    "Arrow Schema Error"
);

define_from_err!(std::io::Error, ErrorKind::Unexpected, "IO Operation failed");

/// Converts a timestamp in milliseconds to `DateTime<Utc>`, handling errors.
///
/// # Arguments
///
/// * `timestamp_ms` - The timestamp in milliseconds to convert.
///
/// # Returns
///
/// This function returns a `Result<DateTime<Utc>, Error>` which is `Ok` with the `DateTime<Utc>` if the conversion is successful,
/// or an `Err` with an appropriate error if the timestamp is ambiguous or invalid.
pub(crate) fn timestamp_ms_to_utc(timestamp_ms: i64) -> Result<DateTime<Utc>> {
    match Utc.timestamp_millis_opt(timestamp_ms) {
        chrono::LocalResult::Single(t) => Ok(t),
        chrono::LocalResult::Ambiguous(_, _) => Err(Error::new(
            ErrorKind::Unexpected,
            "Ambiguous timestamp with two possible results",
        )),
        chrono::LocalResult::None => Err(Error::new(ErrorKind::DataInvalid, "Invalid timestamp")),
    }
    .map_err(|e| e.with_context("timestamp value", timestamp_ms.to_string()))
}

/// Helper macro to check arguments.
///
///
/// Example:
///
/// Following example check `a > 0`, otherwise returns an error.
/// ```ignore
/// use iceberg::check;
/// ensure_data_valid!(a > 0, "{} is not positive.", a);
/// ```
#[macro_export]
macro_rules! ensure_data_valid {
    ($cond: expr, $fmt: literal, $($arg:tt)*) => {
        if !$cond {
            return Err($crate::error::Error::new($crate::error::ErrorKind::DataInvalid, format!($fmt, $($arg)*)))
        }
    };
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use pretty_assertions::assert_eq;

    use super::*;

    fn generate_error_with_backtrace_disabled() -> Error {
        Error::new(
            ErrorKind::Unexpected,
            "something wrong happened".to_string(),
        )
        .with_context("path", "/path/to/file".to_string())
        .with_context("called", "send_async".to_string())
        .with_source(anyhow!("networking error"))
        .with_backtrace(Backtrace::disabled())
    }

    fn generate_error_with_backtrace_enabled() -> Error {
        Error::new(
            ErrorKind::Unexpected,
            "something wrong happened".to_string(),
        )
        .with_context("path", "/path/to/file".to_string())
        .with_context("called", "send_async".to_string())
        .with_source(anyhow!("networking error"))
        .with_backtrace(Backtrace::force_capture())
    }

    #[test]
    fn test_error_display_without_backtrace() {
        let s = format!("{}", generate_error_with_backtrace_disabled());
        assert_eq!(
            s,
            r#"Unexpected, context: { path: /path/to/file, called: send_async } => something wrong happened, source: networking error"#
        )
    }

    #[test]
    fn test_error_display_with_backtrace() {
        let s = format!("{}", generate_error_with_backtrace_enabled());
        assert_eq!(
            s,
            r#"Unexpected, context: { path: /path/to/file, called: send_async } => something wrong happened, source: networking error"#
        )
    }

    #[test]
    fn test_error_debug_without_backtrace() {
        let s = format!("{:?}", generate_error_with_backtrace_disabled());
        assert_eq!(
            s,
            r#"Unexpected => something wrong happened

Context:
   path: /path/to/file
   called: send_async

Source: networking error
"#
        )
    }

    /// Backtrace contains build information, so we just assert the header of error content.
    #[test]
    fn test_error_debug_with_backtrace() {
        let s = format!("{:?}", generate_error_with_backtrace_enabled());

        let expected = r#"Unexpected => something wrong happened

Context:
   path: /path/to/file
   called: send_async

Source: networking error

Backtrace:
   0:"#;
        assert_eq!(&s[..expected.len()], expected,);
    }
}
