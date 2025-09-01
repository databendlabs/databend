// Copyright 2021 Datafuse Labs
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

#![allow(non_snake_case)]

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::marker::PhantomData;

use thiserror::Error;

use crate::exception_backtrace::capture;
use crate::span::pretty_print_error;
use crate::span::Span;
use crate::ErrorFrame;
use crate::StackTrace;

#[derive(Error)]
pub struct ErrorCode<C = ()> {
    pub(crate) code: u16,
    pub(crate) name: String,
    pub(crate) display_text: String,
    pub(crate) detail: String,
    pub(crate) span: Span,
    // cause is only used to contain an `anyhow::Error`.
    // TODO: remove `cause` when we completely get rid of `anyhow::Error`.
    pub(crate) cause: Option<Box<dyn std::error::Error + Sync + Send>>,
    pub(crate) stacks: Vec<ErrorFrame>,
    pub(crate) backtrace: StackTrace,
    pub(crate) _phantom: PhantomData<C>,
}

impl<C> ErrorCode<C> {
    pub fn code(&self) -> u16 {
        self.code
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn display_text(&self) -> String {
        if let Some(cause) = &self.cause {
            format!("{}\n{:?}", self.display_text, cause)
        } else {
            self.display_text.clone()
        }
    }

    pub fn message(&self) -> String {
        let msg = self.display_text();
        if self.detail.is_empty() {
            msg
        } else {
            format!("{}\n{}", msg, self.detail)
        }
    }

    pub fn detail(&self) -> String {
        self.detail.clone()
    }

    #[must_use]
    pub fn add_message(self, msg: impl AsRef<str>) -> Self {
        Self {
            display_text: if self.display_text.is_empty() {
                msg.as_ref().to_string()
            } else {
                format!("{}\n{}", msg.as_ref(), self.display_text)
            },
            ..self
        }
    }

    #[must_use]
    pub fn add_message_back(self, msg: impl AsRef<str>) -> Self {
        Self {
            display_text: if self.display_text.is_empty() {
                msg.as_ref().to_string()
            } else {
                format!("{}\n{}", self.display_text, msg.as_ref())
            },
            ..self
        }
    }

    pub fn add_detail_back(self, msg: impl AsRef<str>) -> Self {
        Self {
            detail: if self.detail.is_empty() {
                msg.as_ref().to_string()
            } else {
                format!("{}\n{}", self.detail, msg.as_ref())
            },
            ..self
        }
    }

    pub fn add_detail(self, msg: impl AsRef<str>) -> Self {
        Self {
            detail: if self.detail.is_empty() {
                msg.as_ref().to_string()
            } else {
                format!("{}\n{}", msg.as_ref(), self.detail)
            },
            ..self
        }
    }

    pub fn span(&self) -> Span {
        self.span
    }

    /// Set sql span for this error.
    ///
    /// Used to pretty print the error when the error is related to a sql statement.
    pub fn set_span(self, span: Span) -> Self {
        Self { span, ..self }
    }

    /// Pretty display the error message onto sql statement if span is available.
    pub fn display_with_sql(mut self, sql: &str) -> Self {
        if let Some(span) = self.span.take() {
            self.display_text =
                pretty_print_error(sql, vec![(span, self.display_text.to_string())]);
        }
        self
    }

    pub fn backtrace(&self) -> StackTrace {
        self.backtrace.clone()
    }

    pub fn backtrace_str(&self) -> String {
        format!("{:?}", &self.backtrace)
    }

    pub fn stacks(&self) -> &[ErrorFrame] {
        &self.stacks
    }

    pub fn set_stacks(mut self, stacks: Vec<ErrorFrame>) -> Self {
        self.stacks = stacks;
        self
    }
}

pub type Result<T, C = ()> = std::result::Result<T, ErrorCode<C>>;

impl<C> Debug for ErrorCode<C> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}. Code: {}, Text = {}.",
            self.name,
            self.code(),
            self.message(),
        )?;

        match self.backtrace.frames.is_empty() {
            true => write!(
                f,
                "\n\n<Backtrace disabled by default. Please use RUST_BACKTRACE=1 to enable> "
            ),
            false => write!(f, "\n\n{:?}", &self.backtrace),
        }
    }
}

impl<C> Display for ErrorCode<C> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}. Code: {}, Text = {}.",
            self.name,
            self.code(),
            self.message(),
        )
    }
}

impl<C> ErrorCode<C> {
    /// All std error will be converted to InternalError
    #[track_caller]
    pub fn from_std_error<T: std::error::Error>(error: T) -> Self {
        ErrorCode {
            code: 1001,
            name: String::from("FromStdError"),
            display_text: error.to_string(),
            detail: String::new(),
            span: None,
            cause: None,
            backtrace: capture(),
            stacks: vec![],
            _phantom: PhantomData::<C>,
        }
        .with_context(error.to_string())
    }

    pub fn from_string(error: String) -> Self {
        ErrorCode {
            code: 1001,
            name: String::from("Internal"),
            display_text: error.clone(),
            detail: String::new(),
            span: None,
            cause: None,
            backtrace: capture(),
            stacks: vec![],
            _phantom: PhantomData::<C>,
        }
        .with_context(error)
    }

    pub fn from_string_no_backtrace(error: String) -> Self {
        ErrorCode {
            code: 1001,
            name: String::from("Internal"),
            display_text: error,
            detail: String::new(),
            span: None,
            cause: None,
            stacks: vec![],
            backtrace: StackTrace::no_capture(),
            _phantom: PhantomData::<C>,
        }
    }

    pub fn create(
        code: u16,
        name: impl ToString,
        display_text: String,
        detail: String,
        cause: Option<Box<dyn std::error::Error + Sync + Send>>,
        backtrace: StackTrace,
    ) -> Self {
        ErrorCode {
            code,
            display_text: display_text.clone(),
            detail,
            span: None,
            cause,
            backtrace,
            name: name.to_string(),
            stacks: vec![],
            _phantom: PhantomData::<C>,
        }
        .with_context(display_text)
    }
}

impl ErrorCode {
    pub fn aborting() -> Self {
        ErrorCode::AbortedQuery(
            "Aborted query, because the server is shutting down or the query was killed.",
        )
    }
}

/// Provides the `map_err_to_code` method for `Result`.
///
/// ```
/// use databend_common_exception::ErrorCode;
/// use databend_common_exception::ToErrorCode;
///
/// let x: std::result::Result<(), std::fmt::Error> = Err(std::fmt::Error {});
/// let y: databend_common_exception::Result<()> =
///     x.map_err_to_code(ErrorCode::UnknownException, || 123);
///
/// assert_eq!(
///     "UnknownException. Code: 1067, Text = 123, cause: an error occurred when formatting an argument.",
///     y.unwrap_err().to_string()
/// );
/// ```
pub trait ToErrorCode<T, E, CtxFn>
where E: Display + Send + Sync + 'static
{
    /// Wrap the error value with ErrorCode. It is lazily evaluated:
    /// only when an error does occur.
    ///
    /// `err_code_fn` is one of the ErrorCode builder function such as `ErrorCode::Ok`.
    /// `context_fn` builds display_text for the ErrorCode.
    fn map_err_to_code<ErrFn, D>(self, err_code_fn: ErrFn, context_fn: CtxFn) -> Result<T>
    where
        ErrFn: FnOnce(String) -> ErrorCode,
        D: Display,
        CtxFn: FnOnce() -> D;
}

impl<T, E, CtxFn> ToErrorCode<T, E, CtxFn> for std::result::Result<T, E>
where E: Display + Send + Sync + 'static
{
    fn map_err_to_code<ErrFn, D>(self, make_exception: ErrFn, context_fn: CtxFn) -> Result<T>
    where
        ErrFn: FnOnce(String) -> ErrorCode,
        D: Display,
        CtxFn: FnOnce() -> D,
    {
        self.map_err(|error| {
            let err_text = format!("{}, cause: {}", context_fn(), error);
            make_exception(err_text)
        })
    }
}

impl<C> Clone for ErrorCode<C> {
    fn clone(&self) -> Self {
        ErrorCode::create(
            self.code(),
            &self.name,
            self.display_text(),
            self.detail.clone(),
            None,
            self.backtrace(),
        )
        .set_span(self.span())
        .set_stacks(self.stacks().to_vec())
    }
}

/// Common error code groups for use with ErrorCodeResultExt
pub mod error_code_groups {
    use super::ErrorCode;

    /// All "unknown resource" errors that typically mean "not found"
    pub const UNKNOWN_RESOURCE: &[u16] = &[
        ErrorCode::UNKNOWN_DATABASE,
        ErrorCode::UNKNOWN_TABLE,
        ErrorCode::UNKNOWN_CATALOG,
        ErrorCode::UNKNOWN_VIEW,
        ErrorCode::UNKNOWN_COLUMN,
        ErrorCode::UNKNOWN_SEQUENCE,
        ErrorCode::UNKNOWN_DICTIONARY,
        ErrorCode::UNKNOWN_ROLE,
        ErrorCode::UNKNOWN_NETWORK_POLICY,
        ErrorCode::UNKNOWN_CONNECTION,
        ErrorCode::UNKNOWN_FILE_FORMAT,
        ErrorCode::UNKNOWN_USER,
        ErrorCode::UNKNOWN_PASSWORD_POLICY,
        ErrorCode::UNKNOWN_STAGE,
        ErrorCode::UNKNOWN_WORKLOAD,
        ErrorCode::UNKNOWN_VARIABLE,
        ErrorCode::UNKNOWN_SESSION,
        ErrorCode::UNKNOWN_QUERY,
        ErrorCode::UNKNOWN_ROW_ACCESS_POLICY,
        ErrorCode::UNKNOWN_DATAMASK,
    ];
}

/// Extension trait for Result<T, ErrorCode> to handle common error patterns
pub trait ErrorCodeResultExt<T> {
    /// Converts any of the specified error codes to None, other errors propagate, success becomes Some(T)
    fn or_error_codes(self, codes: &[u16]) -> Result<Option<T>>;

    /// Converts UNKNOWN_DATABASE errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_database(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_TABLE errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_table(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_CATALOG errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_catalog(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_VIEW errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_view(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_COLUMN errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_column(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_SEQUENCE errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_sequence(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_DICTIONARY errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_dictionary(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_ROLE errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_role(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_NETWORK_POLICY errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_network_policy(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_CONNECTION errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_connection(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_FILE_FORMAT errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_file_format(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_USER errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_user(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_PASSWORD_POLICY errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_password_policy(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_STAGE errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_stage(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_WORKLOAD errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_workload(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_VARIABLE errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_variable(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_SESSION errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_session(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_QUERY errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_query(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_ROW_ACCESS_POLICY errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_row_access_policy(self) -> Result<Option<T>>;

    /// Converts UNKNOWN_DATAMASK errors to None, other errors propagate, success becomes Some(T)
    fn or_unknown_datamask(self) -> Result<Option<T>>;

    /// Converts common "not found" errors to None (database, table, sequence, etc.)
    fn or_unknown_resource(self) -> Result<Option<T>>;
}

impl<T> ErrorCodeResultExt<T> for Result<T> {
    fn or_error_codes(self, codes: &[u16]) -> Result<Option<T>> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(e) if codes.iter().any(|c| *c == e.code()) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn or_unknown_database(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_DATABASE])
    }

    fn or_unknown_table(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_TABLE])
    }

    fn or_unknown_catalog(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_CATALOG])
    }

    fn or_unknown_view(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_VIEW])
    }

    fn or_unknown_column(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_COLUMN])
    }

    fn or_unknown_sequence(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_SEQUENCE])
    }

    fn or_unknown_dictionary(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_DICTIONARY])
    }

    fn or_unknown_role(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_ROLE])
    }

    fn or_unknown_network_policy(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_NETWORK_POLICY])
    }

    fn or_unknown_connection(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_CONNECTION])
    }

    fn or_unknown_file_format(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_FILE_FORMAT])
    }

    fn or_unknown_user(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_USER])
    }

    fn or_unknown_password_policy(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_PASSWORD_POLICY])
    }

    fn or_unknown_stage(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_STAGE])
    }

    fn or_unknown_workload(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_WORKLOAD])
    }

    fn or_unknown_variable(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_VARIABLE])
    }

    fn or_unknown_session(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_SESSION])
    }

    fn or_unknown_query(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_QUERY])
    }

    fn or_unknown_row_access_policy(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_ROW_ACCESS_POLICY])
    }

    fn or_unknown_datamask(self) -> Result<Option<T>> {
        self.or_error_codes(&[ErrorCode::UNKNOWN_DATAMASK])
    }

    fn or_unknown_resource(self) -> Result<Option<T>> {
        self.or_error_codes(error_code_groups::UNKNOWN_RESOURCE)
    }
}
