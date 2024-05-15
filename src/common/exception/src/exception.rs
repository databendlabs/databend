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
use std::sync::Arc;

use backtrace::Backtrace;
use thiserror::Error;

use crate::exception_backtrace::capture;
use crate::span::pretty_print_error;
use crate::Span;

#[derive(Clone)]
pub enum ErrorCodeBacktrace {
    Serialized(Arc<String>),
    Symbols(Arc<Backtrace>),
    Address(Arc<Backtrace>),
}

impl ToString for ErrorCodeBacktrace {
    fn to_string(&self) -> String {
        match self {
            ErrorCodeBacktrace::Serialized(backtrace) => Arc::as_ref(backtrace).clone(),
            ErrorCodeBacktrace::Symbols(backtrace) => format!("{:?}", backtrace),
            ErrorCodeBacktrace::Address(backtrace) => {
                let frames_address = backtrace
                    .frames()
                    .iter()
                    .map(|f| (f.ip() as usize, f.symbol_address() as usize))
                    .collect::<Vec<_>>();
                format!("{:?}", frames_address)
            }
        }
    }
}

impl From<&str> for ErrorCodeBacktrace {
    fn from(s: &str) -> Self {
        Self::Serialized(Arc::new(s.to_string()))
    }
}

impl From<String> for ErrorCodeBacktrace {
    fn from(s: String) -> Self {
        Self::Serialized(Arc::new(s))
    }
}

impl From<Arc<String>> for ErrorCodeBacktrace {
    fn from(s: Arc<String>) -> Self {
        Self::Serialized(s)
    }
}

impl From<Backtrace> for ErrorCodeBacktrace {
    fn from(bt: Backtrace) -> Self {
        Self::Symbols(Arc::new(bt))
    }
}

impl From<&Backtrace> for ErrorCodeBacktrace {
    fn from(bt: &Backtrace) -> Self {
        Self::Serialized(Arc::new(format!("{:?}", bt)))
    }
}

impl From<Arc<Backtrace>> for ErrorCodeBacktrace {
    fn from(bt: Arc<Backtrace>) -> Self {
        Self::Symbols(bt)
    }
}

#[derive(Error)]
pub struct ErrorCode {
    code: u16,
    name: String,
    display_text: String,
    detail: String,
    span: Span,
    // cause is only used to contain an `anyhow::Error`.
    // TODO: remove `cause` when we completely get rid of `anyhow::Error`.
    cause: Option<Box<dyn std::error::Error + Sync + Send>>,
    backtrace: Option<ErrorCodeBacktrace>,
}

impl ErrorCode {
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

    /// Set backtrace info for this error.
    ///
    /// Useful when trying to keep original backtrace
    pub fn set_backtrace(mut self, bt: Option<impl Into<ErrorCodeBacktrace>>) -> Self {
        if let Some(b) = bt {
            self.backtrace = Some(b.into());
        }
        self
    }

    pub fn backtrace(&self) -> Option<ErrorCodeBacktrace> {
        self.backtrace.clone()
    }

    pub fn backtrace_str(&self) -> String {
        self.backtrace
            .as_ref()
            .map_or("".to_string(), |x| x.to_string())
    }
}

pub type Result<T, E = ErrorCode> = std::result::Result<T, E>;

impl Debug for ErrorCode {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}. Code: {}, Text = {}.",
            self.name,
            self.code(),
            self.message(),
        )?;

        match self.backtrace.as_ref() {
            None => write!(
                f,
                "\n\n<Backtrace disabled by default. Please use RUST_BACKTRACE=1 to enable> "
            ),
            Some(backtrace) => {
                // TODO: Custom stack frame format for print
                match backtrace {
                    ErrorCodeBacktrace::Symbols(backtrace) => write!(f, "\n\n{:?}", backtrace),
                    ErrorCodeBacktrace::Serialized(backtrace) => write!(f, "\n\n{}", backtrace),
                    ErrorCodeBacktrace::Address(backtrace) => {
                        let frames_address = backtrace
                            .frames()
                            .iter()
                            .map(|f| (f.ip() as usize, f.symbol_address() as usize))
                            .collect::<Vec<_>>();
                        write!(f, "\n\n{:?}", frames_address)
                    }
                }
            }
        }
    }
}

impl Display for ErrorCode {
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

impl ErrorCode {
    /// All std error will be converted to InternalError
    pub fn from_std_error<T: std::error::Error>(error: T) -> Self {
        ErrorCode {
            code: 1001,
            name: String::from("FromStdError"),
            display_text: error.to_string(),
            detail: String::new(),
            span: None,
            cause: None,
            backtrace: capture(),
        }
    }

    pub fn from_string(error: String) -> Self {
        ErrorCode {
            code: 1001,
            name: String::from("Internal"),
            display_text: error,
            detail: String::new(),
            span: None,
            cause: None,
            backtrace: capture(),
        }
    }

    pub fn from_string_no_backtrace(error: String) -> Self {
        ErrorCode {
            code: 1001,
            name: String::from("Internal"),
            display_text: error,
            detail: String::new(),
            span: None,
            cause: None,
            backtrace: None,
        }
    }

    pub fn create(
        code: u16,
        name: impl ToString,
        display_text: String,
        detail: String,
        cause: Option<Box<dyn std::error::Error + Sync + Send>>,
        backtrace: Option<ErrorCodeBacktrace>,
    ) -> ErrorCode {
        ErrorCode {
            code,
            display_text,
            detail,
            span: None,
            cause,
            backtrace,
            name: name.to_string(),
        }
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
///     "Code: 1067, Text = 123, cause: an error occurred when formatting an argument.",
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

impl Clone for ErrorCode {
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
    }
}
