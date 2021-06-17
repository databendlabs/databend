// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#![allow(non_snake_case)]

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use backtrace::Backtrace;
use thiserror::Error;

pub static ABORT_SESSION: u16 = 42;
pub static ABORT_QUERY: u16 = 43;

#[derive(Clone)]
pub enum ErrorCodeBacktrace {
    Serialized(Arc<String>),
    Origin(Arc<Backtrace>),
}

impl ToString for ErrorCodeBacktrace {
    fn to_string(&self) -> String {
        match self {
            ErrorCodeBacktrace::Serialized(backtrace) => Arc::as_ref(backtrace).clone(),
            ErrorCodeBacktrace::Origin(backtrace) => {
                format!("{:?}", backtrace)
            }
        }
    }
}

#[derive(Error)]
pub struct ErrorCode {
    code: u16,
    display_text: String,
    // cause is only used to contain an `anyhow::Error`.
    // TODO: remove `cause` when we completely get rid of `anyhow::Error`.
    cause: Option<Box<dyn std::error::Error + Sync + Send>>,
    backtrace: Option<ErrorCodeBacktrace>,
}

impl ErrorCode {
    pub fn code(&self) -> u16 {
        self.code
    }

    pub fn message(&self) -> String {
        self.cause
            .as_ref()
            .map(|cause| format!("{}\n{:?}", self.display_text, cause))
            .unwrap_or_else(|| self.display_text.clone())
    }

    pub fn add_message(self, msg: String) -> Self {
        Self {
            code: self.code(),
            display_text: format!("{}\n{}", msg, self.display_text),
            cause: self.cause,
            backtrace: self.backtrace,
        }
    }

    pub fn backtrace(&self) -> Option<ErrorCodeBacktrace> {
        self.backtrace.clone()
    }

    pub fn backtrace_str(&self) -> String {
        match self.backtrace.as_ref() {
            None => "".to_string(),
            Some(backtrace) => backtrace.to_string(),
        }
    }
}

macro_rules! as_item {
    ($i:item) => {
        $i
    };
}

macro_rules! build_exceptions {
    ($($body:tt($code:expr)),*) => {
        as_item! {
            impl ErrorCode {
                $(
                pub fn $body(display_text: impl Into<String>) -> ErrorCode {
                    ErrorCode {
                        code:$code,
                        display_text: display_text.into(),
                        cause: None,
                        backtrace: Some(ErrorCodeBacktrace::Origin(Arc::new(Backtrace::new()))),
                    }
                })*
            }
        }
    }
}

build_exceptions! {
    Ok(0),
    UnknownTypeOfQuery(1),
    UnImplement(2),
    UnknownDatabase(3),
    UnknownSetting(4),
    SyntaxException(5),
    BadArguments(6),
    IllegalDataType(7),
    UnknownFunction(8),
    IllegalFunctionState(9),
    BadDataValueType(10),
    UnknownPlan(11),
    IllegalPipelineState(12),
    BadTransformType(13),
    IllegalTransformConnectionState(14),
    LogicalError(15),
    EmptyData(16),
    DataStructMissMatch(17),
    BadDataArrayLength(18),
    UnknownContextID(19),
    UnknownVariable(20),
    UnknownTableFunction(21),
    BadOption(22),
    CannotReadFile(23),
    ParquetError(24),
    UnknownTable(25),
    IllegalAggregateExp(26),
    UnknownAggregateFunction(27),
    NumberArgumentsNotMatch(28),
    NotFoundStream(29),
    EmptyDataFromServer(30),
    NotFoundLocalNode(31),
    PlanScheduleError(32),
    BadPlanInputs(33),
    DuplicateClusterNode(34),
    NotFoundClusterNode(35),
    BadAddressFormat(36),
    DnsParseError(37),
    CannotConnectNode(38),
    DuplicateGetStream(39),
    Timeout(40),
    TooManyUserConnections(41),
    AbortedSession(ABORT_SESSION),
    AbortedQuery(ABORT_QUERY),
    NotFoundSession(44),

    UnknownException(1000),
    TokioError(1001)
}

// Store errors
build_exceptions! {

    FileMetaNotFound(2001),
    FileDamaged(2002)
}

pub type Result<T> = std::result::Result<T, ErrorCode>;

impl Debug for ErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Code: {}, displayText = {}.",
            self.code(),
            self.message(),
        )?;

        match self.backtrace.as_ref() {
            None => Ok(()), // no backtrace
            Some(backtrace) => {
                // TODO: Custom stack frame format for print
                match backtrace {
                    ErrorCodeBacktrace::Origin(backtrace) => write!(f, "\n\n{:?}", backtrace),
                    ErrorCodeBacktrace::Serialized(backtrace) => write!(f, "\n\n{:?}", backtrace),
                }
            }
        }
    }
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Code: {}, displayText = {}.",
            self.code(),
            self.message(),
        )
    }
}

#[derive(Error)]
enum OtherErrors {
    AnyHow { error: anyhow::Error },
}

impl Display for OtherErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OtherErrors::AnyHow { error } => write!(f, "{}", error),
        }
    }
}

impl Debug for OtherErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OtherErrors::AnyHow { error } => write!(f, "{:?}", error),
        }
    }
}

impl From<anyhow::Error> for ErrorCode {
    fn from(error: anyhow::Error) -> Self {
        ErrorCode {
            code: 1002,
            display_text: String::from(""),
            cause: Some(Box::new(OtherErrors::AnyHow { error })),
            backtrace: None,
        }
    }
}

impl From<std::num::ParseIntError> for ErrorCode {
    fn from(error: std::num::ParseIntError) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<std::num::ParseFloatError> for ErrorCode {
    fn from(error: std::num::ParseFloatError) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<common_arrow::arrow::error::ArrowError> for ErrorCode {
    fn from(error: common_arrow::arrow::error::ArrowError) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<serde_json::Error> for ErrorCode {
    fn from(error: serde_json::Error) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<sqlparser::parser::ParserError> for ErrorCode {
    fn from(error: sqlparser::parser::ParserError) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<std::io::Error> for ErrorCode {
    fn from(error: std::io::Error) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl ErrorCode {
    pub fn from_std_error<T: std::error::Error>(error: T) -> Self {
        ErrorCode {
            code: 1002,
            display_text: format!("{}", error),
            cause: None,
            backtrace: Some(ErrorCodeBacktrace::Origin(Arc::new(Backtrace::new()))),
        }
    }

    pub fn create(
        code: u16,
        display_text: String,
        backtrace: Option<ErrorCodeBacktrace>,
    ) -> ErrorCode {
        ErrorCode {
            code,
            display_text,
            cause: None,
            backtrace,
        }
    }
}

/// Provides the `map_err_to_code` method for `Result`.
///
/// ```
/// use common_exception::ToErrorCode;
/// use common_exception::ErrorCode;
///
/// let x: std::result::Result<(), std::fmt::Error> = Err(std::fmt::Error {});
/// let y: common_exception::Result<()> =
///     x.map_err_to_code(ErrorCode::UnknownException, || 123);
///
/// assert_eq!(
///     "Code: 1000, displayText = 123, cause: an error occurred when formatting an argument.",
///     format!("{}", y.unwrap_err())
/// );
/// ```
pub trait ToErrorCode<T, E, CtxFn> {
    /// Wrap the error value with ErrorCode that is evaluated lazily
    /// only once an error does occur.
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
where E: std::error::Error + Send + Sync + 'static
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
