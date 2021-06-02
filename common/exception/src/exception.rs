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

#[derive(Clone)]
pub enum ErrorCodesBacktrace {
    Serialized(Arc<String>),
    Origin(Arc<Backtrace>),
}

impl ToString for ErrorCodesBacktrace {
    fn to_string(&self) -> String {
        match self {
            ErrorCodesBacktrace::Serialized(backtrace) => Arc::as_ref(backtrace).clone(),
            ErrorCodesBacktrace::Origin(backtrace) => {
                format!("{:?}", backtrace)
            }
        }
    }
}

#[derive(Error)]
pub struct ErrorCodes {
    code: u16,
    display_text: String,
    cause: Option<Box<dyn std::error::Error + Sync + Send>>,
    backtrace: Option<ErrorCodesBacktrace>,
}

impl ErrorCodes {
    pub fn code(&self) -> u16 {
        self.code
    }

    pub fn message(&self) -> String {
        self.cause
            .as_ref()
            .map(|cause| format!("{:?}", cause))
            .unwrap_or_else(|| self.display_text.clone())
    }

    pub fn backtrace(&self) -> Option<ErrorCodesBacktrace> {
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
            impl ErrorCodes {
                $(
                pub fn $body(display_text: impl Into<String>) -> ErrorCodes {
                    ErrorCodes {
                        code:$code,
                        display_text: display_text.into(),
                        cause: None,
                        backtrace: Some(ErrorCodesBacktrace::Origin(Arc::new(Backtrace::new()))),
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

    UnknownException(1000),
    TokioError(1001)
}

pub type Result<T> = std::result::Result<T, ErrorCodes>;

impl Debug for ErrorCodes {
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
                    ErrorCodesBacktrace::Origin(backtrace) => write!(f, "\n\n{:?}", backtrace),
                    ErrorCodesBacktrace::Serialized(backtrace) => write!(f, "\n\n{:?}", backtrace),
                }
            }
        }
    }
}

impl Display for ErrorCodes {
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

impl From<anyhow::Error> for ErrorCodes {
    fn from(error: anyhow::Error) -> Self {
        ErrorCodes {
            code: 1002,
            display_text: String::from(""),
            cause: Some(Box::new(OtherErrors::AnyHow { error })),
            backtrace: None,
        }
    }
}

impl From<std::num::ParseIntError> for ErrorCodes {
    fn from(error: std::num::ParseIntError) -> Self {
        ErrorCodes::from_std_error(error)
    }
}

impl From<std::num::ParseFloatError> for ErrorCodes {
    fn from(error: std::num::ParseFloatError) -> Self {
        ErrorCodes::from_std_error(error)
    }
}

impl From<common_arrow::arrow::error::ArrowError> for ErrorCodes {
    fn from(error: common_arrow::arrow::error::ArrowError) -> Self {
        ErrorCodes::from_std_error(error)
    }
}

impl From<serde_json::Error> for ErrorCodes {
    fn from(error: serde_json::Error) -> Self {
        ErrorCodes::from_std_error(error)
    }
}

impl From<sqlparser::parser::ParserError> for ErrorCodes {
    fn from(error: sqlparser::parser::ParserError) -> Self {
        ErrorCodes::from_std_error(error)
    }
}

impl From<std::io::Error> for ErrorCodes {
    fn from(error: std::io::Error) -> Self {
        ErrorCodes::from_std_error(error)
    }
}

impl ErrorCodes {
    pub fn from_std_error<T: std::error::Error>(error: T) -> Self {
        ErrorCodes {
            code: 1002,
            display_text: format!("{}", error),
            cause: None,
            backtrace: Some(ErrorCodesBacktrace::Origin(Arc::new(Backtrace::new()))),
        }
    }

    pub fn create(
        code: u16,
        display_text: String,
        backtrace: Option<ErrorCodesBacktrace>,
    ) -> ErrorCodes {
        ErrorCodes {
            code,
            display_text,
            cause: None,
            backtrace,
        }
    }
}
