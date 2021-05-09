// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#![allow(non_snake_case)]

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use backtrace::Backtrace;
use thiserror::Error;

#[derive(Error)]
pub struct ErrorCodes {
    code: u16,
    display_text: String,
    cause: Option<Box<dyn std::error::Error + Sync + Send>>,
    backtrace: Option<Backtrace>
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
                pub fn $body(display_text: String) -> ErrorCodes {
                    ErrorCodes {
                        code:$code,
                        display_text,
                        cause: None,
                        backtrace: Some(Backtrace::new()),
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
    SyntexException(5),
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

    UnknownException(1000),
    TokioError(1001)
}

pub type Result<T> = std::result::Result<T, ErrorCodes>;

impl Debug for ErrorCodes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.cause
            .as_ref()
            .map(|cause| write!(f, "Code: {}, displayText = {:?}.", self.code, cause))
            .unwrap_or_else(|| {
                write!(
                    f,
                    "Code: {}, displayText = {:?}.",
                    self.code.clone(),
                    self.display_text.clone()
                )?;
                match self.backtrace.as_ref() {
                    None => Ok(()), // no backtrace
                    Some(backtrace) => {
                        // TODO: Custom stack frame format for print
                        write!(f, "\n\n{:?}", backtrace)
                    }
                }
            })
    }
}

impl Display for ErrorCodes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.cause
            .as_ref()
            .map(|cause| write!(f, "Code: {}, displayText = {}.", self.code, cause))
            .unwrap_or_else(|| {
                write!(
                    f,
                    "Code: {}, displayText = {}.",
                    self.code.clone(),
                    self.display_text.clone()
                )
            })
    }
}

#[derive(Error)]
enum OtherErrors {
    AnyHow { error: anyhow::Error }
}

impl Display for OtherErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OtherErrors::AnyHow { error } => write!(f, "{}", error)
        }
    }
}

impl Debug for OtherErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OtherErrors::AnyHow { error } => write!(f, "{:?}", error)
        }
    }
}

impl ErrorCodes {
    pub fn from_anyhow(error: anyhow::Error) -> ErrorCodes {
        ErrorCodes {
            code: 1002,
            display_text: String::from(""),
            cause: Some(Box::new(OtherErrors::AnyHow { error })),
            backtrace: None
        }
    }

    pub fn from_parse_int(error: std::num::ParseIntError) -> ErrorCodes {
        ErrorCodes {
            code: 1002,
            display_text: format!("{}", error),
            cause: None,
            backtrace: Some(Backtrace::new())
        }
    }

    pub fn from_parse_float(error: std::num::ParseFloatError) -> ErrorCodes {
        ErrorCodes {
            code: 1002,
            display_text: format!("{}", error),
            cause: None,
            backtrace: Some(Backtrace::new())
        }
    }

    pub fn from_arrow(error: common_arrow::arrow::error::ArrowError) -> ErrorCodes {
        ErrorCodes {
            code: 1002,
            display_text: format!("{}", error),
            cause: None,
            backtrace: Some(Backtrace::new())
        }
    }

    pub fn from_serde(error: serde_json::Error) -> ErrorCodes {
        ErrorCodes {
            code: 1002,
            display_text: format!("{}", error),
            cause: None,
            backtrace: Some(Backtrace::new())
        }
    }

    pub fn from_parser(error: sqlparser::parser::ParserError) -> ErrorCodes {
        ErrorCodes {
            code: 1002,
            display_text: format!("{}", error),
            cause: None,
            backtrace: Some(Backtrace::new())
        }
    }
}
