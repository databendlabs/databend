#![allow(non_snake_case)]

use std::error::Error;
use std::fmt::{Display, Formatter, Debug};

use sqlparser::parser::ParserError;
use thiserror::Error;

#[derive(Error)]
pub struct ErrorCodes {
    code: u16,
    display_text: String,
    cause: Option<Box<dyn Error>>,
    #[cfg(feature = "backtrace")]
    backtrace: None<std::backtrace::Backtrace>,
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
                        display_text:display_text,
                        cause: None,
                        #[cfg(feature = "backtrace")]
                        backtrace: Some(std::backtrace::Backtrace::capture()),
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

    UnknownException(1000),
    TokioError(1001)
}

pub type Result<T> = std::result::Result<T, ErrorCodes>;

impl Debug for ErrorCodes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.cause.as_ref().map(|cause| {
            write!(f, "Code: {}, displayText = {:?}.", self.code, cause)
        }).unwrap_or_else(|| {
            write!(f, "Code: {}, displayText = {:?}.", self.code.clone(), self.display_text.clone())
        })
    }
}

impl Display for ErrorCodes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.cause.as_ref().map(|cause| {
            write!(f, "Code: {}, displayText = {}.", self.code, cause)
        }).unwrap_or_else(|| {
            write!(f, "Code: {}, displayText = {}.", self.code.clone(), self.display_text.clone())
        })
    }
}

#[derive(Error)]
enum OtherErrors {
    AnyHow { error: anyhow::Error },
    ParserError { error: sqlparser::parser::ParserError },
}

impl Display for OtherErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OtherErrors::AnyHow { error } => write!(f, "{}", error),
            OtherErrors::ParserError { error } => write!(f, "{}", error)
        }
    }
}

impl Debug for OtherErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OtherErrors::AnyHow { error } => write!(f, "{:?}", error),
            OtherErrors::ParserError { error } => write!(f, "{:?}", error),
        }
    }
}

impl From<sqlparser::parser::ParserError> for ErrorCodes {
    fn from(error: ParserError) -> Self {
        ErrorCodes {
            code: 5,
            display_text: String::from(""),
            cause: Some(Box::new(OtherErrors::ParserError { error: error })),
            #[cfg(feature = "backtrace")]
            backtrace: None,
        }
    }
}

impl ErrorCodes {
    pub fn from_anyhow(error: anyhow::Error) -> ErrorCodes {
        ErrorCodes {
            code: 1002,
            display_text: String::from(""),
            cause: Some(Box::new(OtherErrors::AnyHow { error: error })),
            #[cfg(feature = "backtrace")]
            backtrace: None,
        }
    }
}

