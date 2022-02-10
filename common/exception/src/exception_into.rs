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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use backtrace::Backtrace;

use crate::exception::ErrorCodeBacktrace;
use crate::ErrorCode;

#[derive(thiserror::Error)]
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

impl From<std::net::AddrParseError> for ErrorCode {
    fn from(error: std::net::AddrParseError) -> Self {
        ErrorCode::BadAddressFormat(format!("Bad address format, cause: {}", error))
    }
}

impl From<anyhow::Error> for ErrorCode {
    fn from(error: anyhow::Error) -> Self {
        ErrorCode::create(
            1002,
            format!("{}, source: {:?}", error, error.source()),
            Some(Box::new(OtherErrors::AnyHow { error })),
            Some(ErrorCodeBacktrace::Origin(Arc::new(Backtrace::new()))),
        )
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

impl From<Box<bincode::ErrorKind>> for ErrorCode {
    fn from(error: Box<bincode::ErrorKind>) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<serde_json::Error> for ErrorCode {
    fn from(error: serde_json::Error) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<std::convert::Infallible> for ErrorCode {
    fn from(v: std::convert::Infallible) -> Self {
        ErrorCode::from_std_error(v)
    }
}

impl From<sqlparser::parser::ParserError> for ErrorCode {
    fn from(error: sqlparser::parser::ParserError) -> Self {
        ErrorCode::SyntaxException(format!("{}", error))
    }
}

impl From<std::io::Error> for ErrorCode {
    fn from(error: std::io::Error) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<std::string::FromUtf8Error> for ErrorCode {
    fn from(error: std::string::FromUtf8Error) -> Self {
        ErrorCode::BadBytes(format!(
            "Bad bytes, cannot parse bytes with UTF8, cause: {}",
            error
        ))
    }
}

// ===  prost error ===
impl From<prost::EncodeError> for ErrorCode {
    fn from(error: prost::EncodeError) -> Self {
        ErrorCode::BadBytes(format!(
            "Bad bytes, cannot parse bytes with prost, cause: {}",
            error
        ))
    }
}

// ===  octocrab error ===
impl From<octocrab::Error> for ErrorCode {
    fn from(error: octocrab::Error) -> Self {
        ErrorCode::NetworkRequestError(format!("octocrab error, cause: {}", error))
    }
}

// ===  ser/de to/from tonic::Status ===
#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub struct SerializedError {
    code: u16,
    message: String,
    backtrace: String,
}

impl Display for SerializedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "Code: {}, displayText = {}.", self.code, self.message,)
    }
}

impl From<ErrorCode> for SerializedError {
    fn from(e: ErrorCode) -> Self {
        SerializedError {
            code: e.code(),
            message: e.message(),
            backtrace: e.backtrace_str(),
        }
    }
}

impl From<SerializedError> for ErrorCode {
    fn from(se: SerializedError) -> Self {
        ErrorCode::create(
            se.code,
            se.message,
            None,
            Some(ErrorCodeBacktrace::Serialized(Arc::new(se.backtrace))),
        )
    }
}

impl From<tonic::Status> for ErrorCode {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::Unknown => {
                let details = status.details();
                if details.is_empty() {
                    return ErrorCode::UnknownException(status.message());
                }
                match serde_json::from_slice::<SerializedError>(details) {
                    Err(error) => ErrorCode::from(error),
                    Ok(serialized_error) => match serialized_error.backtrace.len() {
                        0 => ErrorCode::create(
                            serialized_error.code,
                            serialized_error.message,
                            None,
                            None,
                        ),
                        _ => ErrorCode::create(
                            serialized_error.code,
                            serialized_error.message,
                            None,
                            Some(ErrorCodeBacktrace::Serialized(Arc::new(
                                serialized_error.backtrace,
                            ))),
                        ),
                    },
                }
            }
            _ => ErrorCode::UnImplement(status.to_string()),
        }
    }
}

impl From<ErrorCode> for tonic::Status {
    fn from(err: ErrorCode) -> Self {
        let rst_json = serde_json::to_vec::<SerializedError>(&SerializedError {
            code: err.code(),
            message: err.message(),
            backtrace: {
                let mut str = err.backtrace_str();
                str.truncate(2 * 1024);
                str
            },
        });

        match rst_json {
            Ok(serialized_error_json) => {
                // Code::Internal will be used by h2, if something goes wrong internally.
                // To distinguish from that, we use Code::Unknown here
                tonic::Status::with_details(
                    tonic::Code::Unknown,
                    err.message(),
                    serialized_error_json.into(),
                )
            }
            Err(error) => tonic::Status::unknown(error.to_string()),
        }
    }
}
