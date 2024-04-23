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

use geozero::error::GeozeroError;

#[derive(Debug)]
pub struct ConvertError {
    target: &'static str,
    data: String,
    message: Option<String>,
}

impl ConvertError {
    pub fn new(target: &'static str, data: String) -> Self {
        ConvertError {
            target,
            data,
            message: None,
        }
    }

    pub fn with_message(mut self, message: String) -> Self {
        self.message = Some(message);
        self
    }
}

#[derive(Debug)]
pub enum Error {
    Parsing(String),
    Protocol(String),
    Transport(String),
    IO(String),
    BadArgument(String),
    InvalidResponse(String),
    Api(databend_client::error::Error),
    #[cfg(feature = "flight-sql")]
    Arrow(arrow_schema::ArrowError),
    Convert(ConvertError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Parsing(msg) => write!(f, "ParseError: {}", msg),
            Error::Protocol(msg) => write!(f, "ProtocolError: {}", msg),
            Error::Transport(msg) => write!(f, "TransportError: {}", msg),
            Error::IO(msg) => write!(f, "IOError: {}", msg),

            Error::BadArgument(msg) => write!(f, "BadArgument: {}", msg),
            Error::InvalidResponse(msg) => write!(f, "ResponseError: {}", msg),
            #[cfg(feature = "flight-sql")]
            Error::Arrow(e) => {
                let msg = match e {
                    arrow_schema::ArrowError::IoError(msg, _) => {
                        static START: &str = "Code:";
                        static END: &str = ". at";

                        let message_index = msg.find(START).unwrap_or(0);
                        let message_end_index = msg.find(END).unwrap_or(msg.len());
                        let message = &msg[message_index..message_end_index];
                        message.replace("\\n", "\n")
                    }
                    other => format!("{}", other),
                };
                write!(f, "ArrowError: {}", msg)
            }
            Error::Convert(e) => write!(
                f,
                "ConvertError: cannot convert {} to {}: {:?}",
                e.data, e.target, e.message
            ),
            Error::Api(e) => write!(f, "APIError: {}", e),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T, E = Error> = core::result::Result<T, E>;

impl From<url::ParseError> for Error {
    fn from(e: url::ParseError) -> Self {
        Error::Parsing(e.to_string())
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(e: std::num::ParseIntError) -> Self {
        Error::Parsing(e.to_string())
    }
}

impl From<std::str::ParseBoolError> for Error {
    fn from(e: std::str::ParseBoolError) -> Self {
        Error::Parsing(e.to_string())
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(e: std::num::ParseFloatError) -> Self {
        Error::Parsing(e.to_string())
    }
}

impl From<chrono::ParseError> for Error {
    fn from(e: chrono::ParseError) -> Self {
        Error::Parsing(e.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IO(e.to_string())
    }
}

impl From<glob::GlobError> for Error {
    fn from(e: glob::GlobError) -> Self {
        Error::IO(e.to_string())
    }
}

impl From<glob::PatternError> for Error {
    fn from(e: glob::PatternError) -> Self {
        Error::Parsing(e.to_string())
    }
}

#[cfg(feature = "flight-sql")]
impl From<tonic::Status> for Error {
    fn from(e: tonic::Status) -> Self {
        Error::Protocol(e.to_string())
    }
}

#[cfg(feature = "flight-sql")]
impl From<tonic::transport::Error> for Error {
    fn from(e: tonic::transport::Error) -> Self {
        Error::Transport(e.to_string())
    }
}

#[cfg(feature = "flight-sql")]
impl From<arrow_schema::ArrowError> for Error {
    fn from(e: arrow_schema::ArrowError) -> Self {
        Error::Arrow(e)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Self {
        Error::Parsing(e.to_string())
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(e: std::string::FromUtf8Error) -> Self {
        Error::Parsing(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Parsing(e.to_string())
    }
}

impl From<hex::FromHexError> for Error {
    fn from(e: hex::FromHexError) -> Self {
        Error::Parsing(e.to_string())
    }
}

impl From<databend_client::error::Error> for Error {
    fn from(e: databend_client::error::Error) -> Self {
        Error::Api(e)
    }
}

impl From<ConvertError> for Error {
    fn from(e: ConvertError) -> Self {
        Error::Convert(e)
    }
}

impl From<GeozeroError> for Error {
    fn from(e: GeozeroError) -> Self {
        Error::Parsing(e.to_string())
    }
}
