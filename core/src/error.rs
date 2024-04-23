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

use crate::response;

#[derive(Debug)]
pub enum Error {
    Parsing(String),
    BadArgument(String),
    Request(String),
    IO(String),
    // if you have not polled the next_page_uri for too long, the session will be expired, you'll get a 404
    // on accessing this next page uri.
    SessionTimeout(String),
    InvalidResponse(response::QueryError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Parsing(msg) => write!(f, "ParsingError: {msg}"),
            Error::BadArgument(msg) => write!(f, "BadArgument: {msg}"),
            Error::Request(msg) => write!(f, "RequestError: {msg}"),
            Error::IO(msg) => write!(f, "IOError: {msg}"),
            Error::SessionTimeout(msg) => write!(f, "SessionExpired: {msg}"),
            Error::InvalidResponse(e) => match &e.detail {
                Some(d) if !d.is_empty() => {
                    write!(f, "ResponseError with {}: {}\n{}", e.code, e.message, d)
                }
                _ => write!(f, "ResponseError with {}: {}", e.code, e.message),
            },
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

impl From<reqwest::header::InvalidHeaderValue> for Error {
    fn from(e: reqwest::header::InvalidHeaderValue) -> Self {
        Error::Parsing(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Parsing(e.to_string())
    }
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Request(e.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IO(e.to_string())
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Self {
        Error::Parsing(e.to_string())
    }
}
