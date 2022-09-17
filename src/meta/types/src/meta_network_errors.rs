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

use std::fmt::Display;

use anyerror::AnyError;
use common_exception::ErrorCode;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use tonic::Code;

// represent network related errors
#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MetaNetworkError {
    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),

    #[error("{0}")]
    GetNodeAddrError(String),

    #[error("{0}")]
    DnsParseError(String),

    #[error(transparent)]
    TLSConfigError(AnyError),

    #[error(transparent)]
    BadAddressFormat(AnyError),

    #[error(transparent)]
    InvalidArgument(#[from] InvalidArgument),

    #[error(transparent)]
    InvalidReply(#[from] InvalidReply),
}

impl MetaNetworkError {
    pub fn add_context(self, context: impl Display) -> Self {
        match self {
            Self::ConnectionError(e) => e.add_context(context).into(),
            Self::GetNodeAddrError(e) => Self::GetNodeAddrError(format!("{}: {}", e, context)),
            Self::DnsParseError(e) => Self::DnsParseError(format!("{}: {}", e, context)),
            Self::TLSConfigError(e) => Self::TLSConfigError(e.add_context(|| context)),
            Self::BadAddressFormat(e) => Self::BadAddressFormat(e.add_context(|| context)),
            Self::InvalidArgument(e) => e.add_context(context).into(),
            Self::InvalidReply(e) => e.add_context(context).into(),
        }
    }
}

impl From<MetaNetworkError> for ErrorCode {
    fn from(net_err: MetaNetworkError) -> Self {
        match net_err {
            MetaNetworkError::BadAddressFormat(any_err) => {
                ErrorCode::BadAddressFormat(any_err.to_string())
            }
            MetaNetworkError::ConnectionError(any_err) => {
                ErrorCode::CannotConnectNode(any_err.to_string())
            }
            MetaNetworkError::GetNodeAddrError(_) => {
                ErrorCode::MetaServiceError(net_err.to_string())
            }
            MetaNetworkError::TLSConfigError(any_err) => {
                ErrorCode::TLSConfigurationFailure(any_err.to_string())
            }
            MetaNetworkError::DnsParseError(_) => ErrorCode::DnsParseError(net_err.to_string()),
            MetaNetworkError::InvalidArgument(inv_arg) => {
                ErrorCode::InvalidArgument(inv_arg.to_string())
            }
            MetaNetworkError::InvalidReply(inv_reply) => {
                ErrorCode::InvalidReply(inv_reply.to_string())
            }
        }
    }
}

pub type MetaNetworkResult<T> = std::result::Result<T, MetaNetworkError>;

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[error("ConnectionError: {msg} source: {source}")]
pub struct ConnectionError {
    msg: String,
    #[source]
    source: AnyError,
}

impl ConnectionError {
    pub fn new(source: impl std::error::Error + 'static, msg: impl Into<String>) -> Self {
        Self {
            msg: msg.into(),
            source: AnyError::new(&source),
        }
    }
    pub fn add_context(mut self, context: impl Display) -> Self {
        self.msg = format!("{}: {}", self.msg, context);
        self
    }
}

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[error("InvalidArgument: {msg} source: {source}")]
pub struct InvalidArgument {
    msg: String,
    #[source]
    source: AnyError,
}

impl InvalidArgument {
    pub fn new(source: impl std::error::Error + 'static, msg: impl Into<String>) -> Self {
        Self {
            msg: msg.into(),
            source: AnyError::new(&source),
        }
    }

    pub fn add_context(mut self, context: impl Display) -> Self {
        self.msg = format!("{}: {}", self.msg, context);
        self
    }
}

#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[error("InvalidReply: {msg} source: {source}")]
pub struct InvalidReply {
    msg: String,
    #[source]
    source: AnyError,
}

impl InvalidReply {
    pub fn new(msg: impl Display, source: &(impl std::error::Error + 'static)) -> Self {
        Self {
            msg: msg.to_string(),
            source: AnyError::new(source),
        }
    }

    pub fn add_context(mut self, context: impl Display) -> Self {
        self.msg = format!("{}: {}", self.msg, context);
        self
    }
}

impl From<std::net::AddrParseError> for MetaNetworkError {
    fn from(error: std::net::AddrParseError) -> Self {
        MetaNetworkError::BadAddressFormat(AnyError::new(&error))
    }
}

impl From<tonic::Status> for MetaNetworkError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            Code::InvalidArgument => {
                MetaNetworkError::InvalidArgument(InvalidArgument::new(status, ""))
            }
            // Code::Ok => {}
            // Code::Cancelled => {}
            // Code::Unknown => {}
            // Code::DeadlineExceeded => {}
            // Code::NotFound => {}
            // Code::AlreadyExists => {}
            // Code::PermissionDenied => {}
            // Code::ResourceExhausted => {}
            // Code::FailedPrecondition => {}
            // Code::Aborted => {}
            // Code::OutOfRange => {}
            // Code::Unimplemented => {}
            // Code::Internal => {}
            // Code::Unavailable => {}
            // Code::DataLoss => {}
            // Code::Unauthenticated => {}
            _ => MetaNetworkError::ConnectionError(ConnectionError::new(status, "")),
        }
    }
}
