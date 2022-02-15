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

use anyerror::AnyError;
use common_exception::ErrorCode;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

// represent network related errors
#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum MetaNetworkError {
    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),

    #[error("{0}")]
    GetNodeAddrError(String),

    #[error(transparent)]
    TLSConfigError(AnyError),

    #[error(transparent)]
    BadAddressFormat(AnyError),
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
        }
    }
}

pub type MetaNetworkResult<T> = std::result::Result<T, MetaNetworkError>;

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
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
}

impl From<std::net::AddrParseError> for MetaNetworkError {
    fn from(error: std::net::AddrParseError) -> Self {
        MetaNetworkError::BadAddressFormat(AnyError::new(&error))
    }
}
