// Copyright 2022 Datafuse Labs.
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

use crate::MetaHandshakeError;
use crate::MetaNetworkError;

/// Error raised by meta service client.
#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MetaClientError {
    #[error("meta-client dedicated runtime error: {0}")]
    ClientRuntimeError(AnyError),

    #[error("meta-client config error: {0}")]
    ConfigError(AnyError),

    #[error(transparent)]
    NetworkError(MetaNetworkError),

    #[error(transparent)]
    HandshakeError(MetaHandshakeError),
}

impl From<MetaClientError> for ErrorCode {
    fn from(e: MetaClientError) -> Self {
        match e {
            MetaClientError::ClientRuntimeError(rt_err) => {
                ErrorCode::MetaServiceError(rt_err.to_string())
            }
            MetaClientError::ConfigError(conf_err) => {
                ErrorCode::MetaServiceError(conf_err.to_string())
            }
            MetaClientError::NetworkError(net_err) => net_err.into(),
            MetaClientError::HandshakeError(handshake_err) => handshake_err.into(),
        }
    }
}
