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

use std::io;

use tonic::Status;

use crate::ConnectionError;
use crate::InvalidReply;
use crate::MetaHandshakeError;
use crate::MetaNetworkError;

/// Error raised by meta service client.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum MetaClientError {
    #[error(transparent)]
    NetworkError(#[from] MetaNetworkError),

    #[error(transparent)]
    HandshakeError(#[from] MetaHandshakeError),
}

impl MetaClientError {
    pub fn name(&self) -> &'static str {
        match self {
            MetaClientError::NetworkError(err) => err.name(),
            MetaClientError::HandshakeError(_) => "MetaHandshakeError",
        }
    }
}

impl From<Status> for MetaClientError {
    fn from(status: Status) -> Self {
        Self::NetworkError(status.into())
    }
}

impl From<ConnectionError> for MetaClientError {
    fn from(e: ConnectionError) -> Self {
        Self::NetworkError(e.into())
    }
}

impl From<MetaClientError> for io::Error {
    fn from(e: MetaClientError) -> Self {
        match e {
            MetaClientError::NetworkError(e) => e.into(),
            MetaClientError::HandshakeError(e) => {
                io::Error::new(io::ErrorKind::NotConnected, e.to_string())
            }
        }
    }
}

impl From<InvalidReply> for MetaClientError {
    fn from(e: InvalidReply) -> Self {
        Self::NetworkError(e.into())
    }
}
