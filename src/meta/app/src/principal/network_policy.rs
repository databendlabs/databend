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

use std::fmt::Display;
use std::fmt::Formatter;

use anyerror::AnyError;
use chrono::DateTime;
use chrono::Utc;
use common_exception::ErrorCode;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct NetworkPolicy {
    pub name: String,
    pub allowed_ip_list: Vec<String>,
    pub blocked_ip_list: Vec<String>,
    pub comment: String,
    pub create_on: DateTime<Utc>,
    pub update_on: Option<DateTime<Utc>>,
}

/// Error when ser/de NetworkPolicy
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub struct NetworkPolicySerdeError {
    pub message: String,
    pub source: AnyError,
}

impl TryFrom<Vec<u8>> for NetworkPolicy {
    type Error = NetworkPolicySerdeError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        match serde_json::from_slice(&value) {
            Ok(network_policy) => Ok(network_policy),
            Err(serialize_error) => Err(NetworkPolicySerdeError {
                message: "Cannot deserialize NetworkPolicy from bytes".to_string(),
                source: AnyError::new(&serialize_error),
            }),
        }
    }
}

impl Display for NetworkPolicySerdeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} cause: {}", self.message, self.source)
    }
}

impl From<NetworkPolicySerdeError> for ErrorCode {
    fn from(e: NetworkPolicySerdeError) -> Self {
        ErrorCode::InvalidReply(e.to_string())
    }
}
