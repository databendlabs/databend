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

use databend_common_exception::ErrorCode;
use databend_meta_types::MetaError;

use crate::errors::TenantError;

/// UDF logic error, unrelated to the backend service providing UDF management, or dependent component.
#[derive(Clone, Debug, thiserror::Error)]
pub enum UdfError {
    // NOTE: do not expose tenant in a for-user error message.
    #[error("UDF not found: '{name}'; {context}")]
    NotFound {
        tenant: String,
        name: String,
        context: String,
    },

    // NOTE: do not expose tenant in a for-user error message.
    #[error("UDF already exists: '{name}'; {reason}")]
    Exists {
        tenant: String,
        name: String,
        reason: String,
    },
}

impl From<UdfError> for ErrorCode {
    fn from(value: UdfError) -> Self {
        let s = value.to_string();
        match value {
            UdfError::NotFound { .. } => ErrorCode::UnknownUDF(s),
            UdfError::Exists { .. } => ErrorCode::UdfAlreadyExists(s),
        }
    }
}

/// The error occurred during accessing API providing UDF management.
#[derive(Clone, Debug, thiserror::Error)]
pub enum UdfApiError {
    #[error("TenantError: '{0}'")]
    TenantError(#[from] TenantError),

    #[error("MetaService error: {meta_err}; {context}")]
    MetaError {
        meta_err: MetaError,
        context: String,
    },
}

impl From<MetaError> for UdfApiError {
    fn from(meta_err: MetaError) -> Self {
        UdfApiError::MetaError {
            meta_err,
            context: "".to_string(),
        }
    }
}

impl From<UdfApiError> for ErrorCode {
    fn from(value: UdfApiError) -> Self {
        match value {
            UdfApiError::TenantError(e) => ErrorCode::from(e),
            UdfApiError::MetaError { meta_err, context } => {
                ErrorCode::MetaServiceError(meta_err.to_string()).add_message_back(context)
            }
        }
    }
}

impl UdfApiError {
    pub fn append_context(self, context: impl Display) -> Self {
        match self {
            UdfApiError::TenantError(e) => UdfApiError::TenantError(e.append_context(context)),
            UdfApiError::MetaError {
                meta_err,
                context: old,
            } => UdfApiError::MetaError {
                meta_err,
                context: format!("{}; {}", old, context),
            },
        }
    }
}
