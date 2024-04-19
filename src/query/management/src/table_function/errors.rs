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
use databend_common_meta_types::MetaError;

use crate::errors::TenantError;

#[derive(Clone, Debug, thiserror::Error)]
pub enum SequenceTableFunctionApiError {
    #[error("TenantError: '{0}'")]
    TenantError(#[from] TenantError),

    #[error("MetaService error: {meta_err}; {context}")]
    MetaError {
        meta_err: MetaError,
        context: String,
    },
}

impl From<MetaError> for SequenceTableFunctionApiError {
    fn from(meta_err: MetaError) -> Self {
        SequenceTableFunctionApiError::MetaError {
            meta_err,
            context: "".to_string(),
        }
    }
}

impl From<SequenceTableFunctionApiError> for ErrorCode {
    fn from(value: SequenceTableFunctionApiError) -> Self {
        match value {
            SequenceTableFunctionApiError::TenantError(e) => ErrorCode::from(e),
            SequenceTableFunctionApiError::MetaError { meta_err, context } => {
                ErrorCode::from(meta_err).add_message_back(context)
            }
        }
    }
}

impl SequenceTableFunctionApiError {
    pub fn append_context(self, context: impl Display) -> Self {
        match self {
            SequenceTableFunctionApiError::TenantError(e) => {
                SequenceTableFunctionApiError::TenantError(e.append_context(context))
            }
            SequenceTableFunctionApiError::MetaError {
                meta_err,
                context: old,
            } => SequenceTableFunctionApiError::MetaError {
                meta_err,
                context: format!("{}; {}", old, context),
            },
        }
    }
}
