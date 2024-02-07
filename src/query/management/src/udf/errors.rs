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

use databend_common_exception::ErrorCode;
use databend_common_meta_types::MetaError;

use crate::errors::TenantError;

/// UDF logic error, unrelated to the backend service providing UDF management, or dependent component.
#[derive(Clone, Debug, thiserror::Error)]
pub enum UdfError {
    #[error("UDF not found: '{tenant}/{name}'")]
    NotFound { tenant: String, name: String },
}

impl From<UdfError> for ErrorCode {
    fn from(value: UdfError) -> Self {
        let s = value.to_string();
        match value {
            UdfError::NotFound { .. } => ErrorCode::UnknownUDF(s),
        }
    }
}

/// The error occurred during accessing API providing UDF management.
#[derive(Clone, Debug, thiserror::Error)]
pub enum UdfApiError {
    #[error("TenantError: '{0}'")]
    TenantError(#[from] TenantError),

    #[error("MetaService error: {0}")]
    MetaError(#[from] MetaError),
}

impl From<UdfApiError> for ErrorCode {
    fn from(value: UdfApiError) -> Self {
        match value {
            UdfApiError::TenantError(e) => ErrorCode::from(e),
            UdfApiError::MetaError(meta_err) => ErrorCode::from(meta_err),
        }
    }
}
