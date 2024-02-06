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

/// Error related to tenant operation
#[derive(Clone, Debug, thiserror::Error)]
pub enum TenantError {
    #[error("tenant can not be empty string, while {context}")]
    CanNotBeEmpty { context: String },
}

impl From<TenantError> for ErrorCode {
    fn from(value: TenantError) -> Self {
        let s = value.to_string();
        match value {
            TenantError::CanNotBeEmpty { .. } => ErrorCode::TenantIsEmpty(s),
        }
    }
}
