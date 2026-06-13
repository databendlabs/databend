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
use databend_common_meta_app::principal::AutoIncrementKey;
/// Table logic error, unrelated to the backend service providing Table management, or dependent component.
#[derive(Clone, Debug, thiserror::Error)]
pub enum TableError {
    // NOTE: do not expose tenant in a for-user error message.
    #[error("Alter table with error {context}")]
    AlterTableError { tenant: String, context: String },
    #[error("Unknown table id {table_id}, {context}")]
    UnknownTableId {
        tenant: String,
        table_id: u64,
        context: String,
    },
}

impl From<TableError> for ErrorCode {
    fn from(value: TableError) -> Self {
        let s = value.to_string();
        match value {
            TableError::AlterTableError { .. } => ErrorCode::AlterTableError(s),
            TableError::UnknownTableId { .. } => ErrorCode::UnknownTableId(s),
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum MaskingPolicyError {
    #[error(
        "MASKING POLICY `{policy_name}` is still in use. Unset it from all tables before dropping."
    )]
    PolicyInUse { policy_name: String },
}

impl MaskingPolicyError {
    pub fn policy_in_use(policy_name: impl Into<String>) -> Self {
        Self::PolicyInUse {
            policy_name: policy_name.into(),
        }
    }
}

impl From<MaskingPolicyError> for ErrorCode {
    fn from(value: MaskingPolicyError) -> Self {
        let s = value.to_string();
        match value {
            MaskingPolicyError::PolicyInUse { .. } => ErrorCode::ConstraintError(s),
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum RowAccessPolicyError {
    #[error(
        "ROW ACCESS POLICY `{policy_name}` is still in use. Unset it from all tables before dropping."
    )]
    PolicyInUse { policy_name: String },
}

impl RowAccessPolicyError {
    pub fn policy_in_use(policy_name: impl Into<String>) -> Self {
        Self::PolicyInUse {
            policy_name: policy_name.into(),
        }
    }
}

impl From<RowAccessPolicyError> for ErrorCode {
    fn from(value: RowAccessPolicyError) -> Self {
        let s = value.to_string();
        match value {
            RowAccessPolicyError::PolicyInUse { .. } => ErrorCode::ConstraintError(s),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum AutoIncrementError {
    #[error("OutOfAutoIncrementRange: `{key}` while `{context}`")]
    OutOfAutoIncrementRange {
        key: AutoIncrementKey,
        context: String,
    },
}

impl From<AutoIncrementError> for ErrorCode {
    fn from(value: AutoIncrementError) -> Self {
        let s = value.to_string();
        match value {
            AutoIncrementError::OutOfAutoIncrementRange { .. } => ErrorCode::AutoIncrementError(s),
        }
    }
}
