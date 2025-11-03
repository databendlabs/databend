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
use databend_common_meta_app::app_error::AppErrorMessage;
use databend_common_meta_app::app_error::TxnRetryMaxTimes;
use databend_common_meta_app::principal::AutoIncrementKey;
use databend_common_meta_types::InvalidArgument;
use databend_common_meta_types::MetaError;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecurityPolicyKind {
    Masking,
    RowAccess,
}

impl std::fmt::Display for SecurityPolicyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecurityPolicyKind::Masking => write!(f, "MASKING POLICY"),
            SecurityPolicyKind::RowAccess => write!(f, "ROW ACCESS POLICY"),
        }
    }
}

/// Security policy related business error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum SecurityPolicyError {
    #[error(
        "{policy_kind} `{policy_name}` is still in use. Unset it from all tables before dropping."
    )]
    PolicyInUse {
        tenant: String,
        policy_kind: SecurityPolicyKind,
        policy_name: String,
    },
}

impl SecurityPolicyError {
    pub fn policy_in_use(
        tenant: impl Into<String>,
        policy_kind: SecurityPolicyKind,
        policy_name: impl Into<String>,
    ) -> Self {
        Self::PolicyInUse {
            tenant: tenant.into(),
            policy_kind,
            policy_name: policy_name.into(),
        }
    }
}

impl From<SecurityPolicyError> for ErrorCode {
    fn from(value: SecurityPolicyError) -> Self {
        let s = value.to_string();
        match value {
            SecurityPolicyError::PolicyInUse { .. } => ErrorCode::ConstraintError(s),
        }
    }
}

impl From<SecurityPolicyError> for InvalidArgument {
    fn from(value: SecurityPolicyError) -> Self {
        let msg = value.to_string();
        InvalidArgument::new(value, msg)
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum MaskingPolicyError {
    #[error(transparent)]
    TxnRetry(#[from] TxnRetryMaxTimes),
    #[error(transparent)]
    Meta(#[from] MetaError),
    #[error(transparent)]
    PolicyInUse(#[from] SecurityPolicyError),
}

impl From<MaskingPolicyError> for ErrorCode {
    fn from(value: MaskingPolicyError) -> Self {
        match value {
            MaskingPolicyError::TxnRetry(err) => ErrorCode::TxnRetryMaxTimes(err.message()),
            MaskingPolicyError::Meta(err) => ErrorCode::from(err),
            MaskingPolicyError::PolicyInUse(err) => err.into(),
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum RowAccessPolicyError {
    #[error(transparent)]
    TxnRetry(#[from] TxnRetryMaxTimes),
    #[error(transparent)]
    Meta(#[from] MetaError),
    #[error(transparent)]
    PolicyInUse(#[from] SecurityPolicyError),
}

impl From<RowAccessPolicyError> for ErrorCode {
    fn from(value: RowAccessPolicyError) -> Self {
        match value {
            RowAccessPolicyError::TxnRetry(err) => ErrorCode::TxnRetryMaxTimes(err.message()),
            RowAccessPolicyError::Meta(err) => ErrorCode::from(err),
            RowAccessPolicyError::PolicyInUse(err) => err.into(),
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
