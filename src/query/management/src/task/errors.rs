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
use databend_common_proto_conv::Incompatible;

use crate::errors::TenantError;

/// Task logic error, unrelated to the backend service providing Task management, or dependent component.
#[derive(Clone, Debug, thiserror::Error)]
pub enum TaskError {
    // NOTE: do not expose tenant in a for-user error message.
    #[error("Task not found: '{name}'; {context}")]
    NotFound {
        tenant: String,
        name: String,
        context: String,
    },

    // NOTE: do not expose tenant in a for-user error message.
    #[error("Task already exists: '{name}'; {reason}")]
    Exists {
        tenant: String,
        name: String,
        reason: String,
    },

    // NOTE: do not expose tenant in a for-user error message.
    #[error("Task timezone invalid: '{name}'; {reason}")]
    InvalidTimezone {
        tenant: String,
        name: String,
        reason: String,
    },

    // NOTE: do not expose tenant in a for-user error message.
    #[error("Task cron invalid: '{name}'; {reason}")]
    InvalidCron {
        tenant: String,
        name: String,
        reason: String,
    },

    #[error("Task cannot have both `SCHEDULE` and `AFTER`: '{name}'")]
    ScheduleAndAfterConflict { tenant: String, name: String },
}

impl From<TaskError> for ErrorCode {
    fn from(value: TaskError) -> Self {
        let s = value.to_string();
        match value {
            TaskError::NotFound { .. } => ErrorCode::UnknownTask(s),
            TaskError::Exists { .. } => ErrorCode::TaskAlreadyExists(s),
            TaskError::InvalidTimezone { .. } => ErrorCode::TaskAlreadyExists(s),
            TaskError::InvalidCron { .. } => ErrorCode::TaskCronInvalid(s),
            TaskError::ScheduleAndAfterConflict { .. } => {
                ErrorCode::TaskScheduleAndAfterConflict(s)
            }
        }
    }
}

/// The error occurred during accessing API providing Task management.
#[derive(Clone, Debug, thiserror::Error)]
pub enum TaskApiError {
    #[error("TenantError: '{0}'")]
    TenantError(#[from] TenantError),

    #[error("MetaService error: {meta_err}; {context}")]
    MetaError {
        meta_err: MetaError,
        context: String,
    },

    #[error("Incompatible error: {inner}")]
    Incompatible { inner: Incompatible },

    #[error("There are simultaneous update to task: {task_name} afters: {after}")]
    SimultaneousUpdateTaskAfter { task_name: String, after: String },
}

impl From<MetaError> for TaskApiError {
    fn from(meta_err: MetaError) -> Self {
        TaskApiError::MetaError {
            meta_err,
            context: "".to_string(),
        }
    }
}

impl From<Incompatible> for TaskApiError {
    fn from(value: Incompatible) -> Self {
        TaskApiError::Incompatible { inner: value }
    }
}

impl From<TaskApiError> for ErrorCode {
    fn from(value: TaskApiError) -> Self {
        match value {
            TaskApiError::TenantError(e) => ErrorCode::from(e),
            TaskApiError::MetaError { meta_err, context } => {
                ErrorCode::from(meta_err).add_message_back(context)
            }
            TaskApiError::Incompatible { inner } => ErrorCode::from_std_error(inner),
            TaskApiError::SimultaneousUpdateTaskAfter { .. } => {
                ErrorCode::from_string(value.to_string())
            }
        }
    }
}
