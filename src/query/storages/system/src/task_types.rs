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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    Suspended = 0,
    Started = 1,
}

impl Display for TaskStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskStatus::Suspended => write!(f, "Suspended"),
            TaskStatus::Started => write!(f, "Started"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskRunState {
    Scheduled = 0,
    Executing = 1,
    Succeeded = 2,
    Failed = 3,
    Cancelled = 4,
}

impl Display for TaskRunState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskRunState::Scheduled => write!(f, "SCHEDULED"),
            TaskRunState::Executing => write!(f, "EXECUTING"),
            TaskRunState::Succeeded => write!(f, "SUCCEEDED"),
            TaskRunState::Failed => write!(f, "FAILED"),
            TaskRunState::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskRecord {
    pub task_id: u64,
    pub task_name: String,
    pub query_text: String,
    pub condition_text: String,
    pub after: Vec<String>,
    pub comment: Option<String>,
    pub owner: String,
    pub schedule_options: Option<String>,
    pub warehouse: Option<String>,
    pub next_scheduled_at: Option<DateTime<Utc>>,
    pub suspend_task_after_num_failures: Option<i32>,
    pub error_integration: Option<String>,
    pub status: TaskStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_suspended_at: Option<DateTime<Utc>>,
    pub session_params: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskRunRecord {
    pub task_id: u64,
    pub task_name: String,
    pub query_text: String,
    pub condition_text: String,
    pub comment: Option<String>,
    pub owner: String,
    pub run_id: String,
    pub query_id: String,
    pub schedule_options: Option<String>,
    pub warehouse: Option<String>,
    pub attempt_number: i32,
    pub state: TaskRunState,
    pub scheduled_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error_code: i64,
    pub error_message: Option<String>,
    pub root_task_id: String,
    pub session_params: BTreeMap<String, String>,
}

pub fn format_task_schedule_options(
    interval: Option<i32>,
    milliseconds_interval: Option<u64>,
    cron: Option<String>,
    time_zone: Option<String>,
    schedule_type: i32,
) -> Result<String> {
    match schedule_type {
        0 => {
            if let Some(ms) = milliseconds_interval {
                Ok(format!(
                    "INTERVAL {} SECOND {} MILLISECOND",
                    interval.unwrap_or_default(),
                    ms
                ))
            } else {
                Ok(format!("INTERVAL {} SECOND", interval.unwrap_or_default()))
            }
        }
        1 => {
            let cron = cron.ok_or_else(|| {
                ErrorCode::IllegalCloudControlMessageFormat(
                    "cron expression schedule has null value",
                )
            })?;
            let mut res = format!("CRON {cron}");
            if let Some(time_zone) = time_zone {
                res.push_str(&format!(" TIMEZONE {time_zone}"));
            }
            Ok(res)
        }
        other => Err(ErrorCode::IllegalCloudControlMessageFormat(format!(
            "Illegal schedule type {other}"
        ))),
    }
}

fn parse_rfc3339(raw: &str, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| {
            ErrorCode::IllegalCloudControlMessageFormat(format!(
                "illegal {field} message {raw}, {e}"
            ))
        })
}

#[cfg(feature = "cloud-control")]
impl TryFrom<databend_common_cloud_control::pb::Task> for TaskRecord {
    type Error = ErrorCode;

    fn try_from(value: databend_common_cloud_control::pb::Task) -> Result<Self> {
        let status = match value.status {
            0 => TaskStatus::Suspended,
            1 => TaskStatus::Started,
            s => {
                return Err(ErrorCode::IllegalCloudControlMessageFormat(format!(
                    "Illegal status code {s}"
                )));
            }
        };

        Ok(TaskRecord {
            task_id: value.task_id,
            task_name: value.task_name,
            query_text: value.query_text,
            condition_text: value.when_condition.unwrap_or_default(),
            after: value.after.clone(),
            comment: value.comment,
            owner: value.owner,
            schedule_options: match value.schedule_options {
                Some(ref schedule) if value.after.is_empty() => Some(format_task_schedule_options(
                    schedule.interval,
                    schedule.milliseconds_interval,
                    schedule.cron.clone(),
                    schedule.time_zone.clone(),
                    schedule.schedule_type,
                )?),
                _ => None,
            },
            warehouse: value.warehouse_options.and_then(|opts| opts.warehouse),
            next_scheduled_at: value
                .next_scheduled_at
                .as_deref()
                .map(|s| parse_rfc3339(s, "next_scheduled_at"))
                .transpose()?,
            suspend_task_after_num_failures: value.suspend_task_after_num_failures,
            error_integration: value.error_integration,
            status,
            created_at: parse_rfc3339(&value.created_at, "created_at")?,
            updated_at: parse_rfc3339(&value.updated_at, "updated_at")?,
            last_suspended_at: value
                .last_suspended_at
                .as_deref()
                .map(|s| parse_rfc3339(s, "last_suspended_at"))
                .transpose()?,
            session_params: value.session_parameters,
        })
    }
}

#[cfg(feature = "cloud-control")]
impl TryFrom<databend_common_cloud_control::pb::TaskRun> for TaskRunRecord {
    type Error = ErrorCode;

    fn try_from(value: databend_common_cloud_control::pb::TaskRun) -> Result<Self> {
        let state = match value.state {
            0 => TaskRunState::Scheduled,
            1 => TaskRunState::Executing,
            2 => TaskRunState::Succeeded,
            3 => TaskRunState::Failed,
            4 => TaskRunState::Cancelled,
            s => {
                return Err(ErrorCode::IllegalCloudControlMessageFormat(format!(
                    "Illegal state code {s}"
                )));
            }
        };

        Ok(TaskRunRecord {
            task_id: value.task_id,
            task_name: value.task_name,
            query_text: value.query_text,
            condition_text: value.condition_text,
            comment: value.comment,
            owner: value.owner,
            run_id: value.run_id,
            query_id: value.query_id,
            schedule_options: match value.schedule_options {
                Some(ref schedule) if value.task_id.to_string() == value.root_task_id => {
                    Some(format_task_schedule_options(
                        schedule.interval,
                        schedule.milliseconds_interval,
                        schedule.cron.clone(),
                        schedule.time_zone.clone(),
                        schedule.schedule_type,
                    )?)
                }
                _ => None,
            },
            warehouse: value.warehouse_options.and_then(|opts| opts.warehouse),
            attempt_number: value.attempt_number,
            state,
            scheduled_at: parse_rfc3339(&value.scheduled_time, "scheduled_time")?,
            completed_at: value
                .completed_time
                .as_deref()
                .map(|s| parse_rfc3339(s, "completed_time"))
                .transpose()?,
            error_code: value.error_code,
            error_message: value.error_message,
            root_task_id: value.root_task_id,
            session_params: value.session_parameters,
        })
    }
}
