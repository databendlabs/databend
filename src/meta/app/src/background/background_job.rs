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

use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;

use chrono::DateTime;
use chrono::Utc;
use cron::Schedule;
use databend_common_meta_types::SeqV;

use crate::background::BackgroundJobIdIdent;
use crate::background::BackgroundJobIdent;
use crate::background::BackgroundTaskType;
use crate::principal::UserIdentity;
use crate::tenant::Tenant;
use crate::tenant::ToTenant;

#[derive(Clone, Debug, Default, Eq, PartialEq, num_derive::FromPrimitive)]
pub enum BackgroundJobState {
    #[default]
    RUNNING = 0,
    FAILED = 1,
    SUSPENDED = 2,
}

impl Display for BackgroundJobState {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, num_derive::FromPrimitive)]
pub enum BackgroundJobType {
    #[default]
    ONESHOT = 0,
    INTERVAL = 1,
    CRON = 2,
}

impl Display for BackgroundJobType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct ManualTriggerParams {
    pub id: String,
    pub trigger: UserIdentity,
    pub triggered_at: DateTime<Utc>,
}

impl ManualTriggerParams {
    pub fn new(id: String, trigger: UserIdentity) -> Self {
        Self {
            id,
            trigger,
            triggered_at: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BackgroundJobParams {
    pub job_type: BackgroundJobType,
    pub scheduled_job_interval: std::time::Duration,
    pub scheduled_job_cron: String,
    pub scheduled_job_timezone: Option<chrono_tz::Tz>,

    pub manual_trigger_params: Option<ManualTriggerParams>,
}

impl BackgroundJobParams {
    pub fn new_one_shot_job() -> Self {
        Self {
            job_type: BackgroundJobType::ONESHOT,
            ..Default::default()
        }
    }

    pub fn new_interval_job(interval_seconds: std::time::Duration) -> Self {
        Self {
            job_type: BackgroundJobType::INTERVAL,
            scheduled_job_interval: interval_seconds,
            ..Default::default()
        }
    }

    pub fn new_cron_job(cron: String, timezone: Option<chrono_tz::Tz>) -> Self {
        Self {
            job_type: BackgroundJobType::CRON,
            scheduled_job_cron: cron,
            scheduled_job_timezone: timezone,
            ..Default::default()
        }
    }

    pub fn get_next_running_time(&self, last_run_at: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self.job_type {
            BackgroundJobType::ONESHOT => None,
            BackgroundJobType::INTERVAL => {
                Some(last_run_at + chrono::Duration::from_std(self.scheduled_job_interval).unwrap())
            }
            BackgroundJobType::CRON => {
                let schedule =
                    Schedule::from_str(self.scheduled_job_cron.as_str()).unwrap_or_else(|_| {
                        panic!("invalid cron expression {}", self.scheduled_job_cron)
                    });
                let upcoming = schedule.after(&last_run_at).next().unwrap();
                Some(upcoming.with_timezone(&Utc))
            }
        }
    }
}

impl Display for BackgroundJobParams {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self.job_type {
            BackgroundJobType::ONESHOT => write!(f, "ONESHOT"),
            BackgroundJobType::INTERVAL => {
                write!(f, "INTERVAL: {:?} seconds", self.scheduled_job_interval)
            }
            BackgroundJobType::CRON => write!(
                f,
                "CRON: {} {}",
                self.scheduled_job_cron,
                self.scheduled_job_timezone
                    .as_ref()
                    .unwrap_or(&chrono_tz::UTC)
            ),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BackgroundJobStatus {
    pub job_state: BackgroundJobState,
    pub last_task_id: Option<String>,
    pub last_task_run_at: Option<DateTime<Utc>>,

    pub next_task_scheduled_time: Option<DateTime<Utc>>,
}

impl Display for BackgroundJobStatus {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "job_state: {}, last_task_id: {}, last_task_run_at: {}, next_scheduled_time: {}",
            self.job_state,
            self.last_task_id.as_ref().unwrap_or(&"".to_string()),
            self.last_task_run_at
                .map(|x| x.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
                .unwrap_or("".to_string()),
            self.next_task_scheduled_time
                .map(|x| x.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
                .unwrap_or("".to_string()),
        )
    }
}

impl BackgroundJobStatus {
    pub fn new(params: &BackgroundJobParams) -> Self {
        Self {
            job_state: BackgroundJobState::RUNNING,
            last_task_id: None,
            last_task_run_at: None,
            next_task_scheduled_time: match params.job_type {
                BackgroundJobType::ONESHOT => None,
                BackgroundJobType::INTERVAL => Some(Utc::now()),
                BackgroundJobType::CRON => params.get_next_running_time(Utc::now()),
            },
        }
    }
}

// Info
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BackgroundJobInfo {
    pub job_params: Option<BackgroundJobParams>,
    pub job_status: Option<BackgroundJobStatus>,
    pub task_type: BackgroundTaskType,

    pub last_updated: Option<DateTime<Utc>>,
    pub message: String,

    // Audit
    pub creator: Option<UserIdentity>,
    pub created_at: DateTime<Utc>,
}

impl BackgroundJobInfo {
    pub fn new_compactor_job(job_params: BackgroundJobParams, creator: UserIdentity) -> Self {
        Self {
            job_status: Option::from(BackgroundJobStatus::new(&job_params)),
            job_params: Some(job_params),
            task_type: BackgroundTaskType::COMPACTION,
            last_updated: Some(Utc::now()),
            message: "".to_string(),
            creator: Some(creator),
            created_at: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateBackgroundJobReq {
    pub if_not_exists: bool,
    pub job_name: BackgroundJobIdent,
    pub job_info: BackgroundJobInfo,
}

impl Display for CreateBackgroundJobReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "create_background_job({}, {}, {:?}, {:?}, {}, {:?})",
            self.job_name.name(),
            self.job_info.task_type,
            self.job_info.job_params,
            self.job_info.job_status,
            self.job_info.message,
            self.job_info.last_updated
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateBackgroundJobReply {
    pub id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetBackgroundJobReq {
    pub name: BackgroundJobIdent,
}

impl Display for GetBackgroundJobReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "get_background_job({})", self.name.name())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetBackgroundJobReply {
    pub id_ident: BackgroundJobIdIdent,
    pub info: SeqV<BackgroundJobInfo>,
}

impl GetBackgroundJobReply {
    pub fn new(id_ident: BackgroundJobIdIdent, info: SeqV<BackgroundJobInfo>) -> Self {
        Self { id_ident, info }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateBackgroundJobStatusReq {
    pub job_name: BackgroundJobIdent,
    pub status: BackgroundJobStatus,
}

impl Display for UpdateBackgroundJobStatusReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "update_background_job_status({}, {})",
            self.job_name.name(),
            self.status
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateBackgroundJobParamsReq {
    pub job_name: BackgroundJobIdent,
    pub params: BackgroundJobParams,
}

impl Display for UpdateBackgroundJobParamsReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "update_background_job_params({}, {})",
            self.job_name.name(),
            self.params
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateBackgroundJobReq {
    pub job_name: BackgroundJobIdent,
    pub info: BackgroundJobInfo,
    pub job_id_seq: u64,
}

impl Display for UpdateBackgroundJobReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "update_background_job({}, {}, {:?}, {:?}, {}, {:?})",
            self.job_name.name(),
            self.info.task_type,
            self.info.job_params,
            self.info.job_status,
            self.info.message,
            self.info.last_updated
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateBackgroundJobReply {
    pub id_ident: BackgroundJobIdIdent,
}

impl UpdateBackgroundJobReply {
    pub fn new(id_ident: BackgroundJobIdIdent) -> Self {
        Self { id_ident }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteBackgroundJobReq {
    pub name: BackgroundJobIdent,
}

impl Display for DeleteBackgroundJobReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "delete_background_job({})", self.name.name())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteBackgroundJobReply {}
// list
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListBackgroundJobsReq {
    pub tenant: Tenant,
}

impl ListBackgroundJobsReq {
    pub fn new(tenant: impl ToTenant) -> Self {
        Self {
            tenant: tenant.to_tenant(),
        }
    }
}

impl Display for ListBackgroundJobsReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "list_background_job({})", self.tenant.tenant_name())
    }
}
