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
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;

use crate::background::task_creator::BackgroundTaskCreator;
use crate::background::BackgroundJobIdent;
use crate::background::BackgroundTaskIdent;
use crate::background::ManualTriggerParams;
use crate::schema::TableStatistics;
use crate::tenant::Tenant;
use crate::tenant::ToTenant;

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    num_derive::FromPrimitive,
)]
pub enum BackgroundTaskState {
    #[default]
    STARTED = 0,
    DONE = 1,
    FAILED = 2,
}

impl Display for BackgroundTaskState {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

// BackgroundTaskType
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    num_derive::FromPrimitive,
)]
pub enum BackgroundTaskType {
    #[default]
    COMPACTION = 0,
    VACUUM = 1,
}

impl Display for BackgroundTaskType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct CompactionStats {
    pub db_id: u64,
    pub table_id: u64,
    pub before_compaction_stats: Option<TableStatistics>,
    pub after_compaction_stats: Option<TableStatistics>,
    pub total_compaction_time: Option<Duration>,
}

impl Display for CompactionStats {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "db_id: {}, table_id: {}, before_compaction_stats: {:?}, after_compaction_stats: {:?}, total_compaction_time: {:?}",
            self.db_id,
            self.table_id,
            self.before_compaction_stats,
            self.after_compaction_stats,
            self.total_compaction_time,
        )
    }
}

// TODO(zhihanz) provide detailed vacuum stats
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct VacuumStats {}

impl Display for VacuumStats {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "vacuum stats")
    }
}

// Serde is required by `ListBackgroundTasksResponse.task_infos`
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct BackgroundTaskInfo {
    pub last_updated: Option<DateTime<Utc>>,
    pub task_type: BackgroundTaskType,
    pub task_state: BackgroundTaskState,
    pub message: String,
    pub compaction_task_stats: Option<CompactionStats>,
    pub vacuum_stats: Option<VacuumStats>,

    pub manual_trigger: Option<ManualTriggerParams>,
    pub creator: Option<BackgroundTaskCreator>,
    pub created_at: DateTime<Utc>,
}

impl BackgroundTaskInfo {
    pub fn new_compaction_task(
        job_ident: BackgroundJobIdent,
        db_id: u64,
        tb_id: u64,
        tb_stats: TableStatistics,
        manual_trigger: Option<ManualTriggerParams>,
        message: String,
    ) -> Self {
        let now = Utc::now();
        Self {
            last_updated: Some(now),
            task_type: BackgroundTaskType::COMPACTION,
            task_state: BackgroundTaskState::STARTED,
            message,
            compaction_task_stats: Some(CompactionStats {
                db_id,
                table_id: tb_id,
                before_compaction_stats: Some(tb_stats),
                after_compaction_stats: None,
                total_compaction_time: None,
            }),
            vacuum_stats: None,
            manual_trigger,
            creator: Some(job_ident.into()),
            created_at: now,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateBackgroundTaskReq {
    pub task_name: BackgroundTaskIdent,
    pub task_info: BackgroundTaskInfo,
    pub expire_at: u64,
}

impl Display for UpdateBackgroundTaskReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "update_background_task({:?}, {}, {}, {}, {:?})",
            self.task_name,
            self.task_info.task_type,
            self.task_info.task_state,
            self.task_info.message,
            self.task_info.last_updated
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateBackgroundTaskReply {
    pub last_updated: DateTime<Utc>,
    pub expire_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetBackgroundTaskReq {
    pub name: BackgroundTaskIdent,
}

impl Display for GetBackgroundTaskReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "get_background_task({:?})", self.name)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetBackgroundTaskReply {
    pub task_info: Option<BackgroundTaskInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListBackgroundTasksReq {
    pub tenant: Tenant,
}

impl Display for ListBackgroundTasksReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "list_background_tasks({})", self.tenant.tenant_name())
    }
}

impl ListBackgroundTasksReq {
    pub fn new(tenant: impl ToTenant) -> ListBackgroundTasksReq {
        ListBackgroundTasksReq {
            tenant: tenant.to_tenant(),
        }
    }
}
