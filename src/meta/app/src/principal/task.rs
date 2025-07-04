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
use std::collections::HashSet;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;

pub const EMPTY_TASK_ID: u64 = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ScheduleType {
    IntervalType = 0,
    CronType = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Status {
    Suspended = 0,
    Started = 1,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum State {
    Scheduled = 0,
    Executing = 1,
    Succeeded = 2,
    Failed = 3,
    Cancelled = 4,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleOptions {
    pub interval: Option<i32>,
    pub cron: Option<String>,
    pub time_zone: Option<String>,
    pub schedule_type: ScheduleType,
    pub milliseconds_interval: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WarehouseOptions {
    pub warehouse: Option<String>,
    pub using_warehouse_size: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Task {
    pub task_id: u64,
    pub task_name: String,
    pub query_text: String,
    pub when_condition: Option<String>,
    pub after: Vec<String>,
    pub comment: Option<String>,
    // expired useless
    pub owner: String,
    pub owner_user: String,
    pub schedule_options: Option<ScheduleOptions>,
    pub warehouse_options: Option<WarehouseOptions>,
    pub next_scheduled_at: Option<DateTime<Utc>>,
    pub suspend_task_after_num_failures: Option<u64>,
    // TODO
    pub error_integration: Option<String>,
    pub status: Status,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_suspended_at: Option<DateTime<Utc>>,
    // TODO
    pub session_params: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskRun {
    pub task: Task,
    pub run_id: u64,
    pub attempt_number: i32,
    pub state: State,
    pub scheduled_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error_code: i64,
    pub error_message: Option<String>,
    // expired useless
    pub root_task_id: u64,
}

impl TaskRun {
    pub fn key(&self) -> String {
        format!("{}@{}", self.task.task_name, self.run_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AfterTaskInfo {
    pub afters: Arc<Vec<String>>,
}

pub struct AfterTaskState {
    waiting: HashSet<String>,
}

impl From<&Task> for AfterTaskInfo {
    fn from(value: &Task) -> Self {
        AfterTaskInfo {
            afters: Arc::new(value.after.clone()),
        }
    }
}

impl AfterTaskState {
    pub fn completed_task(&mut self, task_name: &str) -> bool {
        self.waiting.remove(task_name);
        self.waiting.is_empty()
    }
}

impl From<&AfterTaskInfo> for AfterTaskState {
    fn from(value: &AfterTaskInfo) -> Self {
        Self {
            waiting: HashSet::from_iter(value.afters.to_vec()),
        }
    }
}
