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

mod background_task;
mod background_job;

pub use background_task::BackgroundTaskIdent;
pub use background_task::BackgroundTaskInfo;
pub use background_task::CompactionStats;
pub use background_task::VacuumStats;
pub use background_task::BackgroundTaskType;
pub use background_task::BackgroundTaskState;
pub use background_task::BackgroundTaskId;

pub use background_job::BackgroundJobIdent;
pub use background_job::BackgroundJobInfo;
pub use background_job::BackgroundJobId;
pub use background_job::BackgroundJobType;
pub use background_job::BackgroundJobState;

pub use background_task::GetBackgroundTaskReq;
pub use background_task::GetBackgroundTaskReply;
pub use background_task::UpdateBackgroundTaskReply;
pub use background_task::UpdateBackgroundTaskReq;
pub use background_task::ListBackgroundTasksReq;

pub use background_job::GetBackgroundJobReq;
pub use background_job::GetBackgroundJobReply;
pub use background_job::CreateBackgroundJobReq;
pub use background_job::CreateBackgroundJobReply;
pub use background_job::UpdateBackgroundJobReply;
pub use background_job::UpdateBackgroundJobReq;
pub use background_job::ListBackgroundJobsReq;
pub use background_job::DeleteBackgroundJobReq;
pub use background_job::DeleteBackgroundJobReply;
