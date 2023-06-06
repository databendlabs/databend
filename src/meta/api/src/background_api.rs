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

use common_meta_app::background::{BackgroundJobInfo, BackgroundTaskInfo, CreateBackgroundJobReply, CreateBackgroundJobReq, DeleteBackgroundJobReply, DeleteBackgroundJobReq, GetBackgroundJobReply, GetBackgroundJobReq, GetBackgroundTaskReply, GetBackgroundTaskReq, ListBackgroundJobsReq, ListBackgroundTasksReq, UpdateBackgroundJobReply, UpdateBackgroundJobReq, UpdateBackgroundTaskReply, UpdateBackgroundTaskReq};
use crate::kv_app_error::KVAppError;

#[async_trait::async_trait]
pub trait BackgroundApi: Send + Sync {
    async fn create_background_job(
        &self,
        req: CreateBackgroundJobReq,
    ) -> Result<CreateBackgroundJobReply, KVAppError>;

    async fn drop_background_job(
        &self,
        req: DeleteBackgroundJobReq,
    ) -> Result<DeleteBackgroundJobReply, KVAppError>;
    async fn update_background_job(
        &self,
        req: UpdateBackgroundJobReq,
    ) -> Result<UpdateBackgroundJobReply, KVAppError>;
    async fn get_background_job(
        &self,
        req: GetBackgroundJobReq,
    ) -> Result<GetBackgroundJobReply, KVAppError>;
    async fn list_background_jobs(
        &self,
        req: ListBackgroundJobsReq,
    ) -> Result<Vec<(u64, BackgroundJobInfo)>, KVAppError>;
    // Return a list of background tasks (task_id, BackgroundInfo)
    async fn list_background_tasks(&self, req: ListBackgroundTasksReq) -> Result<Vec<(u64, BackgroundTaskInfo)>, KVAppError>;

    async fn update_background_task(
        &self,
        req: UpdateBackgroundTaskReq,
    ) -> Result<UpdateBackgroundTaskReply, KVAppError>;

    async fn get_background_task(&self, req: GetBackgroundTaskReq) -> Result<GetBackgroundTaskReply, KVAppError>;
}
