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

use std::any::Any;
use std::sync::Arc;

use common_exception::Result;
use common_meta_app::schema::LockLevel;
use common_pipeline_core::LockGuard;

use crate::catalog::Catalog;
use crate::table_context::TableContext;

#[async_trait::async_trait]
pub trait LockApi: Sync + Send {
    fn level(&self) -> LockLevel;

    fn catalog(&self) -> &str;

    fn get_expire_secs(&self) -> u64;

    fn table_id(&self) -> u64;

    fn watch_delete_key(&self, revision: u64) -> String;

    fn create_table_lock_req(&self) -> Box<dyn LockRequest>;

    fn extend_table_lock_req(&self, revision: u64, acquire_lock: bool) -> Box<dyn LockRequest>;

    fn delete_table_lock_req(&self, revision: u64) -> Box<dyn LockRequest>;

    fn list_table_lock_req(&self) -> Box<dyn LockRequest>;

    async fn try_lock(&self, ctx: Arc<dyn TableContext>) -> Result<Option<LockGuard>>;

    /// Return true if the table is locked.
    async fn check_lock(&self, catalog: Arc<dyn Catalog>) -> Result<bool> {
        let list_table_lock_req = self.list_table_lock_req();
        let reply = catalog.list_lock_revisions(list_table_lock_req).await?;
        Ok(!reply.is_empty())
    }
}

pub trait LockRequest: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn clone_self(&self) -> Box<dyn LockRequest>;
}

impl Clone for Box<dyn LockRequest> {
    fn clone(&self) -> Self {
        self.clone_self()
    }
}
