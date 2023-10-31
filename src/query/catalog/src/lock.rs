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

use std::sync::Arc;

use chrono::Utc;
use common_exception::Result;
use common_meta_app::schema::CreateLockRevReq;
use common_meta_app::schema::DeleteLockRevReq;
use common_meta_app::schema::ExtendLockRevReq;
use common_meta_app::schema::ListLockRevReq;
use common_meta_app::schema::LockLevel;
use common_pipeline_core::LockGuard;

use crate::catalog::Catalog;
use crate::table_context::TableContext;

#[async_trait::async_trait]
pub trait Lock: Sync + Send {
    fn lock_level(&self) -> LockLevel;

    fn get_catalog(&self) -> &str;

    fn get_expire_secs(&self) -> u64;

    fn get_table_id(&self) -> u64;

    fn get_user(&self) -> String;

    fn get_node(&self) -> String;

    fn get_session_id(&self) -> String;

    fn watch_delete_key(&self, revision: u64) -> String;

    async fn try_lock(&self, ctx: Arc<dyn TableContext>) -> Result<Option<LockGuard>>;
}

#[async_trait::async_trait]
pub trait LockExt: Lock {
    /// Return true if the table is locked.
    async fn check_lock(&self, catalog: Arc<dyn Catalog>) -> Result<bool> {
        let req = ListLockRevReq {
            table_id: self.get_table_id(),
            level: self.lock_level(),
        };
        let reply = catalog.list_lock_revisions(req).await?;
        Ok(!reply.is_empty())
    }

    fn gen_create_lock_req(&self) -> CreateLockRevReq {
        CreateLockRevReq {
            table_id: self.get_table_id(),
            level: self.lock_level(),
            user: self.get_user(),
            node: self.get_node(),
            session_id: self.get_session_id(),
            expire_at: Utc::now().timestamp() as u64 + self.get_expire_secs(),
        }
    }

    fn gen_list_lock_req(&self) -> ListLockRevReq {
        ListLockRevReq {
            table_id: self.get_table_id(),
            level: self.lock_level(),
        }
    }

    fn gen_delete_lock_req(&self, revision: u64) -> DeleteLockRevReq {
        DeleteLockRevReq {
            table_id: self.get_table_id(),
            level: self.lock_level(),
            revision,
        }
    }

    fn gen_extend_lock_req(&self, revision: u64, acquire_lock: bool) -> ExtendLockRevReq {
        ExtendLockRevReq {
            table_id: self.get_table_id(),
            level: self.lock_level(),
            revision,
            acquire_lock,
            expire_at: Utc::now().timestamp() as u64 + self.get_expire_secs(),
        }
    }
}

impl<T: ?Sized> LockExt for T where T: Lock {}
