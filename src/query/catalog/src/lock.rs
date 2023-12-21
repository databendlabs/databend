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

use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::LockKey;
use databend_common_meta_app::schema::LockType;
use databend_common_pipeline_core::LockGuard;

use crate::catalog::Catalog;
use crate::table_context::TableContext;

#[async_trait::async_trait]
pub trait Lock: Sync + Send {
    fn lock_type(&self) -> LockType;

    fn gen_lock_key(&self) -> LockKey;

    fn get_catalog(&self) -> &str;

    fn get_table_id(&self) -> u64;

    fn watch_delete_key(&self, revision: u64) -> String;

    async fn try_lock(&self, ctx: Arc<dyn TableContext>) -> Result<Option<LockGuard>>;
}

#[async_trait::async_trait]
pub trait LockExt: Lock {
    /// Return true if the table is locked.
    async fn check_lock(&self, catalog: Arc<dyn Catalog>) -> Result<bool> {
        let req = ListLockRevReq {
            lock_key: self.gen_lock_key(),
        };
        let reply = catalog.list_lock_revisions(req).await?;
        Ok(!reply.is_empty())
    }

    fn gen_create_lock_req(
        &self,
        user: String,
        node: String,
        query_id: String,
        expire_secs: u64,
    ) -> CreateLockRevReq {
        CreateLockRevReq {
            lock_key: self.gen_lock_key(),
            user,
            node,
            query_id,
            expire_secs,
        }
    }

    fn gen_list_lock_req(&self) -> ListLockRevReq {
        ListLockRevReq {
            lock_key: self.gen_lock_key(),
        }
    }

    fn gen_delete_lock_req(&self, revision: u64) -> DeleteLockRevReq {
        DeleteLockRevReq {
            lock_key: self.gen_lock_key(),
            revision,
        }
    }

    fn gen_extend_lock_req(
        &self,
        revision: u64,
        expire_secs: u64,
        acquire_lock: bool,
    ) -> ExtendLockRevReq {
        ExtendLockRevReq {
            lock_key: self.gen_lock_key(),
            revision,
            acquire_lock,
            expire_secs,
        }
    }
}

impl<T: ?Sized> LockExt for T where T: Lock {}
