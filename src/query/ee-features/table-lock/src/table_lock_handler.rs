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

use common_base::base::GlobalInstance;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_license::license_manager::get_license_manager;
use common_meta_app::schema::TableInfo;

use crate::TableLockHeartbeat;

#[async_trait::async_trait]
pub trait TableLockHandler: Sync + Send {
    async fn try_lock(
        &self,
        ctx: Arc<dyn TableContext>,
        table_info: TableInfo,
    ) -> Result<TableLockHeartbeat>;
}

pub struct DummyTableLock {}

#[async_trait::async_trait]
impl TableLockHandler for DummyTableLock {
    #[async_backtrace::framed]
    async fn try_lock(
        &self,
        _ctx: Arc<dyn TableContext>,
        _table_info: TableInfo,
    ) -> Result<TableLockHeartbeat> {
        Ok(TableLockHeartbeat::default())
    }
}

pub struct TableLockHandlerWrapper {
    handler: Box<dyn TableLockHandler>,
}

impl TableLockHandlerWrapper {
    pub fn new(handler: Box<dyn TableLockHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn try_lock(
        &self,
        ctx: Arc<dyn TableContext>,
        table_info: TableInfo,
    ) -> Result<TableLockHeartbeat> {
        self.handler.try_lock(ctx, table_info).await
    }

    pub fn instance(ctx: Arc<dyn TableContext>) -> Arc<TableLockHandlerWrapper> {
        let enterprise_enabled = get_license_manager()
            .manager
            .check_enterprise_enabled(
                &ctx.get_settings(),
                ctx.get_tenant(),
                "table_lock".to_string(),
            )
            .is_ok();
        if enterprise_enabled {
            GlobalInstance::get()
        } else {
            Arc::new(TableLockHandlerWrapper::new(Box::new(DummyTableLock {})))
        }
    }
}
