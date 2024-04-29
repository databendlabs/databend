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

use arrow_array::RecordBatch;
use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::tenant::Tenant;

#[async_trait::async_trait]
pub trait BackgroundServiceHandler: Sync + Send {
    async fn execute_sql(&self, sql: String) -> Result<Option<RecordBatch>>;

    async fn execute_scheduled_job(
        &self,
        tenant: Tenant,
        user: UserIdentity,
        name: String,
    ) -> Result<()>;
    async fn start(&self) -> Result<()>;
}

pub struct BackgroundServiceHandlerWrapper {
    pub handler: Box<dyn BackgroundServiceHandler>,
}

impl BackgroundServiceHandlerWrapper {
    pub fn new(handler: Box<dyn BackgroundServiceHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn execute_sql(&self, sql: String) -> Result<Option<RecordBatch>> {
        self.handler.execute_sql(sql).await
    }

    #[async_backtrace::framed]
    pub async fn start(&self) -> Result<()> {
        self.handler.start().await
    }
    #[async_backtrace::framed]
    pub async fn execute_scheduled_job(
        &self,
        tenant: Tenant,
        user: UserIdentity,
        name: String,
    ) -> Result<()> {
        self.handler.execute_scheduled_job(tenant, user, name).await
    }
}

pub fn get_background_service_handler() -> Arc<BackgroundServiceHandlerWrapper> {
    GlobalInstance::get()
}
