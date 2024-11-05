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

use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_sql::plans::CreateTablePlan;

#[async_trait::async_trait]
pub trait AttachTableHandler: Sync + Send {
    async fn build_attach_table_request(
        &self,
        storage_prefix: &str,
        plan: &CreateTablePlan,
    ) -> Result<CreateTableReq>;
}

pub struct AttachTableHandlerWrapper {
    handler: Box<dyn AttachTableHandler>,
}

impl AttachTableHandlerWrapper {
    pub fn new(handler: Box<dyn AttachTableHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn build_attach_table_request(
        &self,
        storage_prefix: &str,
        plan: &CreateTablePlan,
    ) -> Result<CreateTableReq> {
        self.handler
            .build_attach_table_request(storage_prefix, plan)
            .await
    }
}

pub fn get_attach_table_handler() -> Arc<AttachTableHandlerWrapper> {
    GlobalInstance::get()
}
