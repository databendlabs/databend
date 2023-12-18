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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_sql::plans::CreateStreamPlan;
use databend_common_sql::plans::DropStreamPlan;

#[async_trait::async_trait]
pub trait StreamHandler: Sync + Send {
    async fn do_create_stream(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &CreateStreamPlan,
    ) -> Result<CreateTableReply>;

    async fn do_drop_stream(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DropStreamPlan,
    ) -> Result<DropTableReply>;
}

pub struct StreamHandlerWrapper {
    handler: Box<dyn StreamHandler>,
}

impl StreamHandlerWrapper {
    pub fn new(handler: Box<dyn StreamHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn do_create_stream(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &CreateStreamPlan,
    ) -> Result<CreateTableReply> {
        self.handler.do_create_stream(ctx, plan).await
    }

    #[async_backtrace::framed]
    pub async fn do_drop_stream(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DropStreamPlan,
    ) -> Result<DropTableReply> {
        self.handler.do_drop_stream(ctx, plan).await
    }
}

pub fn get_stream_handler() -> Arc<StreamHandlerWrapper> {
    GlobalInstance::get()
}
