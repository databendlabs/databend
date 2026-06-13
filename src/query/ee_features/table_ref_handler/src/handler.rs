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
use databend_common_sql::plans::CreateTableBranchPlan;
use databend_common_sql::plans::CreateTableTagPlan;
use databend_common_sql::plans::DropTableBranchPlan;
use databend_common_sql::plans::DropTableTagPlan;

#[async_trait::async_trait]
pub trait TableRefHandler: Sync + Send {
    async fn do_create_table_branch(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &CreateTableBranchPlan,
    ) -> Result<()>;

    async fn do_create_table_tag(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &CreateTableTagPlan,
    ) -> Result<()>;

    async fn do_drop_table_branch(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DropTableBranchPlan,
    ) -> Result<()>;

    async fn do_drop_table_tag(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DropTableTagPlan,
    ) -> Result<()>;
}

pub struct TableRefHandlerWrapper {
    handler: Box<dyn TableRefHandler>,
}

impl TableRefHandlerWrapper {
    pub fn new(handler: Box<dyn TableRefHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn do_create_table_branch(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &CreateTableBranchPlan,
    ) -> Result<()> {
        self.handler.do_create_table_branch(ctx, plan).await
    }

    #[async_backtrace::framed]
    pub async fn do_create_table_tag(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &CreateTableTagPlan,
    ) -> Result<()> {
        self.handler.do_create_table_tag(ctx, plan).await
    }

    #[async_backtrace::framed]
    pub async fn do_drop_table_branch(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DropTableBranchPlan,
    ) -> Result<()> {
        self.handler.do_drop_table_branch(ctx, plan).await
    }

    #[async_backtrace::framed]
    pub async fn do_drop_table_tag(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DropTableTagPlan,
    ) -> Result<()> {
        self.handler.do_drop_table_tag(ctx, plan).await
    }
}

pub fn get_table_ref_handler() -> Arc<TableRefHandlerWrapper> {
    GlobalInstance::get()
}
