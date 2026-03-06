// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::CreateTableBranchPlan;
use databend_common_sql::plans::CreateTableTagPlan;
use databend_common_sql::plans::DropTableBranchPlan;
use databend_common_sql::plans::DropTableTagPlan;
use databend_enterprise_table_ref_handler::TableRefHandler;
use databend_enterprise_table_ref_handler::TableRefHandlerWrapper;
use databend_meta_client::types::MatchSeq;
use databend_storages_common_table_meta::meta::TableSnapshot;

pub struct RealTableRefHandler {}

const LEGACY_TABLE_REF_HANDLER_MESSAGE: &str = "Legacy experimental table refs were removed; binder should reject CREATE/DROP BRANCH|TAG before execution";

#[async_trait::async_trait]
impl TableRefHandler for RealTableRefHandler {
    #[async_backtrace::framed]
    async fn do_create_table_branch(
        &self,
        _ctx: Arc<dyn TableContext>,
        _plan: &CreateTableBranchPlan,
    ) -> Result<()> {
        // Keep this placeholder for the upcoming table-ref redesign. The legacy
        // implementation has been removed, so reaching this handler is unexpected.
        Err(ErrorCode::Unimplemented(LEGACY_TABLE_REF_HANDLER_MESSAGE))
    }

    #[async_backtrace::framed]
    async fn do_create_table_tag(
        &self,
        _ctx: Arc<dyn TableContext>,
        _plan: &CreateTableTagPlan,
    ) -> Result<()> {
        Err(ErrorCode::Unimplemented(LEGACY_TABLE_REF_HANDLER_MESSAGE))
    }

    #[async_backtrace::framed]
    async fn do_drop_table_branch(
        &self,
        _ctx: Arc<dyn TableContext>,
        _plan: &DropTableBranchPlan,
    ) -> Result<()> {
        Err(ErrorCode::Unimplemented(LEGACY_TABLE_REF_HANDLER_MESSAGE))
    }

    #[async_backtrace::framed]
    async fn do_drop_table_tag(
        &self,
        _ctx: Arc<dyn TableContext>,
        _plan: &DropTableTagPlan,
    ) -> Result<()> {
        Err(ErrorCode::Unimplemented(LEGACY_TABLE_REF_HANDLER_MESSAGE))
    }
}

impl RealTableRefHandler {
    pub fn init() -> Result<()> {
        let handler = RealTableRefHandler {};
        let wrapper = TableRefHandlerWrapper::new(Box::new(handler));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }
}
