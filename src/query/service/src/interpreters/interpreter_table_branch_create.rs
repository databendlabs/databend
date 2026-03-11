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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_sql::plans::CreateTableBranchPlan;
use databend_common_storages_fuse::TableContext;
use databend_enterprise_table_ref_handler::get_table_ref_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct CreateTableBranchInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateTableBranchPlan,
}

impl CreateTableBranchInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateTableBranchPlan) -> Result<Self> {
        Ok(CreateTableBranchInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateTableBranchInterpreter {
    fn name(&self) -> &str {
        "CreateTableBranchInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::TableRef)?;

        if !self
            .ctx
            .get_settings()
            .get_enable_experimental_table_ref()
            .unwrap_or_default()
        {
            return Err(ErrorCode::Unimplemented(
                "Table ref is an experimental feature, `set enable_experimental_table_ref=1` to use this feature",
            ));
        }

        let handler = get_table_ref_handler();
        handler
            .do_create_table_branch(self.ctx.clone(), &self.plan)
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
