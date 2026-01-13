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
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_sql::plans::DropTableRefPlan;
use databend_common_storages_fuse::TableContext;
use databend_enterprise_table_ref_handler::get_table_ref_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct DropTableRefInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropTableRefPlan,
}

impl DropTableRefInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropTableRefPlan) -> Result<Self> {
        Ok(DropTableRefInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropTableRefInterpreter {
    fn name(&self) -> &str {
        "DropTableRefInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::TableRef)?;

        let handler = get_table_ref_handler();
        handler
            .do_drop_table_ref(self.ctx.clone(), &self.plan)
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
