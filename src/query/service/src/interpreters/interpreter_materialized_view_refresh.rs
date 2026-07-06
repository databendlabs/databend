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
use databend_common_sql::plans::RefreshMaterializedViewPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct RefreshMaterializedViewInterpreter {
    _ctx: Arc<QueryContext>,
    _plan: RefreshMaterializedViewPlan,
}

impl RefreshMaterializedViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RefreshMaterializedViewPlan) -> Result<Self> {
        Ok(RefreshMaterializedViewInterpreter {
            _ctx: ctx,
            _plan: plan,
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for RefreshMaterializedViewInterpreter {
    fn name(&self) -> &str {
        "RefreshMaterializedViewInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        Err(ErrorCode::Unimplemented(
            "REFRESH MATERIALIZED VIEW is not supported yet",
        ))
    }
}
