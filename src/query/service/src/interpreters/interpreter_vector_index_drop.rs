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

use aggregating_index::get_agg_index_handler;
use common_exception::Result;
use common_meta_app::schema::DropIndexReq;
use common_meta_app::schema::IndexNameIdent;
use common_sql::plans::DropVectorIndexPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DropVectorIndexInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropVectorIndexPlan,
}

impl DropVectorIndexInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropVectorIndexPlan) -> Result<Self> {
        Ok(DropVectorIndexInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropVectorIndexInterpreter {
    fn name(&self) -> &str {
        "DropVectorIndexInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let index_name = self.plan.index.clone();
        let catalog = self.ctx.get_catalog(&self.ctx.get_current_catalog())?;
        let drop_index_req = DropIndexReq {
            if_exists: self.plan.if_exists,
            name_ident: IndexNameIdent { tenant, index_name },
        };

        let handler = get_agg_index_handler();
        let _ = handler.do_drop_index(catalog, drop_index_req).await?;

        Ok(PipelineBuildResult::create())
    }
}
