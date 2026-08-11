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

use databend_common_catalog::table_context::TableContextTableAccess;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateMaterializedViewMeta;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::MATERIALIZED_VIEW_ENGINE;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_sql::plans::CreateMaterializedViewPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct CreateMaterializedViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateMaterializedViewPlan,
}

impl CreateMaterializedViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateMaterializedViewPlan) -> Result<Self> {
        Ok(CreateMaterializedViewInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateMaterializedViewInterpreter {
    fn name(&self) -> &str {
        "CreateMaterializedViewInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;

        let materialized_view = CreateMaterializedViewMeta {
            definition: self.plan.mv_definition.clone(),
            // TODO
            expected_source_generation: 0,
        };

        let plan = CreateTableReq {
            create_option: self.plan.create_option,
            catalog_name: if self.plan.create_option.is_overriding() {
                Some(self.plan.catalog.to_string())
            } else {
                None
            },
            name_ident: TableNameIdent {
                tenant: self.plan.tenant.clone(),
                db_name: self.plan.database.clone(),
                table_name: self.plan.view_name.clone(),
            },
            table_meta: TableMeta {
                schema: self.plan.schema.clone(),
                engine: MATERIALIZED_VIEW_ENGINE.to_string(),
                options: self.plan.options.clone(),
                ..Default::default()
            },
            source_table_option: self.plan.source_table_option.clone(),
            as_dropped: false,
            materialized_view: Some(materialized_view),
            table_properties: None,
            table_partition: None,
        };
        catalog.create_table(plan).await?;

        Ok(PipelineBuildResult::create())
    }
}
