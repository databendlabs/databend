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
use databend_common_meta_app::schema::MVId;
use databend_common_meta_app::schema::MVMetaIdent;
use databend_common_meta_app::schema::is_materialized_view_engine;
use databend_common_sql::plans::DropMaterializedViewPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;

pub struct DropMaterializedViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropMaterializedViewPlan,
}

impl DropMaterializedViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropMaterializedViewPlan) -> Result<Self> {
        Ok(DropMaterializedViewInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropMaterializedViewInterpreter {
    fn name(&self) -> &str {
        "DropMaterializedViewInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.clone();
        let db_name = self.plan.database.clone();
        let view_name = self.plan.view_name.clone();
        let tbl = match self
            .ctx
            .get_table(&catalog_name, &db_name, &view_name)
            .await
        {
            Ok(table) => Some(table),
            Err(error) => {
                if (error.code() == ErrorCode::UNKNOWN_TABLE
                    || error.code() == ErrorCode::UNKNOWN_CATALOG
                    || error.code() == ErrorCode::UNKNOWN_DATABASE)
                    && self.plan.if_exists
                {
                    None
                } else if error.code() == ErrorCode::UNKNOWN_TABLE && !self.plan.if_exists {
                    return Err(ErrorCode::UnknownMaterializedView(format!(
                        "unknown materialized view `{}`.`{}` in catalog '{}'",
                        db_name, view_name, &catalog_name
                    )));
                } else {
                    return Err(error);
                }
            }
        };

        if let Some(table) = &tbl {
            if !is_materialized_view_engine(table.engine()) {
                return Err(ErrorCode::TableEngineNotSupported(format!(
                    "{}.{} is not a MATERIALIZED VIEW, please use `DROP TABLE {}.{}`",
                    &self.plan.database,
                    &self.plan.view_name,
                    &self.plan.database,
                    &self.plan.view_name
                )));
            }

            let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
            let ident =
                MVMetaIdent::new_generic(self.plan.tenant.clone(), MVId::new(table.get_id()));
            if let Err(err) = catalog.drop_materialized_view(&ident).await {
                if self.plan.if_exists && err.code() == ErrorCode::UNKNOWN_MATERIALIZED_VIEW {
                    return Ok(PipelineBuildResult::create());
                }
                return Err(err);
            }
        };

        Ok(PipelineBuildResult::create())
    }
}
