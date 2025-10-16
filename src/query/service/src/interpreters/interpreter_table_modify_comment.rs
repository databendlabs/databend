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

use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_sql::plans::ModifyTableCommentPlan;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;

use crate::interpreters::interpreter_table_add_column::commit_table_meta;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
pub struct ModifyTableCommentInterpreter {
    ctx: Arc<QueryContext>,
    plan: ModifyTableCommentPlan,
}

impl ModifyTableCommentInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ModifyTableCommentPlan) -> Result<Self> {
        Ok(ModifyTableCommentInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ModifyTableCommentInterpreter {
    fn name(&self) -> &str {
        "ModifyTableCommentInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();

        let catalog = self.ctx.get_catalog(catalog_name).await?;
        match catalog
            .get_table(&self.ctx.get_tenant(), db_name, tbl_name)
            .await
        {
            Ok(table) => {
                // check mutability
                table.check_mutable()?;

                let table_info = table.get_table_info();
                let engine = table.engine();
                if matches!(engine, VIEW_ENGINE | STREAM_ENGINE) {
                    return Err(ErrorCode::TableEngineNotSupported(format!(
                        "{}.{} engine is {} that doesn't support alter",
                        &self.plan.database, &self.plan.table, engine
                    )));
                }
                if table_info.db_type != DatabaseType::NormalDB {
                    return Err(ErrorCode::TableEngineNotSupported(format!(
                        "{}.{} doesn't support alter",
                        &self.plan.database, &self.plan.table
                    )));
                }

                let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;
                let mut new_table_meta = table_info.meta.clone();
                new_table_meta.comment = self.plan.new_comment.clone();

                commit_table_meta(
                    &self.ctx,
                    table.as_ref(),
                    table_info,
                    new_table_meta,
                    catalog,
                )
                .await?
            }
            Err(e) => {
                if !(e.code() == ErrorCode::UNKNOWN_TABLE && self.plan.if_exists) {
                    return Err(e);
                }
            }
        }
        Ok(PipelineBuildResult::create())
    }
}
