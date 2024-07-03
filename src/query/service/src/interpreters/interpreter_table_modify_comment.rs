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
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_types::MatchSeq;
use databend_common_sql::plans::ModifyTableCommentPlan;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_storages_view::view_table::VIEW_ENGINE;

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
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();

        let tbl = self
            .ctx
            .get_catalog(catalog_name)
            .await?
            .get_table(&self.ctx.get_tenant(), db_name, tbl_name)
            .await
            .ok();

        if let Some(table) = &tbl {
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
            let table_id = table_info.ident.table_id;
            let table_version = table_info.ident.seq;
            let mut new_table_meta = table_info.meta.clone();
            new_table_meta.comment = self.plan.new_comment.clone();

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Exact(table_version),
                new_table_meta,
            };

            catalog.update_single_table_meta(req,table_info).await?;
        };

        Ok(PipelineBuildResult::create())
    }
}
