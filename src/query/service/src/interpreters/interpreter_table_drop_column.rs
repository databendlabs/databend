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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::DatabaseType;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_types::MatchSeq;
use common_sql::plans::DropTableColumnPlan;
use common_storages_share::save_share_table_info;
use common_storages_view::view_table::VIEW_ENGINE;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DropTableColumnInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropTableColumnPlan,
}

impl DropTableColumnInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropTableColumnPlan) -> Result<Self> {
        Ok(DropTableColumnInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropTableColumnInterpreter {
    fn name(&self) -> &str {
        "DropTableColumnInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();
        let table = self
            .ctx
            .get_catalog(catalog_name)?
            .get_table(self.ctx.get_tenant().as_str(), db_name, tbl_name)
            .await?;

        let table_info = table.get_table_info();
        if table_info.engine() == VIEW_ENGINE {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} engine is VIEW that doesn't support alter",
                &self.plan.database, &self.plan.table
            )));
        }
        if table_info.db_type != DatabaseType::NormalDB {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} doesn't support alter",
                &self.plan.database, &self.plan.table
            )));
        }

        let catalog = self.ctx.get_catalog(catalog_name)?;
        let mut new_table_meta = table.get_table_info().meta.clone();
        new_table_meta.drop_column(&self.plan.column)?;

        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        let req = UpdateTableMetaReq {
            table_id,
            seq: MatchSeq::Exact(table_version),
            new_table_meta,
            copied_files: None,
        };

        let res = catalog.update_table_meta(table_info, req).await?;
        if let Some(share_table_info) = res.share_table_info {
            save_share_table_info(
                &self.ctx.get_tenant(),
                self.ctx.get_data_operator()?.operator(),
                share_table_info,
            )
            .await?;
        }

        Ok(PipelineBuildResult::create())
    }
}
