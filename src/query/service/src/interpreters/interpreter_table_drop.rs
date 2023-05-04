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

use common_catalog::table::TableExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::DropTableByIdReq;
use common_sql::plans::DropTablePlan;
use common_storages_share::save_share_spec;
use common_storages_view::view_table::VIEW_ENGINE;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DropTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropTablePlan,
}

impl DropTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropTablePlan) -> Result<Self> {
        Ok(DropTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropTableInterpreter {
    fn name(&self) -> &str {
        "DropTableInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();
        let tbl = self
            .ctx
            .get_table(catalog_name, db_name, tbl_name)
            .await
            .ok();

        if tbl.is_none() && !self.plan.if_exists {
            return Err(ErrorCode::UnknownTable(format!(
                "unknown table {}.{}",
                db_name, tbl_name
            )));
        }
        if let Some(tbl) = tbl {
            if tbl.get_table_info().engine() == VIEW_ENGINE {
                return Err(ErrorCode::TableEngineNotSupported(format!(
                    "{}.{} engine is VIEW that doesn't support drop, use `DROP VIEW {}.{}` instead",
                    &self.plan.database, &self.plan.table, &self.plan.database, &self.plan.table
                )));
            }
            let catalog = self.ctx.get_catalog(catalog_name)?;

            let resp = catalog
                .drop_table_by_id(DropTableByIdReq {
                    if_exists: self.plan.if_exists,
                    tb_id: tbl.get_table_info().ident.table_id,
                })
                .await?;

            // if `plan.all`, truncate, then purge the historical data
            if self.plan.all {
                let purge = true;
                // the above `catalog.drop_table` operation changed the table meta version,
                // thus if we do not refresh the table instance, `truncate` will fail
                let latest = tbl.as_ref().refresh(self.ctx.as_ref()).await?;
                latest.truncate(self.ctx.clone(), purge).await?
            }

            if let Some((spec_vec, share_table_info)) = resp.spec_vec {
                save_share_spec(
                    &self.ctx.get_tenant(),
                    self.ctx.get_data_operator()?.operator(),
                    Some(spec_vec),
                    Some(share_table_info),
                )
                .await?;
            }
        }

        Ok(PipelineBuildResult::create())
    }
}
