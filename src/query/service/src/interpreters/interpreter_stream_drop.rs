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
use common_meta_app::schema::DropTableByIdReq;
use common_sql::plans::DropStreamPlan;
use common_storages_stream::stream_table::STREAM_ENGINE;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DropStreamInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropStreamPlan,
}

impl DropStreamInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropStreamPlan) -> Result<Self> {
        Ok(DropStreamInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropStreamInterpreter {
    fn name(&self) -> &str {
        "DropStreamInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.clone();
        let db_name = self.plan.database.clone();
        let stream_name = self.plan.stream_name.clone();
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let tenant = self.ctx.get_tenant();
        let tbl = catalog
            .get_table(tenant.as_str(), &db_name, &stream_name)
            .await
            .ok();

        if tbl.is_none() && !self.plan.if_exists {
            return Err(ErrorCode::UnknownTable(format!(
                "unknown stream `{}`.`{}` in catalog '{}'",
                db_name, stream_name, &catalog_name
            )));
        }

        if let Some(table) = &tbl {
            let engine = table.get_table_info().engine();
            if engine != STREAM_ENGINE {
                return Err(ErrorCode::Internal(format!(
                    "{}.{} is not STREAM, please use `DROP {} {}.{}`",
                    &self.plan.database,
                    &self.plan.stream_name,
                    engine,
                    &self.plan.database,
                    &self.plan.stream_name
                )));
            }

            catalog
                .drop_table_by_id(DropTableByIdReq {
                    if_exists: self.plan.if_exists,
                    tenant: self.plan.tenant.clone(),
                    tb_id: table.get_id(),
                })
                .await?;
        };

        Ok(PipelineBuildResult::create())
    }
}
