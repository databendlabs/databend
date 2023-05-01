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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::DropTableByIdReq;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TableNameIdent;
use common_sql::plans::AlterViewPlan;
use common_sql::Planner;
use common_storages_view::view_table::VIEW_ENGINE;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct AlterViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterViewPlan,
}

impl AlterViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterViewPlan) -> Result<Self> {
        Ok(AlterViewInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterViewInterpreter {
    fn name(&self) -> &str {
        "AlterViewInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        // check whether view has exists
        if !self
            .ctx
            .get_catalog(&self.plan.catalog)?
            .list_tables(&self.plan.tenant, &self.plan.database)
            .await?
            .iter()
            .any(|table| {
                table.name() == self.plan.view_name.as_str()
                    && table.get_table_info().engine() == VIEW_ENGINE
            })
        {
            return Err(ErrorCode::ViewAlreadyExists(format!(
                "{}.{} view does not exist",
                self.plan.database, self.plan.view_name
            )));
        }

        self.alter_view().await
    }
}

impl AlterViewInterpreter {
    #[async_backtrace::framed]
    async fn alter_view(&self) -> Result<PipelineBuildResult> {
        // drop view
        let catalog = self.ctx.get_catalog(&self.plan.catalog)?;
        let tbl = catalog
            .get_table(
                self.plan.tenant.as_str(),
                self.plan.database.as_str(),
                self.plan.view_name.as_str(),
            )
            .await?;
        catalog
            .drop_table_by_id(DropTableByIdReq {
                if_exists: true,
                tb_id: tbl.get_id(),
            })
            .await?;

        // create new view
        let mut options = BTreeMap::new();
        let subquery = if self.plan.column_names.is_empty() {
            self.plan.subquery.clone()
        } else {
            let mut planner = Planner::new(self.ctx.clone());
            let (plan, _) = planner.plan_sql(&self.plan.subquery.clone()).await?;
            if plan.schema().fields().len() != self.plan.column_names.len() {
                return Err(ErrorCode::BadDataArrayLength(format!(
                    "column name length mismatch, expect {}, got {}",
                    plan.schema().fields().len(),
                    self.plan.column_names.len(),
                )));
            }
            format!(
                "select * from ({}) {}({})",
                self.plan.subquery,
                self.plan.view_name,
                self.plan.column_names.join(", ")
            )
        };
        options.insert("query".to_string(), subquery);

        let plan = CreateTableReq {
            if_not_exists: true,
            name_ident: TableNameIdent {
                tenant: self.plan.tenant.clone(),
                db_name: self.plan.database.clone(),
                table_name: self.plan.view_name.clone(),
            },
            table_meta: TableMeta {
                engine: VIEW_ENGINE.to_string(),
                options,
                ..Default::default()
            },
        };
        catalog.create_table(plan).await?;

        Ok(PipelineBuildResult::create())
    }
}
