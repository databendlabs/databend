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
use chrono::Utc;
use common_exception::Result;
use common_meta_app::schema::CreateIndexReq;
use common_meta_app::schema::IndexMeta;
use common_meta_app::schema::IndexNameIdent;
use common_meta_app::schema::IndexType;
use common_sql::plans::CreateVectorIndexPlan;
use common_storages_fuse::FuseTable;
use common_storages_fuse::TableContext;

use super::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct CreateVectorIndexInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateVectorIndexPlan,
}

impl CreateVectorIndexInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateVectorIndexPlan) -> Result<Self> {
        Ok(CreateVectorIndexInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateVectorIndexInterpreter {
    fn name(&self) -> &str {
        "CreateIndexInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = &self.plan;
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;
        let ctx = self.ctx.clone();
        let column_idx = table
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == &plan.column)
            .ok_or(common_exception::ErrorCode::UnknownColumn(format!(
                "column {} not found in table {}",
                plan.column,
                table.name()
            )))?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        fuse_table
            .create_vector_index(ctx, column_idx, &plan.vector_index, &plan.metric_type)
            .await?;
        let catalog = self.ctx.get_catalog(&plan.catalog)?;
        let index_name = format!("{}.{}.{}", plan.catalog, plan.database, plan.table);
        let tenant = self.ctx.get_tenant();
        let create_index_req = CreateIndexReq {
            if_not_exists: false,
            name_ident: IndexNameIdent { tenant, index_name },
            meta: IndexMeta {
                table_id: table.get_id(),
                index_type: IndexType::IVF,
                created_on: Utc::now(),
                drop_on: None,
                query: String::new(),
            },
        };
        let handler = get_agg_index_handler();
        let _ = handler.do_create_index(catalog, create_index_req).await?;
        Ok(PipelineBuildResult::create())
    }
}
