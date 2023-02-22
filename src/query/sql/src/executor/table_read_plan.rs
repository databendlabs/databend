// Copyright 2021 Datafuse Labs.
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

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;

#[async_trait::async_trait]
pub trait ToReadDataSourcePlan {
    /// Real read_plan to access partitions/push_downs
    async fn read_plan(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataSourcePlan> {
        self.read_plan_with_catalog(ctx, "default".to_owned(), push_downs)
            .await
    }

    async fn read_plan_with_catalog(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog: String,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataSourcePlan>;
}

#[async_trait::async_trait]
impl ToReadDataSourcePlan for dyn Table {
    async fn read_plan_with_catalog(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog: String,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataSourcePlan> {
        let (statistics, parts) = self
            .read_partitions(ctx.clone(), push_downs.clone())
            .await?;

        // We need the partition sha256 to specify the result cache.
        if ctx.get_settings().get_enable_query_result_cache()? {
            let sha = parts.compute_sha256()?;
            ctx.add_partitions_sha(sha);
        }

        let source_info = self.get_data_source_info();

        let schema = &source_info.schema();
        let description = statistics.get_description(&source_info.desc());

        let output_schema = match (self.benefit_column_prune(), &push_downs) {
            (true, Some(push_downs)) => match &push_downs.prewhere {
                Some(prewhere) => Arc::new(prewhere.output_columns.project_schema(schema)),
                _ => match &push_downs.projection {
                    Some(projection) => Arc::new(projection.project_schema(schema)),
                    _ => schema.clone(),
                },
            },
            _ => schema.clone(),
        };

        // TODO pass in catalog name

        Ok(DataSourcePlan {
            catalog,
            source_info,
            output_schema,
            parts,
            statistics,
            description,
            tbl_args: self.table_args(),
            push_downs,
        })
    }
}
