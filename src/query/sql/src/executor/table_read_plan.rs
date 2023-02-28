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
use common_catalog::plan::VirtualColumnDataSource;
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
        self.read_plan_with_catalog(ctx, "default".to_owned(), push_downs, None)
            .await
    }

    async fn read_plan_with_catalog(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog: String,
        push_downs: Option<PushDownInfo>,
        virtual_column_data_source: Option<VirtualColumnDataSource>,
    ) -> Result<DataSourcePlan>;
}

#[async_trait::async_trait]
impl ToReadDataSourcePlan for dyn Table {
    async fn read_plan_with_catalog(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog: String,
        push_downs: Option<PushDownInfo>,
        virtual_column_data_source: Option<VirtualColumnDataSource>,
    ) -> Result<DataSourcePlan> {
        let (statistics, parts) = self.read_partitions(ctx, push_downs.clone()).await?;

        let source_info = self.get_data_source_info();

        let schema = match virtual_column_data_source {
            None => source_info.schema(),
            Some(ref virtual_column_data_source) => virtual_column_data_source.schema.clone(),
        };

        let description = statistics.get_description(&source_info.desc());

        let mut output_schema = match (self.benefit_column_prune(), &push_downs) {
            (true, Some(push_downs)) => match &push_downs.prewhere {
                Some(prewhere) => Arc::new(prewhere.output_columns.project_schema(schema.as_ref())),
                _ => match &push_downs.projection {
                    Some(projection) => Arc::new(projection.project_schema(schema.as_ref())),
                    _ => schema,
                },
            },
            _ => schema,
        };

        // append virtual column to output_schema
        if let Some(ref virtual_column_data_source) = virtual_column_data_source {
            let mut schema = output_schema.as_ref().clone();
            for virtual_column in virtual_column_data_source.project_virtual_columns.values() {
                schema.add_virtual_column(
                    virtual_column.column_name(),
                    virtual_column.table_data_type(),
                    virtual_column.column_id(),
                );
            }
            output_schema = Arc::new(schema);
        }
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
            virtual_column_data_source,
        })
    }
}
