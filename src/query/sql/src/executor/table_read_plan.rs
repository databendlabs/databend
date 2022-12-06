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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
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
        let (statistics, parts) = self.read_partitions(ctx, push_downs.clone()).await?;

        let source_info = self.get_data_source_info();

        let schema = &source_info.schema();
        let description = statistics.get_description(&source_info.desc());

        let scan_fields = match (self.benefit_column_prune(), &push_downs) {
            (true, Some(push_downs)) => match &push_downs.prewhere {
                Some(prewhere) => {
                    extract_scan_fields_from_projection(schema, &prewhere.output_columns)
                }
                _ => match &push_downs.projection {
                    Some(projection) => extract_scan_fields_from_projection(schema, projection),
                    _ => None,
                },
            },
            _ => None,
        };

        // TODO pass in catalog name

        Ok(DataSourcePlan {
            catalog,
            source_info,
            scan_fields,
            parts,
            statistics,
            description,
            tbl_args: self.table_args(),
            push_downs,
        })
    }
}

fn extract_scan_fields_from_projection(
    schema: &DataSchema,
    projection: &Projection,
) -> Option<BTreeMap<usize, DataField>> {
    match projection {
        Projection::Columns(ref indices) => {
            if indices.len() < schema.fields().len() {
                let fields = indices.iter().map(|i| schema.field(*i).clone());

                Some((indices.iter().cloned().zip(fields)).collect::<BTreeMap<_, _>>())
            } else {
                None
            }
        }
        Projection::InnerColumns(ref path_indices) => {
            let column_ids: Vec<usize> = path_indices.keys().cloned().collect();
            let new_schema = schema.inner_project(path_indices);
            Some(
                (column_ids.iter().cloned().zip(new_schema.fields().clone()))
                    .collect::<BTreeMap<_, _>>(),
            )
        }
    }
}
