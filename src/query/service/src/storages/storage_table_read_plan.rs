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

use common_exception::Result;
use common_planners::Extras;
use common_planners::Projection;
use common_planners::ReadDataSourcePlan;
use common_planners::SourceInfo;

use crate::sessions::QueryContext;
use crate::storages::Table;

#[async_trait::async_trait]
pub trait ToReadDataSourcePlan {
    /// Real read_plan to access partitions/push_downs
    async fn read_plan(
        &self,
        ctx: Arc<QueryContext>,
        push_downs: Option<Extras>,
    ) -> Result<ReadDataSourcePlan> {
        self.read_plan_with_catalog(ctx, "default".to_owned(), push_downs)
            .await
    }

    async fn read_plan_with_catalog(
        &self,
        ctx: Arc<QueryContext>,
        catalog: String,
        push_downs: Option<Extras>,
    ) -> Result<ReadDataSourcePlan>;
}

#[async_trait::async_trait]
impl ToReadDataSourcePlan for dyn Table {
    async fn read_plan_with_catalog(
        &self,
        ctx: Arc<QueryContext>,
        catalog: String,
        push_downs: Option<Extras>,
    ) -> Result<ReadDataSourcePlan> {
        let (statistics, parts) = self.read_partitions(ctx, push_downs.clone()).await?;
        let table_info = self.get_table_info();
        let description = statistics.get_description(table_info);

        let scan_fields = match (self.benefit_column_prune(), &push_downs) {
            (true, Some(push_downs)) => match &push_downs.projection {
                Some(projection) => match projection {
                    Projection::Columns(ref indices) => {
                        if indices.len() < table_info.schema().fields().len() {
                            let fields = indices
                                .iter()
                                .map(|i| table_info.schema().field(*i).clone());

                            Some((indices.iter().cloned().zip(fields)).collect::<BTreeMap<_, _>>())
                        } else {
                            None
                        }
                    }
                    Projection::InnerColumns(ref path_indices) => {
                        let column_ids: Vec<usize> = path_indices.keys().cloned().collect();
                        let new_schema = table_info.schema().inner_project(path_indices);
                        Some(
                            (column_ids.iter().cloned().zip(new_schema.fields().clone()))
                                .collect::<BTreeMap<_, _>>(),
                        )
                    }
                },
                _ => None,
            },
            _ => None,
        };

        // TODO pass in catalog name

        Ok(ReadDataSourcePlan {
            catalog,
            source_info: SourceInfo::TableSource(table_info.clone()),
            scan_fields,
            parts,
            statistics,
            description,
            tbl_args: self.table_args(),
            push_downs,
        })
    }
}
