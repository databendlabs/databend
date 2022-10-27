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

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planner::PhysicalScalar;
use common_meta_app::schema::TableInfo;

use super::Extras;

use crate::planner::plans::Partitions;
use  crate::planner::plans::Projection;
use  crate::planner::plans::StageTableInfo;
use  crate::planner::plans::Statistics;


#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SourceInfo {
    // Normal table source, `fuse/system`.
    TableSource(TableInfo),

    // Internal/External source, like `s3://` or `azblob://`.
    StageSource(StageTableInfo),
}

impl SourceInfo {
    pub fn schema(&self) -> Arc<DataSchema> {
        match self {
            SourceInfo::TableSource(table_info) => table_info.schema(),
            SourceInfo::StageSource(table_info) => table_info.schema(),
        }
    }

    pub fn desc(&self) -> String {
        match self {
            SourceInfo::TableSource(table_info) => table_info.desc.clone(),
            SourceInfo::StageSource(table_info) => table_info.desc(),
        }
    }
}

// TODO: Delete the scan plan field, but it depends on plan_parser:L394
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ReadDataSourcePlan {
    // TODO catalog id is better
    pub catalog: String,
    pub source_info: SourceInfo,

    /// Required fields to scan.
    ///
    /// After optimization, only a sub set of the fields in `table_info.schema().fields` are needed.
    /// The key is the column_index of `ColumnEntry` in `Metadata`.
    ///
    /// If it is None, one should use `table_info.schema().fields()`.
    pub scan_fields: Option<BTreeMap<usize, DataField>>,

    pub parts: Partitions,
    pub statistics: Statistics,
    pub description: String,

    pub tbl_args: Option<Vec<PhysicalScalar>>,
    pub push_downs: Option<Extras>,
}

impl ReadDataSourcePlan {
    /// Return schema after the projection
    pub fn schema(&self) -> DataSchemaRef {
        self.scan_fields
            .clone()
            .map(|x| {
                let fields: Vec<_> = x.iter().map(|(_, f)| f.clone()).collect();
                Arc::new(self.source_info.schema().project_by_fields(fields))
            })
            .unwrap_or_else(|| self.source_info.schema())
    }

    /// Return designated required fields or all fields in a hash map.
    pub fn scan_fields(&self) -> BTreeMap<usize, DataField> {
        self.scan_fields
            .clone()
            .unwrap_or_else(|| self.source_info.schema().fields_map())
    }

    pub fn projections(&self) -> Projection {
        let default_proj = || {
            (0..self.source_info.schema().fields().len())
                .into_iter()
                .collect::<Vec<usize>>()
        };

        if let Some(Extras {
            projection: Some(prj),
            ..
        }) = &self.push_downs
        {
            prj.clone()
        } else {
            Projection::Columns(default_proj())
        }
    }
}


#[async_trait::async_trait]
pub trait ToReadDataSourcePlan {
    /// Real read_plan to access partitions/push_downs
    async fn read_plan(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<Extras>,
    ) -> Result<ReadDataSourcePlan> {
        self.read_plan_with_catalog(ctx, "default".to_owned(), push_downs)
            .await
    }

    async fn read_plan_with_catalog(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog: String,
        push_downs: Option<Extras>,
    ) -> Result<ReadDataSourcePlan>;
}

#[async_trait::async_trait]
impl ToReadDataSourcePlan for dyn Table {
    async fn read_plan_with_catalog(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog: String,
        push_downs: Option<Extras>,
    ) -> Result<ReadDataSourcePlan> {
        let (statistics, parts) = self.read_partitions(ctx, push_downs.clone()).await?;

        let table_info = self.get_table_info();
        let table_meta = &table_info.meta;
        let description = statistics.get_description(table_info);

        let scan_fields = match (self.benefit_column_prune(), &push_downs) {
            (true, Some(push_downs)) => match &push_downs.prewhere {
                Some(prewhere) => extract_scan_fields_from_projection(
                    &table_meta.schema,
                    &prewhere.output_columns,
                ),
                _ => match &push_downs.projection {
                    Some(projection) => {
                        extract_scan_fields_from_projection(&table_meta.schema, projection)
                    }
                    _ => None,
                },
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
