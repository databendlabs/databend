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

use common_base::base::ProgressValues;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::InternalColumn;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;
use common_expression::Scalar;
use common_expression::TableField;

#[async_trait::async_trait]
pub trait ToReadDataSourcePlan {
    /// Real read_plan to access partitions/push_downs
    #[async_backtrace::framed]
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
        internal_columns: Option<BTreeMap<FieldIndex, InternalColumn>>,
    ) -> Result<DataSourcePlan>;
}

#[async_trait::async_trait]
impl ToReadDataSourcePlan for dyn Table {
    #[async_backtrace::framed]
    async fn read_plan_with_catalog(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog: String,
        push_downs: Option<PushDownInfo>,
        internal_columns: Option<BTreeMap<FieldIndex, InternalColumn>>,
    ) -> Result<DataSourcePlan> {
        let (statistics, parts) = if let Some(PushDownInfo {
            filter:
                Some(RemoteExpr::Constant {
                    scalar: Scalar::Boolean(false),
                    ..
                }),
            ..
        }) = &push_downs
        {
            Ok((PartStatistics::default(), Partitions::default()))
        } else {
            self.read_partitions(ctx.clone(), push_downs.clone()).await
        }?;

        ctx.incr_total_scan_value(ProgressValues {
            rows: statistics.read_rows,
            bytes: statistics.read_bytes,
        });

        // We need the partition sha256 to specify the result cache.
        if ctx.get_settings().get_enable_query_result_cache()? {
            let sha = parts.compute_sha256()?;
            ctx.add_partitions_sha(sha);
        }

        let source_info = self.get_data_source_info();

        let schema = &source_info.schema();
        let description = statistics.get_description(&source_info.desc());
        let mut output_schema = match (self.benefit_column_prune(), &push_downs) {
            (true, Some(push_downs)) => match &push_downs.prewhere {
                Some(prewhere) => Arc::new(prewhere.output_columns.project_schema(schema)),
                _ => {
                    if let Some(output_columns) = &push_downs.output_columns {
                        Arc::new(output_columns.project_schema(schema))
                    } else if let Some(projection) = &push_downs.projection {
                        Arc::new(projection.project_schema(schema))
                    } else {
                        schema.clone()
                    }
                }
            },
            _ => schema.clone(),
        };

        if let Some(ref push_downs) = push_downs {
            if let Some(ref virtual_columns) = push_downs.virtual_columns {
                let mut schema = output_schema.as_ref().clone();
                let fields = virtual_columns
                    .iter()
                    .map(|c| TableField::new(&c.name, *c.data_type.clone()))
                    .collect::<Vec<_>>();
                schema.add_columns(&fields)?;
                output_schema = Arc::new(schema);
            }
        }

        if let Some(ref internal_columns) = internal_columns {
            let mut schema = output_schema.as_ref().clone();
            for internal_column in internal_columns.values() {
                schema.add_internal_column(
                    internal_column.column_name(),
                    internal_column.table_data_type(),
                    internal_column.column_id(),
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
            query_internal_columns: internal_columns.is_some(),
        })
    }
}
