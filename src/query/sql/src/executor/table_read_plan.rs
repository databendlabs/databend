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

use databend_common_base::base::ProgressValues;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::StreamTablePart;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::FieldIndex;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;

#[async_trait::async_trait]
pub trait ToReadDataSourcePlan {
    async fn read_plan(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        internal_columns: Option<BTreeMap<FieldIndex, InternalColumn>>,
        update_stream_columns: bool,
        dry_run: bool,
    ) -> Result<DataSourcePlan>;
}

#[async_trait::async_trait]
impl ToReadDataSourcePlan for dyn Table {
    #[async_backtrace::framed]
    async fn read_plan(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        internal_columns: Option<BTreeMap<FieldIndex, InternalColumn>>,
        update_stream_columns: bool,
        dry_run: bool,
    ) -> Result<DataSourcePlan> {
        let start = std::time::Instant::now();

        let (statistics, mut parts) = if let Some(PushDownInfo {
            filters:
                Some(Filters {
                    filter:
                        RemoteExpr::Constant {
                            scalar: Scalar::Boolean(false),
                            ..
                        },
                    ..
                }),
            ..
        }) = &push_downs
        {
            Ok((PartStatistics::default(), Partitions::default()))
        } else {
            ctx.set_status_info("[TABLE-SCAN] Reading table partitions");
            self.read_partitions(ctx.clone(), push_downs.clone(), dry_run)
                .await
        }?;

        let mut base_block_ids = None;
        if parts.partitions.len() == 1 {
            let part = parts.partitions[0].clone();
            if let Some(part) = StreamTablePart::from_part(&part) {
                parts = part.inner();
                base_block_ids = Some(part.base_block_ids());
            }
        }

        ctx.set_status_info(&format!(
            "[TABLE-SCAN] Partitions loaded, elapsed: {:?}",
            start.elapsed()
        ));

        ctx.incr_total_scan_value(ProgressValues {
            rows: statistics.read_rows,
            bytes: statistics.read_bytes,
        });

        // We need the partition sha256 to specify the result cache.
        let settings = ctx.get_settings();
        if settings.get_enable_query_result_cache()? {
            let sha = parts.compute_sha256()?;
            ctx.add_partitions_sha(sha);
        }

        let source_info = self.get_data_source_info();
        let description = statistics.get_description(&source_info.desc());
        let mut output_schema = match (self.support_column_projection(), &push_downs) {
            (true, Some(push_downs)) => {
                let schema = &self.schema_with_stream();
                match &push_downs.prewhere {
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
                }
            }
            _ => self.schema(),
        };

        if let Some(ref push_downs) = push_downs {
            if let Some(ref virtual_column) = push_downs.virtual_column {
                let mut schema = output_schema.as_ref().clone();
                for field in &virtual_column.virtual_column_fields {
                    schema.add_internal_field(
                        &field.name,
                        *field.data_type.clone(),
                        field.column_id,
                    );
                }
                output_schema = Arc::new(schema);
            }
        }

        if let Some(ref internal_columns) = internal_columns {
            let mut schema = output_schema.as_ref().clone();
            for internal_column in internal_columns.values() {
                schema.add_internal_field(
                    internal_column.column_name(),
                    internal_column.table_data_type(),
                    internal_column.column_id(),
                );
            }
            output_schema = Arc::new(schema);
        }

        ctx.set_status_info(&format!(
            "[TABLE-SCAN] Scan plan ready, elapsed: {:?}",
            start.elapsed()
        ));

        Ok(DataSourcePlan {
            source_info,
            output_schema,
            parts,
            statistics,
            description,
            tbl_args: self.table_args(),
            push_downs,
            internal_columns,
            base_block_ids,
            update_stream_columns,
            // Set a dummy id, will be set real id later
            table_index: usize::MAX,
            scan_id: usize::MAX,
            block_slot: None,
        })
    }
}
