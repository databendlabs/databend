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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline::core::Pipeline;

use super::ParquetTable;
use crate::ParquetPart;
use crate::ParquetPruner;
use crate::ParquetSourceType;
use crate::parquet_reader::ParquetReaderBuilder;
use crate::source::ParquetSource;
use crate::utils::calc_parallelism;

impl ParquetTable {
    #[inline]
    pub(super) fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let internal_columns = plan
            .internal_columns
            .as_ref()
            .map(|m| {
                m.values()
                    .map(|i| i.column_type.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let need_row_number = internal_columns.contains(&InternalColumnType::FileRowNumber);

        let table_schema: TableSchemaRef = self.table_info.schema();
        // If there is a `ParquetFilePart`, we should create pruner for it.
        // Although `ParquetFilePart`s are always staying at the end of `parts` when `do_read_partitions`,
        // but parts are reshuffled when `redistribute_source_fragment`, so let us check all of them.
        let has_file_part = plan.parts.partitions.iter().any(|p| {
            matches!(
                p.as_any().downcast_ref::<ParquetPart>().unwrap(),
                ParquetPart::SmallFiles(_) | ParquetPart::File(_)
            )
        });
        let pruner = if has_file_part {
            Some(ParquetPruner::try_create(
                ctx.get_function_context()?,
                table_schema.clone(),
                self.leaf_fields.clone(),
                &plan.push_downs,
                self.read_options,
                vec![],
            )?)
        } else {
            None
        };

        let num_threads = calc_parallelism(&ctx, plan)?;

        let topk = plan
            .push_downs
            .as_ref()
            .and_then(|p| p.top_k(&self.schema()));

        let op = Arc::new(self.operator.clone());
        let mut builder = ParquetReaderBuilder::create_with_parquet_schema(
            ctx.clone(),
            op.clone(),
            table_schema.clone(),
            self.schema_descr.clone(),
            Some(self.arrow_schema.clone()),
            Some(self.schema_from.clone()),
        )
        .with_options(self.read_options)
        .with_push_downs(plan.push_downs.as_ref());

        if !need_row_number {
            builder = builder.with_pruner(pruner).with_topk(topk.as_ref());
        }

        let row_group_reader = Arc::new(
            builder.build_row_group_reader(ParquetSourceType::StageTable, need_row_number)?,
        );
        let full_file_reader = if has_file_part {
            Some(Arc::new(builder.build_full_reader(
                ParquetSourceType::StageTable,
                need_row_number,
            )?))
        } else {
            None
        };

        let topk = Arc::new(topk);
        pipeline.add_source(
            |output| {
                ParquetSource::create(
                    ctx.clone(),
                    ParquetSourceType::StageTable,
                    output,
                    row_group_reader.clone(),
                    full_file_reader.clone(),
                    topk.clone(),
                    internal_columns.clone(),
                    plan.push_downs.clone(),
                    table_schema.clone(),
                    op.clone(),
                )
            },
            num_threads,
        )
    }
}
