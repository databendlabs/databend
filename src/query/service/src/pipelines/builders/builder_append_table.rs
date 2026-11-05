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

use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::group_hash_entries;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_pipeline::basic::Exchange;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::pipelines::PipelineBuilder;
use crate::pipelines::builders::SortPipelineBuilder;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

struct PartitionKeyExchange {
    key_offsets: Vec<usize>,
}

impl Exchange for PartitionKeyExchange {
    const NAME: &'static str = "PartitionKey";

    fn partition(&self, data_block: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        let hash_columns = ProjectedBlock::project(&self.key_offsets, &data_block);
        let mut hashes = vec![0; data_block.num_rows()];
        group_hash_entries(hash_columns, &mut hashes);
        let indices = hashes
            .into_iter()
            .map(|hash| hash % n as u64)
            .collect::<Vec<_>>();
        DataBlock::scatter(&data_block, &indices, n)
    }
}

/// This file implements append to table pipeline builder.
impl PipelineBuilder {
    /// Coalesce hash-partitioned writes on the current node. This only regroups rows
    /// within this node: for one writer per partition key across the cluster, callers
    /// must route the same partition keys to one node first (INSERT and COPY do this
    /// with a hash exchange; INSERT ALL and MERGE INTO do not).
    pub fn build_table_write_layout(
        ctx: Arc<QueryContext>,
        pipeline: &mut Pipeline,
        table: Arc<dyn Table>,
    ) -> Result<()> {
        let input_schema = DataSchema::from(&table.schema().remove_virtual_computed_fields());
        Self::build_table_write_layout_with_schema(ctx, pipeline, table.as_ref(), input_schema)
    }

    /// Same as [`Self::build_table_write_layout`] for inputs whose columns differ from the
    /// table schema (MERGE INTO carries stream columns, REPLACE INTO may carry a DELETE WHEN
    /// column). Partition keys are resolved by name.
    pub fn build_table_write_layout_with_schema(
        ctx: Arc<QueryContext>,
        pipeline: &mut Pipeline,
        table: &dyn Table,
        input_schema: DataSchema,
    ) -> Result<()> {
        if table.engine() != "FUSE" {
            return Ok(());
        }
        let fuse_table = FuseTable::try_from_table(table)?;
        if !fuse_table.use_hash_write_distribution() {
            return Ok(());
        }

        let num_input_columns = input_schema.num_fields();
        // Share the key evaluation and sort order with the serialize stage so the
        // lane layout and the persisted cluster statistics describe the same columns.
        let layout = fuse_table.get_cluster_stats_gen(
            ctx.clone(),
            0,
            fuse_table.get_block_thresholds(),
            Arc::new(input_schema),
        )?;
        if layout.partition_key_index.is_empty() {
            return Ok(());
        }

        let key_exprs = match layout.eval_operators.as_slice() {
            [] => Vec::new(),
            [BlockOperator::Map { exprs, .. }] => exprs.clone(),
            _ => {
                return Err(ErrorCode::Internal(
                    "unexpected key evaluation operators for table write layout",
                ));
            }
        };
        // Vector clustering appends its sort key later in the append pipeline, so only
        // the evaluated partition/cluster key columns exist at this point.
        let num_sort_columns = num_input_columns + key_exprs.len();
        let fields = layout.out_fields[..num_sort_columns].to_vec();

        // Sort each lane by the partition key, then by the linear cluster key, so a
        // partition's blocks within a lane do not overlap on the cluster key.
        // Hilbert keys have no lexicographic order and vector keys are assigned later.
        let mut sort_desc: Vec<SortColumnDescription> = Vec::new();
        let mut push_key = |offset: usize| {
            if sort_desc.iter().all(|desc| desc.offset != offset) {
                sort_desc.push(SortColumnDescription {
                    offset,
                    asc: true,
                    nulls_first: false,
                });
            }
        };
        layout.partition_key_index.iter().for_each(|&i| push_key(i));
        if layout.is_linear() {
            layout.sort_descs().iter().for_each(|d| push_key(d.offset));
        }

        let func_ctx = ctx.get_function_context()?;
        if !key_exprs.is_empty() {
            let op = BlockOperator::Map {
                exprs: key_exprs,
                projections: None,
            };
            pipeline.add_transformer(|| {
                CompoundBlockOperator::new(vec![op.clone()], func_ctx.clone(), num_input_columns)
            });
        }

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let key_offsets = layout.partition_key_index.clone();
        pipeline.exchange(max_threads, Arc::new(PartitionKeyExchange { key_offsets }))?;

        let sort_schema = DataSchemaRefExt::create(fields);
        let enable_fixed_rows = ctx.get_settings().get_enable_fixed_rows_sort()?;
        let sort_builder = SortPipelineBuilder::create(
            ctx.clone(),
            sort_schema,
            sort_desc.into(),
            None,
            enable_fixed_rows,
        )?
        .with_block_size_hit(table.get_block_thresholds().max_rows_per_block);
        sort_builder.build_local_full_sort_pipeline(pipeline, false)?;

        if num_sort_columns > num_input_columns {
            let projection = (0..num_input_columns).collect::<Vec<_>>();
            pipeline.add_transformer(|| {
                CompoundBlockOperator::new(
                    vec![BlockOperator::Project {
                        projection: projection.clone(),
                    }],
                    func_ctx.clone(),
                    num_sort_columns,
                )
            });
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_append2table_with_commit_pipeline(
        ctx: Arc<QueryContext>,
        main_pipeline: &mut Pipeline,
        table: Arc<dyn Table>,
        source_schema: DataSchemaRef,
        copied_files: Option<UpsertTableCopiedFileReq>,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        overwrite: bool,
        deduplicated_label: Option<String>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        Self::fill_and_reorder_columns(ctx.clone(), main_pipeline, table.clone(), source_schema)?;
        Self::build_table_write_layout(ctx.clone(), main_pipeline, table.clone())?;

        table.append_data(ctx.clone(), main_pipeline, table_meta_timestamps)?;
        table.commit_insertion(
            ctx,
            main_pipeline,
            copied_files,
            update_stream_meta,
            overwrite,
            None,
            deduplicated_label,
            table_meta_timestamps,
        )?;

        Ok(())
    }

    pub fn build_append2table_without_commit_pipeline(
        ctx: Arc<QueryContext>,
        main_pipeline: &mut Pipeline,
        table: Arc<dyn Table>,
        source_schema: DataSchemaRef,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        Self::fill_and_reorder_columns(ctx.clone(), main_pipeline, table.clone(), source_schema)?;
        // This layout is local to each writer. Distributed COPY does not yet route
        // partition keys across nodes, so one partition may retain a tail block
        // from each worker.
        Self::build_table_write_layout(ctx.clone(), main_pipeline, table.clone())?;

        table.append_data(ctx, main_pipeline, table_meta_timestamps)?;

        Ok(())
    }
}
