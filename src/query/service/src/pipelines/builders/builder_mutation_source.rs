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

use databend_common_base::runtime::Runtime;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_sources::OneBlockSource;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::binder::MutationType;
use databend_common_sql::executor::physical_plans::MutationSource;
use databend_common_sql::StreamContext;
use databend_common_storages_fuse::operations::CommitMeta;
use databend_common_storages_fuse::operations::ConflictResolveContext;
use databend_common_storages_fuse::operations::MutationAction;
use databend_common_storages_fuse::operations::MutationBlockPruningContext;
use databend_common_storages_fuse::FuseLazyPartInfo;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::SegmentLocation;

use crate::pipelines::processors::TransformAddStreamColumns;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_mutation_source(&mut self, mutation_source: &MutationSource) -> Result<()> {
        let table = self
            .ctx
            .build_table_by_table_info(&mutation_source.table_info, None)?;

        let table = FuseTable::try_from_table(table.as_ref())?.clone();
        let is_delete = mutation_source.input_type == MutationType::Delete;
        if mutation_source.truncate_table {
            // There is no filter and the mutation type is delete,
            // we can truncate the table directly.
            debug_assert!(mutation_source.partitions.is_empty() && is_delete);
            return self.main_pipeline.add_source(
                |output| {
                    let meta = CommitMeta {
                        conflict_resolve_context: ConflictResolveContext::None,
                        new_segment_locs: vec![],
                        table_id: table.get_id(),
                        virtual_schema: None,
                    };
                    let block = DataBlock::empty_with_meta(Box::new(meta));
                    OneBlockSource::create(output, block)
                },
                1,
            );
        }

        let read_partition_columns: Vec<usize> = mutation_source
            .read_partition_columns
            .clone()
            .into_iter()
            .collect();

        let is_lazy =
            mutation_source.partitions.partitions_type() == PartInfoType::LazyLevel && is_delete;
        if is_lazy {
            let ctx = self.ctx.clone();
            let table_clone = table.clone();
            let ctx_clone = self.ctx.clone();
            let filters_clone = mutation_source.filters.clone();
            let projection = Projection::Columns(read_partition_columns.clone());
            let mut segment_locations =
                Vec::with_capacity(mutation_source.partitions.partitions.len());
            for part in &mutation_source.partitions.partitions {
                // Safe to downcast because we know the the partition is lazy
                let part: &FuseLazyPartInfo = FuseLazyPartInfo::from_part(part)?;
                segment_locations.push(SegmentLocation {
                    segment_idx: part.segment_index,
                    location: part.segment_location.clone(),
                    snapshot_loc: None,
                });
            }
            let prune_ctx = MutationBlockPruningContext {
                segment_locations,
                block_count: None,
            };
            Runtime::with_worker_threads(2, Some("do_mutation_block_pruning".to_string()))?
                .block_on(async move {
                    let (_, partitions) = table_clone
                        .do_mutation_block_pruning(
                            ctx_clone,
                            filters_clone,
                            projection,
                            prune_ctx,
                            true,
                        )
                        .await?;
                    ctx.set_partitions(partitions)?;
                    Ok(())
                })?;
        } else {
            self.ctx
                .set_partitions(mutation_source.partitions.clone())?;
        }

        let filter = mutation_source.filters.clone().map(|v| v.filter);
        let mutation_action = if is_delete {
            MutationAction::Deletion
        } else {
            MutationAction::Update
        };
        let col_indices = mutation_source
            .read_partition_columns
            .clone()
            .into_iter()
            .collect();
        let update_mutation_with_filter =
            mutation_source.input_type == MutationType::Update && filter.is_some();
        table.add_mutation_source(
            self.ctx.clone(),
            filter,
            col_indices,
            &mut self.main_pipeline,
            mutation_action,
        )?;

        if table.change_tracking_enabled() {
            let stream_ctx = StreamContext::try_create(
                self.ctx.get_function_context()?,
                table.schema_with_stream(),
                table.get_table_info().ident.seq,
                is_delete,
                update_mutation_with_filter,
            )?;
            self.main_pipeline
                .add_transformer(|| TransformAddStreamColumns::new(stream_ctx.clone()));
        }

        Ok(())
    }
}
