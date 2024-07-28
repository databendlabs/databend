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
use databend_common_pipeline_sources::EmptySource;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::binder::DataMutationInputType;
use databend_common_sql::executor::physical_plans::MutationSource;
use databend_common_sql::StreamContext;
use databend_common_storages_fuse::operations::MutationAction;
use databend_common_storages_fuse::operations::MutationBlockPruningContext;
use databend_common_storages_fuse::operations::TruncateMode;
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

        let table_clone = table.clone();
        let ctx_clone = self.ctx.clone();
        let filters_clone = mutation_source.filters.clone();
        let input_type_clone = mutation_source.input_type.clone();
        let is_delete = input_type_clone == DataMutationInputType::Delete;

        let cluster = self.ctx.get_cluster();
        let read_partition_columns: Vec<usize> = mutation_source
            .read_partition_columns
            .clone()
            .into_iter()
            .collect();
        self.main_pipeline.set_on_init(move || {
            let ctx = ctx_clone.clone();
            let partitions = Runtime::with_worker_threads(2, None)?.block_on(async move {
                let partitions = if let Some(snapshot) = table_clone.read_table_snapshot().await? {
                    ctx_clone.set_table_snapshot(snapshot.clone());
                    let is_lazy =
                        !cluster.is_empty() && snapshot.segments.len() >= cluster.nodes.len();
                    ctx_clone.set_lazy_mutation_delete(is_lazy);
                    let partitions = table_clone
                        .mutation_read_partitions(
                            ctx_clone.clone(),
                            snapshot.clone(),
                            read_partition_columns.clone(),
                            filters_clone.clone(),
                            is_lazy,
                            is_delete,
                        )
                        .await?;

                    let partitions = if partitions.partitions_type() == PartInfoType::LazyLevel
                        && input_type_clone == DataMutationInputType::Delete
                    {
                        let projection = Projection::Columns(read_partition_columns.clone());
                        let mut segment_locations = Vec::with_capacity(partitions.partitions.len());
                        for part in &partitions.partitions {
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

                        let (partitions, _) = table_clone
                            .do_mutation_block_pruning(
                                ctx_clone,
                                filters_clone,
                                projection,
                                prune_ctx,
                                true,
                                true,
                            )
                            .await?;
                        partitions
                    } else {
                        partitions
                    };

                    Some(partitions)
                } else {
                    None
                };
                Ok(partitions)
            })?;
            ctx.set_partitions(partitions.unwrap())?;
            Ok(())
        });

        let filters = mutation_source.filters.clone();

        if is_delete && filters.is_none() {
            if let Some(snapshot) = self.ctx.get_table_snapshot() {
                // Delete the whole table, just a truncate
                table.build_truncate_pipeline(
                    self.ctx.clone(),
                    &mut self.main_pipeline,
                    TruncateMode::Delete,
                    snapshot,
                )?;
                return Ok(());
            } else {
                return self.main_pipeline.add_source(EmptySource::create, 1);
            }
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
                true,
            )?;
            self.main_pipeline
                .add_transformer(|| TransformAddStreamColumns::new(stream_ctx.clone()));
        }

        Ok(())
    }
}
