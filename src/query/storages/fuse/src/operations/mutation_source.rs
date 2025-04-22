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

use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PruningStatistics;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::FieldIndex;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::Pipeline;
use databend_common_sql::evaluator::BlockOperator;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_pruner::RangePruner;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::operations::mutation::Mutation;
use crate::operations::mutation::MutationAction;
use crate::operations::mutation::MutationPartInfo;
use crate::operations::mutation::MutationSource;
use crate::pruning::create_segment_location_vector;
use crate::pruning::FusePruner;
use crate::FuseLazyPartInfo;
use crate::FuseTable;
use crate::SegmentLocation;

impl FuseTable {
    #[async_backtrace::framed]
    pub fn add_mutation_source(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: Option<RemoteExpr<String>>,
        col_indices: Vec<usize>,
        pipeline: &mut Pipeline,
        mutation_action: MutationAction,
    ) -> Result<()> {
        let all_column_indices = self.all_column_indices();
        let col_indices =
            if matches!(mutation_action, MutationAction::Deletion) || !col_indices.is_empty() {
                col_indices
            } else {
                all_column_indices.clone()
            };
        let projection = Projection::Columns(col_indices.clone());
        let update_stream_columns = self.change_tracking_enabled();
        let block_reader =
            self.create_block_reader(ctx.clone(), projection, false, update_stream_columns, false)?;

        let schema = block_reader.schema().as_ref().clone();
        let filter_expr = Arc::new(filter.map(|v| {
            v.as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| schema.index_of(name).unwrap())
        }));

        let num_column_indices = self.schema_with_stream().fields().len();
        let remain_column_indices: Vec<usize> = all_column_indices
            .into_iter()
            .filter(|index| !col_indices.contains(index))
            .collect();
        let mut source_col_indices = col_indices;
        if matches!(mutation_action, MutationAction::Deletion) && update_stream_columns
            || matches!(mutation_action, MutationAction::Update) && filter_expr.is_some()
        {
            source_col_indices.push(num_column_indices);
        }
        let remain_reader = if remain_column_indices.is_empty() {
            Arc::new(None)
        } else {
            source_col_indices.extend_from_slice(&remain_column_indices);
            Arc::new(Some(
                (*self.create_block_reader(
                    ctx.clone(),
                    Projection::Columns(remain_column_indices),
                    false,
                    update_stream_columns,
                    false,
                )?)
                .clone(),
            ))
        };

        // Resort the block.
        let mut projection = (0..source_col_indices.len()).collect::<Vec<_>>();
        projection.sort_by_key(|&i| source_col_indices[i]);
        let ops = vec![BlockOperator::Project { projection }];

        let max_threads = (ctx.get_settings().get_max_threads()? as usize)
            .min(ctx.partition_num())
            .max(1);
        // Add source pipe.
        pipeline.add_source(
            |output| {
                MutationSource::try_create(
                    ctx.clone(),
                    mutation_action.clone(),
                    output,
                    filter_expr.clone(),
                    block_reader.clone(),
                    remain_reader.clone(),
                    ops.clone(),
                    self.storage_format,
                )
            },
            max_threads,
        )?;
        Ok(())
    }

    pub async fn mutation_read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        snapshot: Arc<TableSnapshot>,
        col_indices: Vec<FieldIndex>,
        filters: Option<Filters>,
        is_lazy: bool,
        is_delete: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        if is_lazy {
            let segment_len = snapshot.segments.len();
            let mut segments = Vec::with_capacity(snapshot.segments.len());
            for (idx, segment_location) in snapshot.segments.iter().enumerate() {
                segments.push(FuseLazyPartInfo::create(idx, segment_location.clone()));
            }
            Ok((
                PartStatistics::new_estimated(
                    None,
                    snapshot.summary.row_count as usize,
                    snapshot.summary.compressed_byte_size as usize,
                    segment_len,
                    segment_len,
                ),
                Partitions::create(PartitionsShuffleKind::Mod, segments),
            ))
        } else {
            let projection = Projection::Columns(col_indices.clone());
            let prune_ctx = MutationBlockPruningContext {
                segment_locations: create_segment_location_vector(snapshot.segments.clone(), None),
                block_count: Some(snapshot.summary.block_count as usize),
            };
            self.do_mutation_block_pruning(ctx, filters, projection, prune_ctx, is_delete)
                .await
        }
    }

    #[async_backtrace::framed]
    #[allow(clippy::too_many_arguments)]
    pub async fn do_mutation_block_pruning(
        &self,
        ctx: Arc<dyn TableContext>,
        filters: Option<Filters>,
        projection: Projection,
        prune_ctx: MutationBlockPruningContext,
        is_delete: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        let MutationBlockPruningContext {
            segment_locations,
            block_count,
        } = prune_ctx;
        let push_down = Some(PushDownInfo {
            projection: Some(projection),
            filters: filters.clone(),
            ..PushDownInfo::default()
        });

        let mut pruner = FusePruner::create(
            &ctx,
            self.operator.clone(),
            self.schema_with_stream(),
            &push_down,
            self.bloom_index_cols(),
            None,
        )?;

        if let Some(inverse) = filters.map(|f| f.inverted_filter) {
            // now the `block_metas` refers to the blocks that need to be deleted completely or partially.
            //
            // let's try pruning the blocks further to get the blocks that need to be deleted completely, so that
            // later during mutation, we need not load the data of these blocks:
            //
            // 1. invert the filter expression
            // 2. apply the inverse filter expression to the block metas, utilizing range index
            //  - for those blocks that need to be deleted completely, they will be filtered out.
            //  - for those blocks that need to be deleted partially, they will NOT be filtered out.
            //
            let inverse = inverse.as_expr(&BUILTIN_FUNCTIONS);
            let func_ctx = ctx.get_function_context()?;
            let range_index = RangeIndex::try_create(
                func_ctx,
                &inverse,
                self.table_info.schema(),
                StatisticsOfColumns::default(), // TODO default values
            )?;
            pruner.set_inverse_range_index(range_index);
        }

        let block_metas = if is_delete {
            pruner.delete_pruning(segment_locations).await?
        } else {
            pruner.read_pruning(segment_locations).await?
        };

        let mut whole_block_deletions = std::collections::HashSet::new();

        if !block_metas.is_empty() {
            if let Some(range_index) = pruner.get_inverse_range_index() {
                for (block_meta_idx, block_meta) in &block_metas {
                    if !range_index.should_keep(&block_meta.as_ref().col_stats, None) {
                        // this block should be deleted completely
                        whole_block_deletions
                            .insert((block_meta_idx.segment_idx, block_meta_idx.block_idx));
                    }
                }
            }
        }

        let range_block_metas = block_metas
            .clone()
            .into_iter()
            .map(|(block_meta_index, block_meta)| (Some(block_meta_index), block_meta))
            .collect::<Vec<_>>();

        let (statistics, inner_parts) = self.read_partitions_with_metas(
            ctx.clone(),
            self.schema_with_stream(),
            None,
            &range_block_metas,
            block_count.unwrap_or_default(),
            PruningStatistics::default(),
        )?;

        let mut parts = Partitions::create(
            PartitionsShuffleKind::Mod,
            block_metas
                .into_iter()
                .zip(inner_parts.partitions.into_iter())
                .map(|((index, block_meta), inner_part)| {
                    let cluster_stats = block_meta.cluster_stats.clone();
                    let key = (index.segment_idx, index.block_idx);
                    let whole_block_mutation = whole_block_deletions.contains(&key);
                    let part_info_ptr: PartInfoPtr =
                        Arc::new(Box::new(Mutation::MutationPartInfo(MutationPartInfo {
                            index,
                            cluster_stats,
                            inner_part,
                            whole_block_mutation,
                        })));
                    part_info_ptr
                })
                .collect(),
        );

        let mut part_num = parts.len();
        let mut num_whole_block_mutation = whole_block_deletions.len();
        let segment_num = pruner.deleted_segments.len();
        // now try to add deleted_segment
        for deleted_segment in pruner.deleted_segments {
            part_num += deleted_segment.summary.block_count as usize;
            num_whole_block_mutation += deleted_segment.summary.block_count as usize;
            parts
                .partitions
                .push(Arc::new(Box::new(Mutation::MutationDeletedSegment(
                    deleted_segment,
                ))));
        }

        if let Some(block_count) = block_count {
            metrics_inc_deletion_block_range_pruned_nums(block_count as u64 - part_num as u64);
        }
        metrics_inc_deletion_block_range_pruned_whole_block_nums(num_whole_block_mutation as u64);
        metrics_inc_deletion_segment_range_purned_whole_segment_nums(segment_num as u64);
        if is_delete {
            log::info!(
                "delete pruning done, number of whole block deletion detected in pruning phase: {}",
                num_whole_block_mutation
            );
        }
        Ok((statistics, parts))
    }
}

pub struct MutationBlockPruningContext {
    pub segment_locations: Vec<SegmentLocation>,
    pub block_count: Option<usize>,
}
