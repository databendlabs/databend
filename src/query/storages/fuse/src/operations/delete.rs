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

use common_base::base::ProgressValues;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::Projection;
use common_catalog::plan::PruningStatistics;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::DeletionFilters;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::ComputedExpr;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::Evaluator;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;
use common_expression::TableDataType;
use common_expression::TableSchema;
use common_expression::Value;
use common_expression::ROW_ID_COL_NAME;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::evaluator::BlockOperator;
use storages_common_index::RangeIndex;
use storages_common_pruner::RangePruner;
use storages_common_table_meta::meta::StatisticsOfColumns;
use storages_common_table_meta::meta::TableSnapshot;
use tracing::info;

use crate::metrics::metrics_inc_deletion_block_range_pruned_nums;
use crate::metrics::metrics_inc_deletion_block_range_pruned_whole_block_nums;
use crate::operations::mutation::MutationAction;
use crate::operations::mutation::MutationPartInfo;
use crate::operations::mutation::MutationSource;
use crate::operations::mutation::SerializeDataTransform;
use crate::pipelines::Pipeline;
use crate::pruning::create_segment_location_vector;
use crate::pruning::FusePruner;
use crate::FuseTable;

pub struct MutationTaskInfo {
    pub(crate) total_tasks: usize,
    pub(crate) num_whole_block_mutation: usize,
}

impl FuseTable {
    /// The flow of Pipeline is as follows:
    /// +--------------+      +----------------------+
    /// |MutationSource| ---> |SerializeDataTransform|   ------
    /// +--------------+      +----------------------+         |      +-----------------------+      +----------+
    /// |     ...      | ---> |          ...         |   ...   | ---> |TableMutationAggregator| ---> |CommitSink|
    /// +--------------+      +----------------------+         |      +-----------------------+      +----------+
    /// |MutationSource| ---> |SerializeDataTransform|   ------
    /// +--------------+      +----------------------+
    #[async_backtrace::framed]
    pub async fn do_delete(
        &self,
        ctx: Arc<dyn TableContext>,
        filters: Option<DeletionFilters>,
        col_indices: Vec<usize>,
        query_row_id_col: bool,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let snapshot_opt = self.read_table_snapshot().await?;

        // check if table is empty
        let snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no deletion
            return Ok(());
        };

        if snapshot.summary.row_count == 0 {
            // empty snapshot, no deletion
            return Ok(());
        }

        // check if unconditional deletion
        let deletion_filters = match filters {
            None => {
                let progress_values = ProgressValues {
                    rows: snapshot.summary.row_count as usize,
                    bytes: snapshot.summary.uncompressed_byte_size as usize,
                };
                ctx.get_write_progress().incr(&progress_values);
                // deleting the whole table... just a truncate
                let purge = false;
                return self.do_truncate(ctx.clone(), purge).await;
            }
            Some(filters) => filters,
        };

        if col_indices.is_empty() && !query_row_id_col {
            // here the situation: filter_expr is not null, but col_indices in empty, which
            // indicates the expr being evaluated is unrelated to the value of rows:
            //   e.g.
            //       `delete from t where 1 = 1`, `delete from t where now()`,
            //       or `delete from t where RANDOM()::INT::BOOLEAN`
            // if the `filter_expr` is of "constant" nullary :
            //   for the whole block, whether all of the rows should be kept or dropped,
            //   we can just return from here, without accessing the block data
            if self.try_eval_const(ctx.clone(), &self.schema(), &deletion_filters.filter)? {
                let progress_values = ProgressValues {
                    rows: snapshot.summary.row_count as usize,
                    bytes: snapshot.summary.uncompressed_byte_size as usize,
                };
                ctx.get_write_progress().incr(&progress_values);

                // deleting the whole table... just a truncate
                let purge = false;
                return self.do_truncate(ctx.clone(), purge).await;
            }
            // do nothing.
            return Ok(());
        }

        self.try_add_deletion_source(
            ctx.clone(),
            deletion_filters,
            col_indices,
            &snapshot,
            query_row_id_col,
            pipeline,
        )
        .await?;
        if pipeline.is_empty() {
            return Ok(());
        }

        let cluster_stats_gen =
            self.get_cluster_stats_gen(ctx.clone(), 0, self.get_block_thresholds())?;
        pipeline.add_transform(|input, output| {
            SerializeDataTransform::try_create(
                ctx.clone(),
                input,
                output,
                self,
                cluster_stats_gen.clone(),
            )
        })?;

        self.chain_mutation_pipes(&ctx, pipeline, snapshot)
    }

    pub fn try_eval_const(
        &self,
        ctx: Arc<dyn TableContext>,
        schema: &TableSchema,
        filter: &RemoteExpr<String>,
    ) -> Result<bool> {
        let dummy_field = DataField::new("dummy", DataType::Null);
        let _dummy_schema = Arc::new(DataSchema::new(vec![dummy_field]));
        let dummy_value = Value::Column(Column::Null { len: 1 });
        let dummy_block = DataBlock::new(vec![BlockEntry::new(DataType::Null, dummy_value)], 1);

        let filter = filter
            .as_expr(&BUILTIN_FUNCTIONS)
            .project_column_ref(|name| schema.index_of(name).unwrap());

        assert_eq!(filter.data_type(), &DataType::Boolean);

        let func_ctx = ctx.get_function_context()?;
        let evaluator = Evaluator::new(&dummy_block, &func_ctx, &BUILTIN_FUNCTIONS);
        let predicates = evaluator
            .run(&filter)
            .map_err(|e| e.add_message("eval try eval const failed:"))?
            .try_downcast::<BooleanType>()
            .unwrap();

        Ok(match &predicates {
            Value::Scalar(v) => *v,
            Value::Column(bitmap) => bitmap.unset_bits() == 0,
        })
    }

    #[async_backtrace::framed]
    async fn try_add_deletion_source(
        &self,
        ctx: Arc<dyn TableContext>,
        deletion_filters: DeletionFilters,
        col_indices: Vec<usize>,
        base_snapshot: &TableSnapshot,
        query_row_id_col: bool,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let projection = Projection::Columns(col_indices.clone());

        {
            let status = "delete: begin pruning".to_string();
            ctx.set_status_info(&status);
            info!(status);
        }

        let filter = deletion_filters.filter.clone();

        let MutationTaskInfo {
            total_tasks,
            num_whole_block_mutation,
        } = self
            .mutation_block_pruning(
                ctx.clone(),
                Some(deletion_filters.filter),
                Some(deletion_filters.inverted_filter),
                projection.clone(),
                base_snapshot,
                true,
            )
            .await?;

        if total_tasks == 0 {
            return Ok(());
        }

        // Status.
        {
            let status = format!(
                "delete: begin to run delete tasks, total tasks: {},  number of whole block deletion detected in pruning phase: {}",
                total_tasks, num_whole_block_mutation
            );
            ctx.set_status_info(&status);
            info!(status);
        }

        let block_reader = self.create_block_reader(projection, false, ctx.clone())?;
        let mut schema = block_reader.schema().as_ref().clone();
        if query_row_id_col {
            schema.add_internal_field(
                ROW_ID_COL_NAME,
                TableDataType::Number(NumberDataType::UInt64),
                1,
            );
        }

        let filter_expr = Arc::new(Some(
            filter
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| schema.index_of(name).unwrap()),
        ));

        let all_column_indices = self.all_column_indices();
        let remain_column_indices: Vec<usize> = all_column_indices
            .into_iter()
            .filter(|index| !col_indices.contains(index))
            .collect();
        let mut source_col_indices = col_indices;
        let remain_reader = if remain_column_indices.is_empty() {
            Arc::new(None)
        } else {
            source_col_indices.extend_from_slice(&remain_column_indices);
            Arc::new(Some(
                (*self.create_block_reader(
                    Projection::Columns(remain_column_indices),
                    false,
                    ctx.clone(),
                )?)
                .clone(),
            ))
        };

        // resort the block.
        let mut projection = (0..source_col_indices.len()).collect::<Vec<_>>();
        projection.sort_by_key(|&i| source_col_indices[i]);
        let ops = vec![BlockOperator::Project { projection }];

        let max_threads =
            std::cmp::min(ctx.get_settings().get_max_threads()? as usize, total_tasks);
        // Add source pipe.
        pipeline.add_source(
            |output| {
                MutationSource::try_create(
                    ctx.clone(),
                    MutationAction::Deletion,
                    output,
                    filter_expr.clone(),
                    block_reader.clone(),
                    remain_reader.clone(),
                    ops.clone(),
                    self.storage_format,
                    query_row_id_col,
                )
            },
            max_threads,
        )?;
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn mutation_block_pruning(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: Option<RemoteExpr<String>>,
        inverted_filter: Option<RemoteExpr<String>>,
        projection: Projection,
        base_snapshot: &TableSnapshot,
        with_origin: bool,
    ) -> Result<MutationTaskInfo> {
        let push_down = Some(PushDownInfo {
            projection: Some(projection),
            filter: filter.clone(),
            ..PushDownInfo::default()
        });

        let segment_locations = base_snapshot.segments.clone();
        let pruner = FusePruner::create(
            &ctx,
            self.operator.clone(),
            self.table_info.schema(),
            &push_down,
        )?;

        let segment_locations = create_segment_location_vector(segment_locations, None);
        let block_metas = pruner.pruning(segment_locations).await?;

        let mut whole_block_deletions = std::collections::HashSet::new();

        if !block_metas.is_empty() {
            if let Some(inverse) = inverted_filter {
                // now the `block_metas` refers to the blocks that need to be deleted completely or partially.
                //
                // let's try pruning the blocks further to get the blocks that need to be deleted completely, so that
                // later during mutation, we need not to load the data of these blocks:
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

        let (_, inner_parts) = self.read_partitions_with_metas(
            self.table_info.schema(),
            None,
            &range_block_metas,
            None,
            base_snapshot.summary.block_count as usize,
            PruningStatistics::default(),
        )?;

        let parts = Partitions::create_nolazy(
            PartitionsShuffleKind::Mod,
            block_metas
                .into_iter()
                .zip(inner_parts.partitions.into_iter())
                .map(|((block_meta_index, block_meta), c)| {
                    let cluster_stats = if with_origin {
                        block_meta.cluster_stats.clone()
                    } else {
                        None
                    };
                    let key = (block_meta_index.segment_idx, block_meta_index.block_idx);
                    let whole_block_deletion = whole_block_deletions.contains(&key);
                    MutationPartInfo::create(
                        block_meta_index,
                        cluster_stats,
                        c,
                        whole_block_deletion,
                    )
                })
                .collect(),
        );

        let part_num = parts.len();
        ctx.set_partitions(parts)?;

        let num_whole_block_mutation = whole_block_deletions.len();

        let block_nums = base_snapshot.summary.block_count;
        metrics_inc_deletion_block_range_pruned_nums(block_nums - part_num as u64);
        metrics_inc_deletion_block_range_pruned_whole_block_nums(num_whole_block_mutation as u64);

        Ok(MutationTaskInfo {
            total_tasks: part_num,
            num_whole_block_mutation,
        })
    }

    pub fn all_column_indices(&self) -> Vec<FieldIndex> {
        self.table_info
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !matches!(f.computed_expr(), Some(ComputedExpr::Virtual(_))))
            .map(|(i, _)| i)
            .collect::<Vec<FieldIndex>>()
    }
}
