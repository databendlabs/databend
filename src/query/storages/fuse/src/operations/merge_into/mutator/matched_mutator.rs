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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use ahash::AHashMap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::buffer::Buffer;
use common_base::base::tokio::sync::Semaphore;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_catalog::plan::split_prefix;
use common_catalog::plan::split_row_id;
use common_catalog::plan::Projection;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::NumberColumn;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::TableSchemaRef;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::evaluator::BlockOperator;
use common_sql::executor::MatchExpr;
use common_storage::metrics::merge_into::metrics_inc_merge_into_replace_blocks_counter;
use itertools::Itertools;
use log::info;
use opendal::Operator;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;

use crate::io::write_data;
use crate::io::BlockBuilder;
use crate::io::BlockReader;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::io::ReadSettings;
use crate::io::WriteSettings;
use crate::operations::acquire_task_permit;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::merge_into::mutator::SplitByExprMutator;
use crate::operations::mutation::BlockIndex;
use crate::operations::mutation::SegmentIndex;
use crate::operations::read_block;
use crate::operations::BlockMetaIndex;

enum MutationKind {
    Update(UpdateDataBlockMutation),
    Delete(DeleteDataBlockMutation),
}

struct UpdateDataBlockMutation {
    op: BlockOperator,
    split_mutator: SplitByExprMutator,
}

struct DeleteDataBlockMutation {
    split_mutator: SplitByExprMutator,
}

struct AggregationContext {
    row_id_idx: usize,
    ops: Vec<MutationKind>,
    func_ctx: FunctionContext,
    data_accessor: Operator,
    write_settings: WriteSettings,
    read_settings: ReadSettings,
    block_builder: BlockBuilder,
    block_reader: Arc<BlockReader>,
}

type RemainMap = HashMap<usize, (Vec<usize>, Vec<usize>)>;
type UpdatedMap = HashMap<usize, (Vec<usize>, DataBlock)>;

pub struct MatchedAggregator {
    io_request_semaphore: Arc<Semaphore>,
    segment_reader: CompactSegmentInfoReader,
    segment_locations: AHashMap<SegmentIndex, Location>,
    // (update_idx,(updated_columns,remain_columns))
    remain_projections_map: Arc<RemainMap>,
    // block_mutator, store new data after update,
    // BlockMetaIndex => (update_idx,(block_offsets,new_data))
    // todo: (JackTan25) need to add precomputed expr for update to optimize
    updatede_block: HashMap<u64, UpdatedMap>,
    // store the row_id which is deleted/updated
    block_mutation_row_offset: HashMap<u64, HashSet<usize>>,
    aggregation_ctx: Arc<AggregationContext>,
}

impl MatchedAggregator {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<dyn TableContext>,
        row_id_idx: usize,
        matched: MatchExpr,
        target_table_schema: TableSchemaRef,
        input_schema: DataSchemaRef,
        data_accessor: Operator,
        write_settings: WriteSettings,
        read_settings: ReadSettings,
        block_builder: BlockBuilder,
        io_request_semaphore: Arc<Semaphore>,
        segment_locations: Vec<(SegmentIndex, Location)>,
    ) -> Result<Self> {
        let segment_reader =
            MetaReaders::segment_info_reader(data_accessor.clone(), target_table_schema.clone());
        let block_reader = {
            let projection =
                Projection::Columns((0..target_table_schema.num_fields()).collect_vec());
            BlockReader::create(
                data_accessor.clone(),
                target_table_schema.clone(),
                projection,
                ctx.clone(),
                false,
            )
        }?;

        let mut ops = Vec::<MutationKind>::new();
        let mut remain_projections_map = HashMap::new();
        for (expr_idx, item) in matched.iter().enumerate() {
            // delete
            if item.1.is_none() {
                let filter = item.0.as_ref().map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS));
                ops.push(MutationKind::Delete(DeleteDataBlockMutation {
                    split_mutator: SplitByExprMutator::create(
                        filter.clone(),
                        ctx.get_function_context()?,
                    ),
                }))
            } else {
                let update_lists = item.1.as_ref().unwrap();
                let mut set = HashSet::new();
                let mut remain_projections = Vec::new();
                let mut collected_projections = Vec::new();
                let input_len = input_schema.num_fields();
                let eval_projections: HashSet<usize> =
                    (input_len..update_lists.len() + input_len).collect();

                for (idx, _) in update_lists {
                    collected_projections.push(*idx);
                    set.insert(idx);
                }

                for idx in 0..target_table_schema.num_fields() {
                    if !set.contains(&idx) {
                        remain_projections.push(idx);
                    }
                }

                let exprs: Vec<Expr> = update_lists
                    .iter()
                    .map(|item| item.1.as_expr(&BUILTIN_FUNCTIONS))
                    .collect();

                remain_projections_map
                    .insert(expr_idx, (collected_projections, remain_projections));
                let filter = item
                    .0
                    .as_ref()
                    .map(|condition| condition.as_expr(&BUILTIN_FUNCTIONS));

                ops.push(MutationKind::Update(UpdateDataBlockMutation {
                    op: BlockOperator::Map {
                        exprs,
                        projections: Some(eval_projections),
                    },
                    split_mutator: SplitByExprMutator::create(filter, ctx.get_function_context()?),
                }))
            }
        }

        Ok(Self {
            aggregation_ctx: Arc::new(AggregationContext {
                row_id_idx,
                ops,
                func_ctx: ctx.get_function_context()?,
                write_settings,
                read_settings,
                data_accessor,
                block_builder,
                block_reader,
            }),
            io_request_semaphore,
            segment_reader,
            updatede_block: HashMap::new(),
            block_mutation_row_offset: HashMap::new(),
            remain_projections_map: Arc::new(remain_projections_map),
            segment_locations: AHashMap::from_iter(segment_locations.into_iter()),
        })
    }

    #[async_backtrace::framed]
    pub async fn accumulate(&mut self, data_block: DataBlock) -> Result<()> {
        if data_block.is_empty() {
            return Ok(());
        }
        let mut current_block = data_block;
        for (expr_idx, op) in self.aggregation_ctx.ops.iter().enumerate() {
            match op {
                MutationKind::Update(update_mutation) => {
                    let (satisfied_block, unsatisfied_block) =
                        update_mutation.split_mutator.split_by_expr(current_block)?;

                    if !satisfied_block.is_empty() {
                        let row_ids =
                            get_row_id(&satisfied_block, self.aggregation_ctx.row_id_idx)?;
                        let updated_block = update_mutation
                            .op
                            .execute(&self.aggregation_ctx.func_ctx, satisfied_block)?;
                        // record the modified block offsets
                        for (idx, row_id) in row_ids.iter().enumerate() {
                            let (prefix, offset) = split_row_id(*row_id);

                            self.updatede_block
                                .entry(prefix)
                                .and_modify(|v| {
                                    let (mut old_offsets, old_block) = v.remove(&expr_idx).unwrap();
                                    old_offsets.push(offset as usize);
                                    v.insert(
                                        expr_idx,
                                        (
                                            old_offsets,
                                            DataBlock::concat(&[
                                                old_block,
                                                updated_block.slice(idx..idx + 1),
                                            ])
                                            .unwrap(),
                                        ),
                                    );
                                })
                                .or_insert(|| -> HashMap<usize, (Vec<usize>, DataBlock)> {
                                    let mut m = HashMap::new();
                                    m.insert(
                                        expr_idx,
                                        (vec![offset as usize], updated_block.slice(idx..idx + 1)),
                                    );
                                    m
                                }());

                            self.block_mutation_row_offset
                                .entry(prefix)
                                .and_modify(|v| {
                                    v.insert(offset as usize);
                                })
                                .or_insert(vec![offset as usize].into_iter().collect());
                        }
                    }

                    if unsatisfied_block.is_empty() {
                        return Ok(());
                    }

                    current_block = unsatisfied_block;
                }

                MutationKind::Delete(delete_mutation) => {
                    let (satisfied_block, unsatisfied_block) =
                        delete_mutation.split_mutator.split_by_expr(current_block)?;
                    if !satisfied_block.is_empty() {
                        let row_ids =
                            get_row_id(&satisfied_block, self.aggregation_ctx.row_id_idx)?;

                        // record the modified block offsets
                        for row_id in row_ids {
                            let (prefix, offset) = split_row_id(row_id);

                            self.block_mutation_row_offset
                                .entry(prefix)
                                .and_modify(|v| {
                                    v.insert(offset as usize);
                                })
                                .or_insert(vec![offset as usize].into_iter().collect());
                        }
                    }

                    if unsatisfied_block.is_empty() {
                        return Ok(());
                    }

                    current_block = unsatisfied_block;
                }
            }
        }
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn apply(&mut self) -> Result<Option<MutationLogs>> {
        // 1.get modified segments
        let mut segment_infos = HashMap::<SegmentIndex, SegmentInfo>::new();
        for prefix in self.block_mutation_row_offset.keys() {
            let (segment_idx, _) = split_prefix(*prefix);
            let segment_idx = segment_idx as usize;

            if let Entry::Vacant(e) = segment_infos.entry(segment_idx) {
                let (path, ver) = self.segment_locations.get(&segment_idx).ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "unexpected, segment (idx {}) not found, during applying mutation log",
                        segment_idx
                    ))
                })?;

                let load_param = LoadParams {
                    location: path.clone(),
                    len_hint: None,
                    ver: *ver,
                    put_cache: true,
                };

                let compact_segment_info = self.segment_reader.read(&load_param).await?;
                let segment_info: SegmentInfo = compact_segment_info.try_into()?;
                e.insert(segment_info);
            } else {
                continue;
            }
        }

        let io_runtime = GlobalIORuntime::instance();
        let mut mutation_log_handlers = Vec::with_capacity(self.block_mutation_row_offset.len());

        for item in &self.block_mutation_row_offset {
            let (segment_idx, block_idx) = split_prefix(*item.0);
            let segment_idx = segment_idx as usize;
            let block_idx = block_idx as usize;
            let permit = acquire_task_permit(self.io_request_semaphore.clone()).await?;
            let aggregation_ctx = self.aggregation_ctx.clone();
            let block_meta = segment_infos.get(&segment_idx).unwrap().blocks[block_idx].clone();
            let remain_projections = self.remain_projections_map.clone();
            let updated_block_info = self.updatede_block.remove(item.0);
            let modified_offsets = item.1.clone();
            let handle = io_runtime.spawn(async_backtrace::location!().frame({
                async move {
                    let mutation_log_entry = aggregation_ctx
                        .apply_update_and_deletion_to_data_block(
                            segment_idx,
                            block_idx,
                            &block_meta,
                            updated_block_info,
                            remain_projections,
                            modified_offsets,
                        )
                        .await?;

                    drop(permit);
                    Ok::<_, ErrorCode>(mutation_log_entry)
                }
            }));
            mutation_log_handlers.push(handle);
        }

        let log_entries = futures::future::try_join_all(mutation_log_handlers)
            .await
            .map_err(|e| {
                ErrorCode::Internal("unexpected, failed to join apply-deletion tasks.")
                    .add_message_back(e.to_string())
            })?;
        let mut mutation_logs = Vec::new();
        for maybe_log_entry in log_entries {
            if let Some(segment_mutation_log) = maybe_log_entry? {
                mutation_logs.push(segment_mutation_log);
            }
        }
        Ok(Some(MutationLogs {
            entries: mutation_logs,
        }))
    }
}

impl AggregationContext {
    #[async_backtrace::framed]
    async fn apply_update_and_deletion_to_data_block(
        &self,
        segment_idx: SegmentIndex,
        block_idx: BlockIndex,
        block_meta: &BlockMeta,
        block_updated: Option<UpdatedMap>,
        remain_projections: Arc<RemainMap>,
        modified_offsets: HashSet<usize>,
    ) -> Result<Option<MutationLogEntry>> {
        info!(
            "apply update and delete to segment idx {}, block idx {}",
            segment_idx, block_idx,
        );

        let origin_data_block = read_block(
            self.write_settings.storage_format,
            &self.block_reader,
            block_meta,
            &self.read_settings,
        )
        .await?;
        let mut res_blocks = Vec::new();
        let get_block = |data_block: &DataBlock, offsets: &Vec<usize>| -> Result<DataBlock> {
            let mut blocks = Vec::with_capacity(offsets.len());
            for offset in offsets {
                blocks.push(data_block.slice(*offset..*offset + 1));
            }
            DataBlock::concat(&blocks)
        };
        // remain columns for update
        if block_updated.is_some() {
            for (expr_idx, (offsets, mut updated_block)) in block_updated.unwrap() {
                let remain = remain_projections.get(&expr_idx).unwrap().1.clone();
                let mut collected = remain_projections.get(&expr_idx).unwrap().0.clone();
                collected.extend(remain.clone());
                let block_operator = BlockOperator::Project {
                    projection: remain.clone(),
                };
                let remain_block = block_operator
                    .execute(&self.func_ctx, get_block(&origin_data_block, &offsets)?)?;
                for col in remain_block.columns() {
                    updated_block.add_column(col.clone());
                }

                // sort columns
                let mut projection = (0..collected.len()).collect::<Vec<_>>();
                projection.sort_by_key(|&i| collected[i]);
                let sort_operator = BlockOperator::Project { projection };
                let res_block = sort_operator.execute(&self.func_ctx, updated_block)?;
                if !res_block.is_empty() {
                    res_blocks.push(res_block);
                }
            }
        }

        // apply delete
        let mut bitmap = MutableBitmap::new();
        for row in 0..origin_data_block.num_rows() {
            if modified_offsets.contains(&row) {
                bitmap.push(false);
            } else {
                bitmap.push(true);
            }
        }
        let res_block = origin_data_block.filter_with_bitmap(&bitmap.into())?;
        if !res_block.is_empty() {
            res_blocks.push(res_block);
        }

        if res_blocks.is_empty() {
            return Ok(Some(MutationLogEntry::DeletedBlock {
                index: BlockMetaIndex {
                    segment_idx,
                    block_idx,
                },
            }));
        }
        let res_block = DataBlock::concat(&res_blocks)?;

        // serialization and compression is cpu intensive, send them to dedicated thread pool
        // and wait (asyncly, which will NOT block the executor thread)
        let block_builder = self.block_builder.clone();
        let origin_stats = block_meta.cluster_stats.clone();
        let serialized = GlobalIORuntime::instance()
            .spawn_blocking(move || {
                block_builder.build(res_block, |block, generator| {
                    info!("serialize block before get cluster_stats:\n {:?}", block);
                    let cluster_stats =
                        generator.gen_with_origin_stats(&block, origin_stats.clone())?;
                    info!("serialize block after get cluster_stats:\n {:?}", block);
                    Ok((cluster_stats, block))
                })
            })
            .await?;

        // persistent data
        let new_block_meta = serialized.block_meta;
        let new_block_location = new_block_meta.location.0.clone();
        let new_block_raw_data = serialized.block_raw_data;
        let data_accessor = self.data_accessor.clone();
        write_data(new_block_raw_data, &data_accessor, &new_block_location).await?;

        metrics_inc_merge_into_replace_blocks_counter(new_block_meta.row_count as u32);
        // generate log
        let mutation = MutationLogEntry::ReplacedBlock {
            index: BlockMetaIndex {
                segment_idx,
                block_idx,
            },
            block_meta: Arc::new(new_block_meta),
        };

        Ok(Some(mutation))
    }
}

fn get_row_id(data_block: &DataBlock, row_id_idx: usize) -> Result<Buffer<u64>> {
    let row_id_col = data_block.get_by_offset(row_id_idx);
    match row_id_col.value.as_column() {
        Some(Column::Nullable(boxed)) => match &boxed.column {
            Column::Number(NumberColumn::UInt64(data)) => Ok(data.clone()),
            _ => Err(ErrorCode::BadArguments("row id is not uint64")),
        },
        _ => Err(ErrorCode::BadArguments("row id is not uint64")),
    }
}
