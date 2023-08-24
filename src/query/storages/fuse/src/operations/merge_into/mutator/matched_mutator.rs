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
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use ahash::AHashMap;
use common_arrow::arrow::buffer::Buffer;
use common_base::base::tokio::sync::Semaphore;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_catalog::plan::split_prefix;
use common_catalog::plan::split_row_id;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::NumberColumn;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::Expr;
use common_expression::FieldIndex;
use common_expression::FunctionContext;
use common_expression::RemoteExpr;
use common_expression::TableSchemaRef;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::evaluator::BlockOperator;
use opendal::Operator;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;

use crate::io::BlockBuilder;
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

pub type MatchExpr = Vec<(Option<RemoteExpr>, Option<Vec<(FieldIndex, RemoteExpr)>>)>;

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
    // used to read remain columns
    target_table_schema: TableSchemaRef,
    row_id_idx: usize,
    ops: Vec<MutationKind>,
    func_ctx: FunctionContext,
    data_accessor: Operator,
    write_settings: WriteSettings,
    read_settings: ReadSettings,
    block_builder: BlockBuilder,
}

pub struct MatchedAggregator {
    io_request_semaphore: Arc<Semaphore>,
    segment_reader: CompactSegmentInfoReader,
    segment_locations: AHashMap<SegmentIndex, Location>,
    // (update_idx,remain_columns)
    remain_projections_map: HashMap<usize, Vec<usize>>,
    // block_mutator, store new data after update,
    // BlockMetaIndex => (update_idx,new_data)
    updatede_block: HashMap<u64, HashMap<usize, DataBlock>>,
    // store the row_id which is deleted/updated
    block_mutation_row_offset: HashMap<u64, Vec<u64>>,
    aggregation_ctx: Arc<AggregationContext>,
}

impl MatchedAggregator {
    pub fn create(
        row_id_idx: usize,
        matched: MatchExpr,
        target_table_schema: TableSchemaRef,
        input_schema: DataSchemaRef,
        func_ctx: FunctionContext,
        data_accessor: Operator,
        write_settings: WriteSettings,
        read_settings: ReadSettings,
        block_builder: BlockBuilder,
        io_request_semaphore: Arc<Semaphore>,
        segment_locations: Vec<(SegmentIndex, Location)>,
    ) -> Result<Self> {
        let segment_reader =
            MetaReaders::segment_info_reader(data_accessor.clone(), target_table_schema.clone());
        let mut ops = Vec::<MutationKind>::new();
        let mut remain_projections_map = HashMap::new();
        for (expr_idx, item) in matched.iter().enumerate() {
            // delete
            if item.1.is_none() {
                let filter = match &item.0 {
                    None => None,
                    Some(expr) => Some(expr.as_expr(&BUILTIN_FUNCTIONS)),
                };
                ops.push(MutationKind::Delete(DeleteDataBlockMutation {
                    split_mutator: SplitByExprMutator::create(filter.clone(), func_ctx.clone()),
                }))
            } else {
                let update_lists = item.1.as_ref().unwrap();
                let mut set = HashSet::new();
                let mut remain_projections = Vec::new();
                let input_len = input_schema.num_fields();
                let eval_projections: HashSet<usize> =
                    (input_len..update_lists.len() + input_len).collect();

                for (idx, _) in update_lists {
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

                remain_projections_map.insert(expr_idx, remain_projections);
                let filter = match &item.0 {
                    None => None,
                    Some(condition) => Some(condition.as_expr(&BUILTIN_FUNCTIONS)),
                };

                ops.push(MutationKind::Update(UpdateDataBlockMutation {
                    op: BlockOperator::Map {
                        exprs,
                        projections: Some(eval_projections),
                    },
                    split_mutator: SplitByExprMutator::create(filter, func_ctx.clone()),
                }))
            }
        }

        Ok(Self {
            aggregation_ctx: Arc::new(AggregationContext {
                target_table_schema,
                row_id_idx,
                ops,
                func_ctx: func_ctx.clone(),
                write_settings,
                read_settings,
                data_accessor,
                block_builder,
            }),
            io_request_semaphore,
            segment_reader,
            updatede_block: HashMap::new(),
            block_mutation_row_offset: HashMap::new(),
            remain_projections_map,
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
                    let (statisfied_block, unstatisfied_block) =
                        update_mutation.split_mutator.split_by_expr(current_block)?;

                    if !statisfied_block.is_empty() {
                        let row_ids =
                            get_row_id(&statisfied_block, self.aggregation_ctx.row_id_idx)?;
                        let updated_block = update_mutation
                            .op
                            .execute(&self.aggregation_ctx.func_ctx, statisfied_block)?;
                        // record the modified block offsets
                        for (idx, row_id) in row_ids.iter().enumerate() {
                            let (prefix, offset) = split_row_id(*row_id);

                            self.updatede_block
                                .entry(prefix)
                                .and_modify(|v| {
                                    let old_block = v.remove(&expr_idx).unwrap();
                                    v.insert(
                                        expr_idx,
                                        DataBlock::concat(&[
                                            old_block,
                                            updated_block.slice(idx..idx + 1),
                                        ])
                                        .unwrap(),
                                    );
                                })
                                .or_insert(|| -> HashMap<usize, DataBlock> {
                                    let mut m = HashMap::new();
                                    m.insert(expr_idx, updated_block.slice(idx..idx + 1));
                                    m
                                }());
                            self.block_mutation_row_offset
                                .entry(prefix)
                                .and_modify(|v| v.push(offset))
                                .or_insert(Vec::new());
                        }
                    }

                    if unstatisfied_block.is_empty() {
                        return Ok(());
                    }

                    current_block = unstatisfied_block;
                }

                MutationKind::Delete(delete_mutation) => {
                    let (statisfied_block, unstatisfied_block) =
                        delete_mutation.split_mutator.split_by_expr(current_block)?;

                    if unstatisfied_block.is_empty() {
                        return Ok(());
                    }

                    current_block = unstatisfied_block;

                    let row_ids = get_row_id(&statisfied_block, self.aggregation_ctx.row_id_idx)?;

                    // record the modified block offsets
                    for row_id in row_ids {
                        let (prefix, offset) = split_row_id(row_id);

                        self.block_mutation_row_offset
                            .entry(prefix)
                            .and_modify(|v| v.push(offset))
                            .or_insert(Vec::new());
                    }
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
            if segment_infos.contains_key(&segment_idx) {
                continue;
            } else {
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
                segment_infos.insert(segment_idx, segment_info);
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
            let block_meta = segment_infos
                .get(&segment_idx)
                .expect(format!("can't get segment info segment_idx: {}", segment_idx).as_str())
                .blocks[block_idx]
                .clone();
            let handle = io_runtime.spawn(async_backtrace::location!().frame({
                async move {
                    let mutation_log_entry = aggregation_ctx
                        .apply_update_and_deletion_to_data_block(
                            segment_idx,
                            block_idx,
                            &block_meta,
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
    ) -> Result<Option<MutationLogEntry>> {
        todo!()
    }
}

fn get_row_id(data_block: &DataBlock, row_id_idx: usize) -> Result<Buffer<u64>> {
    let row_id_col = data_block.get_by_offset(row_id_idx);
    match row_id_col.value.as_column() {
        Some(column) => match column {
            Column::Number(NumberColumn::UInt64(data)) => Ok(data.clone()),
            _ => Err(ErrorCode::BadArguments("row id is not uint64")),
        },
        _ => Err(ErrorCode::BadArguments("row id is not uint64")),
    }
}
