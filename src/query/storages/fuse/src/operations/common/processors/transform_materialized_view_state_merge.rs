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

use bumpalo::Bump;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::ProbeState;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::aggregate::PayloadFlushState;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AggregateFunctionRef;
use databend_common_pipeline_transforms::AccumulatingTransform;

#[derive(Clone)]
pub struct MaterializedViewStateMergePlan {
    pub aggregate_functions: Vec<AggregateFunctionRef>,
    pub group_data_types: Vec<DataType>,
    /// Types of the persisted `[aggregate states..., group keys...]` block.
    pub physical_data_types: Vec<DataType>,
    /// Types of the finalized `[aggregate results..., group keys...]` storage block.
    pub final_data_types: Vec<DataType>,
}

pub struct TransformMaterializedViewStateMerge {
    plan: MaterializedViewStateMergePlan,
    hash_table: AggregateHashTable,
    probe_state: ProbeState,
}

impl TransformMaterializedViewStateMerge {
    pub fn create(plan: MaterializedViewStateMergePlan) -> Self {
        let hash_table = Self::create_hash_table(&plan);
        Self {
            plan,
            hash_table,
            probe_state: ProbeState::default(),
        }
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn create_hash_table(plan: &MaterializedViewStateMergePlan) -> AggregateHashTable {
        AggregateHashTable::new(
            plan.group_data_types.clone(),
            plan.aggregate_functions.clone(),
            HashTableConfig::default(),
            Arc::new(Bump::new()),
        )
    }

    fn normalize_state_columns(&self, block: DataBlock) -> Result<DataBlock> {
        let num_states = self.plan.aggregate_functions.len();
        if block.num_columns() != self.plan.physical_data_types.len() {
            return Err(ErrorCode::Internal(format!(
                "materialized view state block has {} columns, expected {}",
                block.num_columns(),
                self.plan.physical_data_types.len()
            )));
        }

        let rows = block.num_rows();
        let entries = block
            .take_columns()
            .into_iter()
            .enumerate()
            .map(|(offset, entry)| {
                if offset >= num_states {
                    return Ok(entry);
                }
                match &entry {
                    BlockEntry::Const(Scalar::Null, _, _) => Err(ErrorCode::Internal(format!(
                        "materialized view aggregate state column {} contains NULL",
                        offset
                    ))),
                    BlockEntry::Column(Column::Nullable(column))
                        if column.validity.null_count() > 0 =>
                    {
                        Err(ErrorCode::Internal(format!(
                            "materialized view aggregate state column {} contains NULL",
                            offset
                        )))
                    }
                    _ => Ok(entry.remove_nullable()),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(DataBlock::new(entries, rows))
    }

    fn align_final_output_types(&self, block: DataBlock) -> Result<DataBlock> {
        if block.num_columns() != self.plan.final_data_types.len() {
            return Err(ErrorCode::Internal(format!(
                "materialized view final block has {} columns, expected {}",
                block.num_columns(),
                self.plan.final_data_types.len()
            )));
        }

        let rows = block.num_rows();
        let entries = block
            .take_columns()
            .into_iter()
            .zip(self.plan.final_data_types.iter())
            .enumerate()
            .map(|(offset, (entry, target_type))| {
                let actual_type = entry.data_type();
                if &actual_type == target_type {
                    Ok(entry)
                } else if matches!(target_type, DataType::Nullable(_))
                    && actual_type == target_type.remove_nullable()
                {
                    Ok(entry.into_nullable())
                } else {
                    Err(ErrorCode::Internal(format!(
                        "materialized view final column {} expects {}, got {}",
                        offset, target_type, actual_type
                    )))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(DataBlock::new(entries, rows))
    }

    fn flush(&mut self) -> Result<Vec<DataBlock>> {
        if self.hash_table.len() == 0 {
            return Ok(vec![]);
        }
        let mut hash_table =
            std::mem::replace(&mut self.hash_table, Self::create_hash_table(&self.plan));
        self.probe_state = ProbeState::default();

        let mut flush_state = PayloadFlushState::default();
        let mut blocks = Vec::new();
        while hash_table.merge_result(&mut flush_state)? {
            let mut entries = flush_state.take_aggregate_results();
            entries.extend(flush_state.take_group_columns());
            let num_rows = entries.first().map(BlockEntry::len).unwrap_or_default();
            blocks.push(DataBlock::new(entries, num_rows));
        }

        if blocks.is_empty() {
            return Ok(vec![]);
        }
        let block = DataBlock::concat(&blocks)?.consume_convert_to_full();
        self.align_final_output_types(block)
            .map(|block| vec![block])
    }
}

impl AccumulatingTransform for TransformMaterializedViewStateMerge {
    const NAME: &'static str = "TransformMaterializedViewStateMerge";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        if block.num_rows() == 0 {
            return Ok(vec![]);
        }
        let block = self.normalize_state_columns(block)?;
        let num_states = self.plan.aggregate_functions.len();
        let group_offsets = (num_states..block.num_columns()).collect::<Vec<_>>();
        let state_offsets = (0..num_states).collect::<Vec<_>>();
        let group_columns = ProjectedBlock::project(&group_offsets, &block);
        let aggregate_states = ProjectedBlock::project(&state_offsets, &block);
        self.hash_table.add_groups(
            &mut self.probe_state,
            group_columns,
            &[],
            aggregate_states,
            block.num_rows(),
        )?;
        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if output { self.flush() } else { Ok(vec![]) }
    }
}
