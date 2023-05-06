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
use std::vec;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Column;
use common_expression::DataBlock;
use common_functions::aggregates::StateAddr;
use common_functions::aggregates::StateAddrs;
use common_hashtable::HashtableEntryMutRefLike;
use common_hashtable::HashtableLike;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::transforms::AccumulatingTransform;
use common_pipeline_transforms::processors::transforms::AccumulatingTransformer;

use crate::pipelines::processors::transforms::aggregator::aggregate_cell::AggregateHashTableDropper;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::HashTableCell;
use crate::pipelines::processors::transforms::PartitionedHashTableDropper;
use crate::pipelines::processors::AggregatorParams;
use crate::sessions::QueryContext;

#[allow(clippy::enum_variant_names)]
enum HashTable<Method: HashMethodBounds> {
    MovedOut,
    HashTable(HashTableCell<Method, usize>),
    PartitionedHashTable(HashTableCell<PartitionedHashMethod<Method>, usize>),
}

impl<Method: HashMethodBounds> Default for HashTable<Method> {
    fn default() -> Self {
        Self::MovedOut
    }
}

struct AggregateSettings {
    convert_threshold: usize,
    spilling_bytes_threshold_per_proc: usize,
}

impl TryFrom<Arc<QueryContext>> for AggregateSettings {
    type Error = ErrorCode;

    fn try_from(ctx: Arc<QueryContext>) -> std::result::Result<Self, Self::Error> {
        let settings = ctx.get_settings();
        let convert_threshold = settings.get_group_by_two_level_threshold()? as usize;
        let value = settings.get_spilling_bytes_threshold_per_proc()?;

        Ok(AggregateSettings {
            convert_threshold,
            spilling_bytes_threshold_per_proc: match value == 0 {
                true => usize::MAX,
                false => value,
            },
        })
    }
}

// SELECT column_name, agg(xxx) FROM table_name GROUP BY column_name
pub struct TransformPartialAggregate<Method: HashMethodBounds> {
    method: Method,
    settings: AggregateSettings,
    hash_table: HashTable<Method>,

    params: Arc<AggregatorParams>,
}

impl<Method: HashMethodBounds> TransformPartialAggregate<Method> {
    #[allow(dead_code)]
    pub fn try_create(
        ctx: Arc<QueryContext>,
        method: Method,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
    ) -> Result<Box<dyn Processor>> {
        let hashtable = method.create_hash_table()?;
        let _dropper = AggregateHashTableDropper::create(params.clone());
        let hashtable = HashTableCell::create(hashtable, _dropper);

        let hash_table = match !Method::SUPPORT_PARTITIONED || !params.has_distinct_combinator() {
            true => HashTable::HashTable(hashtable),
            false => HashTable::PartitionedHashTable(PartitionedHashMethod::convert_hashtable(
                &method, hashtable,
            )?),
        };

        Ok(AccumulatingTransformer::create(
            input,
            output,
            TransformPartialAggregate::<Method> {
                method,
                params,
                hash_table,
                settings: AggregateSettings::try_from(ctx)?,
            },
        ))
    }

    // Block should be `convert_to_full`.
    #[inline(always)]
    fn aggregate_arguments(
        block: &DataBlock,
        params: &Arc<AggregatorParams>,
    ) -> Result<Vec<Vec<Column>>> {
        let aggregate_functions_arguments = &params.aggregate_functions_arguments;
        let mut aggregate_arguments_columns =
            Vec::with_capacity(aggregate_functions_arguments.len());
        for function_arguments in aggregate_functions_arguments {
            let mut function_arguments_column = Vec::with_capacity(function_arguments.len());

            for argument_index in function_arguments {
                // Unwrap safety: chunk has been `convert_to_full`.
                let argument_column = block
                    .get_by_offset(*argument_index)
                    .value
                    .as_column()
                    .unwrap();
                function_arguments_column.push(argument_column.clone());
            }

            aggregate_arguments_columns.push(function_arguments_column);
        }

        Ok(aggregate_arguments_columns)
    }

    #[inline(always)]
    #[allow(clippy::ptr_arg)] // &[StateAddr] slower than &StateAddrs ~20%
    fn execute(
        params: &Arc<AggregatorParams>,
        block: &DataBlock,
        places: &StateAddrs,
    ) -> Result<()> {
        let aggregate_functions = &params.aggregate_functions;
        let offsets_aggregate_states = &params.offsets_aggregate_states;
        let aggregate_arguments_columns = Self::aggregate_arguments(block, params)?;

        // This can beneficial for the case of dereferencing
        // This will help improve the performance ~hundreds of megabits per second
        let aggr_arg_columns_slice = &aggregate_arguments_columns;

        for index in 0..aggregate_functions.len() {
            let rows = block.num_rows();
            let function = &aggregate_functions[index];
            let state_offset = offsets_aggregate_states[index];
            let function_arguments = &aggr_arg_columns_slice[index];
            function.accumulate_keys(places, state_offset, function_arguments, rows)?;
        }

        Ok(())
    }

    fn execute_one_block(&mut self, block: DataBlock) -> Result<()> {
        let block = block.convert_to_full();

        let group_columns = self
            .params
            .group_columns
            .iter()
            .map(|&index| block.get_by_offset(index))
            .map(|c| (c.value.as_column().unwrap().clone(), c.data_type.clone()))
            .collect::<Vec<_>>();

        unsafe {
            let rows_num = block.num_rows();
            let state = self.method.build_keys_state(&group_columns, rows_num)?;

            match &mut self.hash_table {
                HashTable::MovedOut => unreachable!(),
                HashTable::HashTable(hashtable) => {
                    let mut places = Vec::with_capacity(rows_num);

                    for key in self.method.build_keys_iter(&state)? {
                        places.push(match hashtable.hashtable.insert_and_entry(key) {
                            Err(entry) => Into::<StateAddr>::into(*entry.get()),
                            Ok(mut entry) => {
                                let place = self.params.alloc_layout(&mut hashtable.arena);
                                *entry.get_mut() = place.addr();
                                place
                            }
                        })
                    }

                    Self::execute(&self.params, &block, &places)
                }
                HashTable::PartitionedHashTable(hashtable) => {
                    let mut places = Vec::with_capacity(rows_num);

                    for key in self.method.build_keys_iter(&state)? {
                        places.push(match hashtable.hashtable.insert_and_entry(key) {
                            Err(entry) => Into::<StateAddr>::into(*entry.get()),
                            Ok(mut entry) => {
                                let place = self.params.alloc_layout(&mut hashtable.arena);
                                *entry.get_mut() = place.addr();
                                place
                            }
                        })
                    }

                    Self::execute(&self.params, &block, &places)
                }
            }
        }
    }
}

impl<Method: HashMethodBounds> AccumulatingTransform for TransformPartialAggregate<Method> {
    const NAME: &'static str = "TransformPartialAggregate";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        self.execute_one_block(block)?;

        #[allow(clippy::collapsible_if)]
        if Method::SUPPORT_PARTITIONED {
            if matches!(&self.hash_table, HashTable::HashTable(cell)
                if cell.len() >= self.settings.convert_threshold ||
                    cell.allocated_bytes() >= self.settings.spilling_bytes_threshold_per_proc
            ) {
                if let HashTable::HashTable(cell) = std::mem::take(&mut self.hash_table) {
                    self.hash_table = HashTable::PartitionedHashTable(
                        PartitionedHashMethod::convert_hashtable(&self.method, cell)?,
                    );
                }
            }

            if matches!(&self.hash_table, HashTable::PartitionedHashTable(cell) if cell.allocated_bytes() > self.settings.spilling_bytes_threshold_per_proc)
            {
                if let HashTable::PartitionedHashTable(v) = std::mem::take(&mut self.hash_table) {
                    let _dropper = v._dropper.clone();
                    let cells = PartitionedHashTableDropper::split_cell(v);
                    let mut blocks = Vec::with_capacity(cells.len());
                    for (bucket, cell) in cells.into_iter().enumerate() {
                        if cell.hashtable.len() != 0 {
                            blocks.push(DataBlock::empty_with_meta(
                                AggregateMeta::<Method, usize>::create_spilling(
                                    bucket as isize,
                                    cell,
                                ),
                            ));
                        }
                    }

                    let method = PartitionedHashMethod::<Method>::create(self.method.clone());
                    let new_hashtable = method.create_hash_table()?;
                    self.hash_table = HashTable::PartitionedHashTable(HashTableCell::create(
                        new_hashtable,
                        _dropper.unwrap(),
                    ));
                    return Ok(blocks);
                }

                unreachable!()
            }
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        Ok(match std::mem::take(&mut self.hash_table) {
            HashTable::MovedOut => unreachable!(),
            HashTable::HashTable(v) => match v.hashtable.len() == 0 {
                true => vec![],
                false => vec![DataBlock::empty_with_meta(
                    AggregateMeta::<Method, usize>::create_hashtable(-1, v),
                )],
            },
            HashTable::PartitionedHashTable(v) => {
                let cells = PartitionedHashTableDropper::split_cell(v);
                let mut blocks = Vec::with_capacity(cells.len());
                for (bucket, cell) in cells.into_iter().enumerate() {
                    if cell.hashtable.len() != 0 {
                        blocks.push(DataBlock::empty_with_meta(
                            AggregateMeta::<Method, usize>::create_hashtable(bucket as isize, cell),
                        ));
                    }
                }

                blocks
            }
        })
    }
}
