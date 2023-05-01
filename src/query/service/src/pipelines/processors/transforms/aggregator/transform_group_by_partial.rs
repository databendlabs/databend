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
use common_expression::DataBlock;
use common_hashtable::HashtableLike;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::transforms::AccumulatingTransform;
use common_pipeline_transforms::processors::transforms::AccumulatingTransformer;
use common_sql::IndexType;

use crate::pipelines::processors::transforms::aggregator::aggregate_cell::GroupByHashTableDropper;
use crate::pipelines::processors::transforms::aggregator::aggregate_cell::HashTableCell;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::PartitionedHashTableDropper;
use crate::pipelines::processors::AggregatorParams;
use crate::sessions::QueryContext;

#[allow(clippy::enum_variant_names)]
enum HashTable<Method: HashMethodBounds> {
    MovedOut,
    HashTable(HashTableCell<Method, ()>),
    PartitionedHashTable(HashTableCell<PartitionedHashMethod<Method>, ()>),
}

impl<Method: HashMethodBounds> Default for HashTable<Method> {
    fn default() -> Self {
        Self::MovedOut
    }
}

struct GroupBySettings {
    convert_threshold: usize,
    spilling_bytes_threshold_per_proc: usize,
}

impl TryFrom<Arc<QueryContext>> for GroupBySettings {
    type Error = ErrorCode;

    fn try_from(ctx: Arc<QueryContext>) -> std::result::Result<Self, Self::Error> {
        let settings = ctx.get_settings();
        let convert_threshold = settings.get_group_by_two_level_threshold()? as usize;
        let value = settings.get_spilling_bytes_threshold_per_proc()?;

        Ok(GroupBySettings {
            convert_threshold,
            spilling_bytes_threshold_per_proc: match value == 0 {
                true => usize::MAX,
                false => value,
            },
        })
    }
}

// SELECT column_name FROM table_name GROUP BY column_name
pub struct TransformPartialGroupBy<Method: HashMethodBounds> {
    method: Method,
    hash_table: HashTable<Method>,
    group_columns: Vec<IndexType>,
    settings: GroupBySettings,
}

impl<Method: HashMethodBounds> TransformPartialGroupBy<Method> {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        method: Method,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
    ) -> Result<Box<dyn Processor>> {
        let hashtable = method.create_hash_table()?;
        let _dropper = GroupByHashTableDropper::<Method>::create();
        let hash_table = HashTable::HashTable(HashTableCell::create(hashtable, _dropper));

        Ok(AccumulatingTransformer::create(
            input,
            output,
            TransformPartialGroupBy::<Method> {
                method,
                hash_table,
                group_columns: params.group_columns.clone(),
                settings: GroupBySettings::try_from(ctx)?,
            },
        ))
    }
}

impl<Method: HashMethodBounds> AccumulatingTransform for TransformPartialGroupBy<Method> {
    const NAME: &'static str = "TransformPartialGroupBy";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        let block = block.convert_to_full();
        let group_columns = self
            .group_columns
            .iter()
            .map(|&index| block.get_by_offset(index))
            .collect::<Vec<_>>();

        let group_columns = group_columns
            .iter()
            .map(|c| (c.value.as_column().unwrap().clone(), c.data_type.clone()))
            .collect::<Vec<_>>();

        unsafe {
            let rows_num = block.num_rows();
            let state = self.method.build_keys_state(&group_columns, rows_num)?;

            match &mut self.hash_table {
                HashTable::MovedOut => unreachable!(),
                HashTable::HashTable(cell) => {
                    for key in self.method.build_keys_iter(&state)? {
                        let _ = cell.hashtable.insert_and_entry(key);
                    }
                }
                HashTable::PartitionedHashTable(cell) => {
                    for key in self.method.build_keys_iter(&state)? {
                        let _ = cell.hashtable.insert_and_entry(key);
                    }
                }
            };

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
                    if let HashTable::PartitionedHashTable(v) = std::mem::take(&mut self.hash_table)
                    {
                        let _dropper = v._dropper.clone();
                        let cells = PartitionedHashTableDropper::split_cell(v);
                        let mut blocks = Vec::with_capacity(cells.len());
                        for (bucket, cell) in cells.into_iter().enumerate() {
                            if cell.hashtable.len() != 0 {
                                blocks.push(DataBlock::empty_with_meta(
                                    AggregateMeta::<Method, ()>::create_spilling(
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
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        Ok(match std::mem::take(&mut self.hash_table) {
            HashTable::MovedOut => unreachable!(),
            HashTable::HashTable(cell) => match cell.hashtable.len() == 0 {
                true => vec![],
                false => vec![DataBlock::empty_with_meta(
                    AggregateMeta::<Method, ()>::create_hashtable(-1, cell),
                )],
            },
            HashTable::PartitionedHashTable(v) => {
                let _ = v.hashtable.unsize_key_size();
                let cells = PartitionedHashTableDropper::split_cell(v);
                let mut blocks = Vec::with_capacity(cells.len());
                for (bucket, cell) in cells.into_iter().enumerate() {
                    if cell.hashtable.len() != 0 {
                        blocks.push(DataBlock::empty_with_meta(
                            AggregateMeta::<Method, ()>::create_hashtable(bucket as isize, cell),
                        ));
                    }
                }

                blocks
            }
        })
    }
}
