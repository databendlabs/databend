use std::sync::Arc;

use common_exception::Result;
use common_expression::DataBlock;
use common_hashtable::HashtableLike;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::transforms::AccumulatingTransform;
use common_pipeline_transforms::processors::transforms::AccumulatingTransformer;
use common_sql::IndexType;

use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::group_by::{ArenaHolder, HashMethodBounds};
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::AggregatorParams;

enum HashTable<Method: HashMethodBounds> {
    MovedOut,
    HashTable(Method::HashTable<()>),
    PartitionedHashTable(
        <PartitionedHashMethod<Method> as PolymorphicKeysHelper<
            PartitionedHashMethod<Method>,
        >>::HashTable<()>,
    ),
}

impl<Method: HashMethodBounds> Default for HashTable<Method> {
    fn default() -> Self {
        Self::MovedOut
    }
}

// SELECT column_name FROM table_name GROUP BY column_name
pub struct TransformPartialGroupBy<Method: HashMethodBounds> {
    method: Method,
    hash_table: HashTable<Method>,
    group_columns: Vec<IndexType>,
}

impl<Method: HashMethodBounds> TransformPartialGroupBy<Method> {
    #[allow(dead_code)]
    pub fn try_create(
        method: Method,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
    ) -> Result<Box<dyn Processor>> {
        let hash_table = HashTable::HashTable(method.create_hash_table()?);
        Ok(AccumulatingTransformer::create(
            input,
            output,
            TransformPartialGroupBy::<Method> {
                method,
                hash_table,
                group_columns: params.group_columns.clone(),
            },
        ))
    }
}

impl<Method: HashMethodBounds> AccumulatingTransform for TransformPartialGroupBy<Method> {
    const NAME: &'static str = "TransformPartialGroupBy";

    fn transform(&mut self, block: DataBlock) -> Result<Option<DataBlock>> {
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
                HashTable::HashTable(hashtable) => {
                    for key in self.method.build_keys_iter(&state)? {
                        let _ = hashtable.insert_and_entry(key);
                    }
                }
                HashTable::PartitionedHashTable(hashtable) => {
                    for key in self.method.build_keys_iter(&state)? {
                        let _ = hashtable.insert_and_entry(key);
                    }
                }
            };
        }
        Ok(None)
    }

    fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        Ok(Some(DataBlock::empty_with_meta(
            match std::mem::take(&mut self.hash_table) {
                HashTable::MovedOut => unreachable!(),
                HashTable::HashTable(v) => AggregateMeta::<Method, ()>::create_hashtable(v, ArenaHolder::create(None)),
                HashTable::PartitionedHashTable(v) => {
                    AggregateMeta::<Method, ()>::create_partitioned_hashtable(v, ArenaHolder::create(None))
                }
            },
        )))
    }
}
