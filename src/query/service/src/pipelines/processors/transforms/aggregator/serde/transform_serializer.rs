use std::sync::Arc;

use common_exception::Result;
use common_expression::DataBlock;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::BlockMetaTransform;
use common_pipeline_transforms::processors::transforms::BlockMetaTransformer;

use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::estimated_key_size;
use crate::pipelines::processors::transforms::aggregator::serde::serde_meta::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::KeysColumnBuilder;

pub struct TransformGroupBySerializer<Method: HashMethodBounds> {
    method: Method,
}

impl<Method: HashMethodBounds> TransformGroupBySerializer<Method> {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(BlockMetaTransformer::create(
            input,
            output,
            TransformGroupBySerializer { method },
        )))
    }
}

impl<Method> BlockMetaTransform<AggregateMeta<Method, ()>> for TransformGroupBySerializer<Method>
where Method: HashMethodBounds
{
    const NAME: &'static str = "TransformGroupBySerializer";

    fn transform(&mut self, meta: AggregateMeta<Method, ()>) -> Result<DataBlock> {
        match meta {
            AggregateMeta::Partitioned { .. } => unreachable!(),
            AggregateMeta::Serialized(_) => unreachable!(),
            AggregateMeta::HashTable(payload) => {
                let value_size = estimated_key_size(&payload.hashtable);
                let keys_len = Method::HashTable::len(&payload.hashtable);
                let mut group_key_builder = self.method.keys_column_builder(keys_len, value_size);

                for group_entity in Method::HashTable::iter(&payload.hashtable) {
                    group_key_builder.append_value(group_entity.key());
                }

                let data_block = DataBlock::new_from_columns(vec![group_key_builder.finish()]);
                data_block.add_meta(Some(AggregateSerdeMeta::create(payload.bucket)))
            }
        }
    }
}
