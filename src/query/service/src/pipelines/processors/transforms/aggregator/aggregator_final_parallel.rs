use std::sync::Arc;
use common_base::base::ThreadPool;
use common_datablocks::{DataBlock, HashMethod};
use common_functions::aggregates::StateAddr;
use crate::pipelines::processors::AggregatorParams;
use crate::pipelines::processors::transforms::group_by::{AggregatorState, PolymorphicKeysHelper};
use crate::pipelines::processors::transforms::transform_aggregator::Aggregator;
use common_exception::Result;
use crate::pipelines::processors::transforms::aggregator::aggregate_info::AggregateInfo;
use crate::pipelines::processors::transforms::aggregator::FinalAggregator;

pub struct ParallelFinalAggregator<const HAS_AGG: bool, Method>
    where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + Sync + 'static
{
    is_generated: bool,
    method: Arc<Method>,
    // state: Method::State,
    params: Arc<AggregatorParams>,
    // used for deserialization only, so we can reuse it during the loop
    // temp_place: Option<StateAddr>,
    buckets_blocks: Vec<Vec<DataBlock>>,
}

impl<Method, const HAS_AGG: bool> Aggregator for ParallelFinalAggregator<HAS_AGG, Method>
    where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + Sync + 'static
{
    const NAME: &'static str = "GroupByFinalTransform";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        let mut bucket = 0;
        if let Some(meta_info) = block.get_meta() {
            if let Some(meta_info) = meta_info.as_any().downcast_ref::<AggregateInfo>() {
                bucket = meta_info.bucket;
            }
        }

        self.buckets_blocks[bucket].push(block);
        Ok(())
    }

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if !self.is_generated {
            self.is_generated = true;

            let mut thread_pool = ThreadPool::create(8)?;
            let mut join_handles = Vec::with_capacity(self.buckets_blocks.len());

            for bucket_blocks in std::mem::take(&mut self.buckets_blocks) {
                if !bucket_blocks.is_empty() {
                    let method = self.method.clone();
                    let params = self.params.clone();
                    let mut bucket_aggregator = BucketAggregator::<HAS_AGG, Method>::create(method, params)?;
                    join_handles.push(thread_pool.execute(move || bucket_aggregator.merge_blocks(bucket_blocks)));
                }
            }

            for join_handle in join_handles {
                let aggregated_blocks = join_handle.join()?;
            }
            // TODO: get group by blocks;
        }

        Ok(None)
    }
}

struct BucketAggregator<const HAS_AGG: bool, Method>
    where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + Sync + 'static
{
    states_dropped: bool,

    method: Arc<Method>,
    params: Arc<AggregatorParams>,
    aggregator_state: Method::State,

    // used for deserialization only, so we can reuse it during the loop
    temp_place: Option<StateAddr>,
}

impl<const HAS_AGG: bool, Method> BucketAggregator<HAS_AGG, Method>
    where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + Sync + 'static
{
    pub fn create(method: Arc<Method>, params: Arc<AggregatorParams>) -> Result<Self> {
        let state = method.aggregate_state();
        let temp_place = match params.aggregate_functions.is_empty() {
            true => None,
            false => state.alloc_layout(&params),
        };


        Ok(Self {
            method,
            params,
            temp_place,
            states_dropped: false,
            aggregator_state: state,
        })
    }

    pub fn merge_blocks(&mut self, blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        // TODO:
        for data_block in blocks {
            // 1.1 and 1.2.
            let aggregate_function_len = self.params.aggregate_functions.len();
            let keys_column = data_block.column(aggregate_function_len);
            // let s = self.method;
            let keys_iter = self.method.keys_iter_from_column(keys_column)?;
        }

        unimplemented!()
    }
}
