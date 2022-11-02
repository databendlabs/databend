use std::borrow::BorrowMut;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use common_base::base::ThreadPool;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_datavalues::DataType;
use common_datavalues::MutableColumn;
use common_datavalues::ScalarColumn;
use common_datavalues::Series;
use common_datavalues::StringColumn;
use common_exception::Result;
use common_functions::aggregates::StateAddr;
use common_functions::aggregates::StateAddrs;
use tracing::info;

use crate::pipelines::processors::transforms::aggregator::aggregate_info::AggregateInfo;
use crate::pipelines::processors::transforms::group_by::AggregatorState;
use crate::pipelines::processors::transforms::group_by::GroupColumnsBuilder;
use crate::pipelines::processors::transforms::group_by::KeysColumnIter;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::group_by::StateEntityMutRef;
use crate::pipelines::processors::transforms::group_by::StateEntityRef;
use crate::pipelines::processors::transforms::transform_aggregator::Aggregator;
use crate::pipelines::processors::AggregatorParams;
use crate::sessions::QueryContext;

pub struct ParallelFinalAggregator<const HAS_AGG: bool, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + Sync + 'static
{
    is_generated: bool,
    method: Arc<Method>,
    query_ctx: Arc<QueryContext>,
    params: Arc<AggregatorParams>,
    buckets_blocks: HashMap<isize, Vec<DataBlock>>,
    generate_blocks: Vec<DataBlock>,
}

impl<Method, const HAS_AGG: bool> ParallelFinalAggregator<HAS_AGG, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + Sync + 'static
{
    pub fn create(
        ctx: Arc<QueryContext>,
        method: Method,
        params: Arc<AggregatorParams>,
    ) -> Result<Self> {
        Ok(Self {
            params,
            query_ctx: ctx,
            is_generated: false,
            method: Arc::new(method),
            buckets_blocks: HashMap::new(),
            generate_blocks: vec![],
        })
    }
}

impl<Method, const HAS_AGG: bool> Aggregator for ParallelFinalAggregator<HAS_AGG, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + Sync + 'static
{
    const NAME: &'static str = "GroupByFinalTransform";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        let mut bucket = -1;
        if let Some(meta_info) = block.get_meta() {
            if let Some(meta_info) = meta_info.as_any().downcast_ref::<AggregateInfo>() {
                bucket = meta_info.bucket;
            }
        }

        match self.buckets_blocks.entry(bucket) {
            Entry::Vacant(v) => {
                v.insert(vec![block]);
                Ok(())
            }
            Entry::Occupied(mut v) => {
                v.get_mut().push(block);
                Ok(())
            }
        }
    }

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if !self.is_generated {
            self.is_generated = true;

            if self.buckets_blocks.len() == 1 || self.buckets_blocks.contains_key(&-1) {
                let mut data_blocks = vec![];
                for (_, bucket_blocks) in std::mem::take(&mut self.buckets_blocks) {
                    data_blocks.extend(bucket_blocks);
                }

                let method = self.method.clone();
                let params = self.params.clone();
                let mut bucket_aggregator =
                    BucketAggregator::<HAS_AGG, Method>::create(method, params)?;
                self.generate_blocks
                    .extend(bucket_aggregator.merge_blocks(data_blocks)?);
            } else if self.buckets_blocks.len() > 1 {
                info!("Merge to final state using a parallel algorithm.");

                let settings = self.query_ctx.get_settings();
                let max_threads = settings.get_max_threads()? as usize;
                let thread_pool = ThreadPool::create(max_threads)?;
                let mut join_handles = Vec::with_capacity(self.buckets_blocks.len());

                for (_, bucket_blocks) in std::mem::take(&mut self.buckets_blocks) {
                    let method = self.method.clone();
                    let params = self.params.clone();
                    let mut bucket_aggregator =
                        BucketAggregator::<HAS_AGG, Method>::create(method, params)?;
                    join_handles.push(
                        thread_pool.execute(move || bucket_aggregator.merge_blocks(bucket_blocks)),
                    );
                }

                self.generate_blocks.reserve(join_handles.len());
                for join_handle in join_handles {
                    self.generate_blocks.extend(join_handle.join()?);
                }
            }
        }

        Ok(self.generate_blocks.pop())
    }
}

struct BucketAggregator<const HAS_AGG: bool, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + Sync + 'static
{
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
            aggregator_state: state,
        })
    }

    pub fn merge_blocks(&mut self, blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        for data_block in blocks {
            // 1.1 and 1.2.
            let aggregate_function_len = self.params.aggregate_functions.len();
            let keys_column = data_block.column(aggregate_function_len);
            let keys_iter = self.method.keys_iter_from_column(keys_column)?;

            if !HAS_AGG {
                let mut inserted = true;
                for keys_ref in keys_iter.get_slice() {
                    self.aggregator_state
                        .entity_by_key(*keys_ref, &mut inserted);
                }
            } else {
                // first state places of current block
                let places = self.lookup_state(keys_iter.get_slice());

                let states_columns = (0..aggregate_function_len)
                    .map(|i| data_block.column(i))
                    .collect::<Vec<_>>();
                let mut states_binary_columns = Vec::with_capacity(states_columns.len());

                for agg in states_columns.iter().take(aggregate_function_len) {
                    let aggr_column: &StringColumn = Series::check_get(agg)?;
                    states_binary_columns.push(aggr_column);
                }

                let aggregate_functions = &self.params.aggregate_functions;
                let offsets_aggregate_states = &self.params.offsets_aggregate_states;
                if let Some(temp_place) = self.temp_place {
                    for (row, place) in places.iter().enumerate() {
                        for (idx, aggregate_function) in aggregate_functions.iter().enumerate() {
                            let final_place = place.next(offsets_aggregate_states[idx]);
                            let state_place = temp_place.next(offsets_aggregate_states[idx]);

                            let mut data = states_binary_columns[idx].get_data(row);
                            aggregate_function.deserialize(state_place, &mut data)?;
                            aggregate_function.merge(final_place, state_place)?;
                        }
                    }
                }
            }
        }

        let mut group_columns_builder = self
            .method
            .group_columns_builder(self.aggregator_state.len(), &self.params);

        if !HAS_AGG {
            for group_entity in self.aggregator_state.iter() {
                group_columns_builder.append_value(group_entity.get_state_key());
            }

            let columns = group_columns_builder.finish()?;
            Ok(vec![DataBlock::create(
                self.params.output_schema.clone(),
                columns,
            )])
        } else {
            let aggregate_functions = &self.params.aggregate_functions;
            let offsets_aggregate_states = &self.params.offsets_aggregate_states;

            let mut aggregates_column_builder: Vec<Box<dyn MutableColumn>> = {
                let mut values = vec![];
                for aggregate_function in aggregate_functions {
                    let builder = aggregate_function.return_type()?.create_mutable(1024);
                    values.push(builder)
                }
                values
            };

            for group_entity in self.aggregator_state.iter() {
                let place: StateAddr = group_entity.get_state_value().into();

                for (idx, aggregate_function) in aggregate_functions.iter().enumerate() {
                    let arg_place = place.next(offsets_aggregate_states[idx]);
                    let builder: &mut dyn MutableColumn =
                        aggregates_column_builder[idx].borrow_mut();
                    aggregate_function.merge_result(arg_place, builder)?;
                }

                group_columns_builder.append_value(group_entity.get_state_key());
            }

            // Build final state block.
            let fields_len = self.params.output_schema.fields().len();
            let mut columns = Vec::with_capacity(fields_len);

            for mut array in aggregates_column_builder {
                columns.push(array.to_column());
            }

            columns.extend_from_slice(&group_columns_builder.finish()?);
            Ok(vec![DataBlock::create(
                self.params.output_schema.clone(),
                columns,
            )])
        }
    }

    /// Allocate aggregation function state for each key(the same key can always get the same state)
    #[inline(always)]
    fn lookup_state(
        &mut self,
        keys: &[<Method::State as AggregatorState<Method>>::KeyRef<'_>],
    ) -> StateAddrs {
        let mut places = Vec::with_capacity(keys.len());

        let mut inserted = true;
        let unsafe_state = &mut self.aggregator_state as *mut Method::State;
        for key in keys {
            let mut entity = unsafe { (*unsafe_state).entity_by_key(*key, &mut inserted) };

            match inserted {
                true => {
                    if let Some(place) = unsafe { (*unsafe_state).alloc_layout(&self.params) } {
                        places.push(place);
                        entity.set_state_value(place.addr());
                    }
                }
                false => {
                    let place: StateAddr = entity.get_state_value().into();
                    places.push(place);
                }
            }
        }
        places
    }
}

impl<const HAS_AGG: bool, Method> Drop for BucketAggregator<HAS_AGG, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + Sync + 'static
{
    fn drop(&mut self) {
        let aggregator_params = self.params.as_ref();
        let aggregate_functions = &aggregator_params.aggregate_functions;
        let offsets_aggregate_states = &aggregator_params.offsets_aggregate_states;

        let functions = aggregate_functions
            .iter()
            .filter(|p| p.need_manual_drop_state())
            .collect::<Vec<_>>();

        let state_offsets = offsets_aggregate_states
            .iter()
            .enumerate()
            .filter(|(idx, _)| aggregate_functions[*idx].need_manual_drop_state())
            .map(|(_, s)| *s)
            .collect::<Vec<_>>();

        for group_entity in self.aggregator_state.iter() {
            let place: StateAddr = group_entity.get_state_value().into();

            for (function, state_offset) in functions.iter().zip(state_offsets.iter()) {
                unsafe { function.drop_state(place.next(*state_offset)) }
            }
        }

        if let Some(temp_place) = self.temp_place {
            for (state_offset, function) in state_offsets.iter().zip(functions.iter()) {
                let place = temp_place.next(*state_offset);
                unsafe { function.drop_state(place) }
            }
        }
    }
}
