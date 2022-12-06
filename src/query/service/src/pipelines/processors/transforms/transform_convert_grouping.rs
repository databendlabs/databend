use std::any::Any;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use common_datablocks::{BlockMetaInfoPtr, DataBlock, HashMethod};
use common_pipeline_core::processors::processor::{Event, ProcessorPtr};
use crate::pipelines::processors::{AggregatorParams, AggregatorTransformParams};
use crate::sessions::QueryContext;
use common_exception::Result;
use common_pipeline_core::processors::port::{InputPort, OutputPort};
use common_pipeline_core::processors::{Processor, ResizeProcessor};
use crate::pipelines::processors::transforms::aggregator::{AggregateInfo, OverflowInfo};
use crate::pipelines::processors::transforms::group_by::{KeysColumnIter, PolymorphicKeysHelper};

//
struct ConvertGroupingMetaInfo {
    pub bucket: isize,
    pub blocks: Vec<DataBlock>,
}

pub enum State {
    // Consume all input ports once and skip overflow blocks.
    InitConsume(InitConsume),
    Finished,
}

pub struct TransformConvertGrouping<Method: HashMethod + PolymorphicKeysHelper<Method>> {
    output: Arc<OutputPort>,
    inputs: Vec<Arc<InputPort>>,

    state: State,
    method: Method,
    params: Arc<AggregatorParams>,
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method>> TransformConvertGrouping<Method> {
    pub fn create(
        ctx: Arc<QueryContext>,
        input_nums: usize,
        output_nums: usize,
        aggregator_params: Arc<AggregatorParams>,
    ) -> Result<Self> {
        // transform_params.method
        // let mut inputs_port = Vec::with_capacity(input_nums);
        //
        // for _index in 0..input_nums {
        //     inputs_port.push(InputPort::create());
        // }

        // let inputs_block = vec![None; inputs_port.len()];

        // Ok(TransformConvertGrouping {
        //     inputs_block,
        //     inputs: inputs_port,
        //     output: OutputPort::create(),
        // })
        unimplemented!()
    }

    pub fn get_inputs(&self) -> &[Arc<InputPort>] {
        &self.inputs
    }

    pub fn get_outputs(&self) -> &[Arc<OutputPort>] {
        // &self.outputs
        unimplemented!()
    }

    pub fn convert_to_two_level(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        let aggregate_function_len = self.params.aggregate_functions.len();
        let keys_column = data_block.column(aggregate_function_len);
        let keys_iter = self.method.keys_iter_from_column(keys_column)?;

        let mut indices = Vec::with_capacity(data_block.num_rows());

        for key_item in keys_iter.iter() {
            let hash = self.method.get_hash(key_item);
            indices.push(hash as usize >> (64u32 - 8));
        }

        DataBlock::scatter_block(&data_block, &indices, 256)
    }
}

#[async_trait::async_trait]
impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static> Processor for TransformConvertGrouping<Method> {
    fn name(&self) -> String {
        String::from("TransformConvertGrouping")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        // if self.output.is_finished() {
        //     for input in self.inputs {
        //         input.finish();
        //     }
        //
        //     // self.buckets_blocks.clear();
        //     return Ok(Event::Finished);
        // }
        unimplemented!()

        // match &mut self.state {
        //     State::InitConsume(state) => state.init_event(&self.inputs),
        // }

        // if !self.inputs_block.is_empty() {
        //     if !self.first_read_inputs() {
        //         return Ok(Event::NeedData);
        //     }
        // }
        //
        // if !self.buckets_blocks.is_empty() && !self.single_level_blocks.is_empty() {
        //     return Ok(Event::Sync);
        // }
        //
        // if !self.output.can_push() {
        //     // TODO: set not need data.
        //     return Ok(Event::NeedConsume);
        // }
        //
        // // TODO: pulling all input
        // todo!()
    }

    fn process(&mut self) -> Result<()> {
        self.state = match std::mem::replace(&mut self.state, State::Finished) {
            State::InitConsume(mut state) => {
                if state.buckets_blocks.is_empty() && state.overflow_blocks.is_empty() {
                    // TODO: all single level data blocks.
                }

                // We process one single level data block for each call.
                if let Some(data_block) = state.single_level_blocks.pop() {
                    let blocks = self.convert_to_two_level(data_block)?;
                    for (bucket, block) in blocks.into_iter().enumerate() {
                        if !block.is_empty() {
                            match state.buckets_blocks.entry(bucket) {
                                Entry::Occupied(mut v) => {
                                    v.get_mut().push(block);
                                }
                                Entry::Vacant(mut v) => {
                                    v.insert(vec![block]);
                                }
                            };
                        }
                    }
                }
                unimplemented!()
            }
            State::Finished => State::Finished,
        };

        Ok(())
    }
}

struct InitConsume {
    overflow_blocks: Vec<DataBlock>,
    single_level_blocks: Vec<DataBlock>,
    buckets_blocks: HashMap<usize, Vec<DataBlock>>,
    inputs_consume_flag: Vec<bool>,
}

impl InitConsume {
    pub fn init_event(&mut self, inputs: &[Arc<InputPort>]) -> Result<Event> {
        let mut res = true;
        for (index, input) in inputs.iter().enumerate() {
            if !input.is_finished() && !self.inputs_consume_flag[index] {
                input.set_need_data();

                if !input.has_data() {
                    res = false;
                }

                let data_block = input.pull_data().unwrap()?;

                if let Some(meta_info) = data_block.get_meta() {
                    if let Some(meta_info) = meta_info.as_any().downcast_ref::<AggregateInfo>() {
                        if meta_info.overflow.is_some() {
                            res = false;
                            self.overflow_blocks.push(data_block);
                            continue;
                        }

                        if meta_info.bucket >= 0 {
                            let bucket = meta_info.bucket as usize;
                            self.inputs_consume_flag[bucket] = true;
                            match self.buckets_blocks.entry(bucket) {
                                Entry::Occupied(mut v) => {
                                    v.get_mut().push(data_block);
                                }
                                Entry::Vacant(v) => {
                                    v.insert(vec![data_block]);
                                }
                            };

                            continue;
                        }
                    }
                }

                res = false;
                self.single_level_blocks.push(data_block);
            }
        }

        match res {
            true => Ok(Event::Sync),
            false => Ok(Event::NeedData),
        }
    }
}
