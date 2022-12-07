use std::any::Any;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::mem::replace;
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

///
struct ConvertGroupingMetaInfo {
    pub bucket: isize,
    pub blocks: Vec<DataBlock>,
}

enum InputPortState {
    Active {
        port: Arc<InputPort>,
        bucket: isize,
    },
    Finished(Arc<InputPort>),
}

pub struct TransformConvertGrouping<Method: HashMethod + PolymorphicKeysHelper<Method>> {
    output: Arc<OutputPort>,
    inputs: Vec<InputPortState>,

    // state: State,
    working_bucket: isize,
    method: Method,
    params: Arc<AggregatorParams>,
    buckets_blocks: HashMap<isize, Vec<DataBlock>>,
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
        // &self.inputs
        unimplemented!()
    }

    pub fn get_outputs(&self) -> &[Arc<OutputPort>] {
        // &self.outputs
        unimplemented!()
    }

    fn convert_to_two_level(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
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

    // fn init_event(&mut self) -> Result<()> {
    //     let mut res = true;
    //
    //     if let State::InitConsume(state) = &mut self.state {}
    //
    //
    //     if res {
    //         if let State::InitConsume(state) = replace(&mut self.state, State::Finished) {
    //             if state.overflow_blocks.is_empty() && state.buckets_blocks.is_empty() {
    //                 // all is single level blocks
    //                 // self.output.can_push()
    //                 // replace(&mut self.state, State::GenerateBlock())
    //             }
    //         }
    //         state
    //     }
    //     match res {
    //         true => Ok(Event::Sync),
    //         false => Ok(Event::NeedData),
    //     }
    // }
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
        if self.output.is_finished() {
            for input in &self.inputs {
                if let InputPortState::Active { port, .. } = input {
                    port.finish();
                }
            }

            // let _drop_state = std::mem::replace(&mut self.state, State::Finished);
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            for input in &self.inputs {
                if let InputPortState::Active { port, .. } = input {
                    port.set_not_need_data();
                }
            }

            return Ok(Event::NeedConsume);
        }

        let mut next_working_bucket = self.working_bucket + 1;

        for input in self.inputs.iter_mut() {
            match input {
                InputPortState::Active { port, .. } if port.is_finished() => {
                    *input = InputPortState::Finished(port.clone());
                }
                InputPortState::Active { port, bucket } if *bucket == self.working_bucket => {
                    port.set_need_data();

                    if !port.has_data() {
                        next_working_bucket = self.working_bucket;
                        continue;
                    }

                    let data_block = port.pull_data().unwrap()?;
                    let data_block_meta: Option<&AggregateInfo> = data_block.get_meta().and_then(|meta| meta.as_any().downcast_ref::<AggregateInfo>());

                    match data_block_meta {
                        // XXX: None | Some(info) if info.bucket == -1 is compile failure.
                        None => {
                            *input = InputPortState::Finished(port.clone());
                            match self.buckets_blocks.entry(-1) {
                                Entry::Vacant(v) => {
                                    v.insert(vec![data_block]);
                                }
                                Entry::Occupied(mut v) => {
                                    v.get_mut().push(data_block);
                                }
                            };
                        }
                        Some(info) if info.bucket == -1 => {
                            *input = InputPortState::Finished(port.clone());
                            match self.buckets_blocks.entry(-1) {
                                Entry::Vacant(v) => {
                                    v.insert(vec![data_block]);
                                }
                                Entry::Occupied(mut v) => {
                                    v.get_mut().push(data_block);
                                }
                            };
                        }
                        Some(info) => match info.overflow {
                            None => {
                                *bucket = info.bucket + 1;
                                match self.buckets_blocks.entry(info.bucket) {
                                    Entry::Vacant(v) => {
                                        v.insert(vec![data_block]);
                                    }
                                    Entry::Occupied(mut v) => {
                                        v.get_mut().push(data_block);
                                    }
                                };
                            }
                            Some(_) => {
                                // Skipping overflow block.
                                next_working_bucket = self.working_bucket;
                                match self.buckets_blocks.entry(-2) {
                                    Entry::Vacant(v) => {
                                        v.insert(vec![data_block]);
                                    }
                                    Entry::Occupied(mut v) => {
                                        v.get_mut().push(data_block);
                                    }
                                };
                            }
                        }
                    };
                }
                _ => { /* finished or done current bucket, do nothing */ }
            }
        }

        if self.working_bucket + 1 == next_working_bucket {
            // current working bucket is process completed.

            if self.working_bucket == 0 {
                // all single level data block
                if self.buckets_blocks.len() == 1 && self.buckets_blocks.contains_key(&-1) {
                    self.working_bucket = 256;

                    // self.output.push_data(Ok(DataBlock::empty_with_meta(
                    //
                    // )))

                    return Ok(Event::NeedConsume);
                }

                // need convert to two level data block
                self.working_bucket = next_working_bucket;
                return Ok(Event::Sync);
            }

            self.working_bucket = next_working_bucket;
            // TODO: push data block
        }
        unimplemented!()
        // match &mut self.state {
        //     State::InitConsume(state) => state.init_event(&self.inputs),
        //     _ => Ok(())
        // }?;
        //
        // match self.state {
        //     State::InitConsume(_) => Ok(Event::NeedData),
        //     State::Finished => Ok(Event::Finished)
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
        // self.state = match std::mem::replace(&mut self.state, State::Finished) {
        //     State::InitConsume(mut state) => {
        //         if state.buckets_blocks.is_empty() && state.overflow_blocks.is_empty() {
        //             // TODO: all single level data blocks.
        //             // let grouping_meta_info = ConvertGroupingMetaInfo {
        //             //     bucket: -1,
        //             //     blocks: state.single_level_blocks,
        //             // };
        //         }
        //
        //         // We process one single level data block for each call.
        //         // if let Some(data_block) = state.single_level_blocks.pop() {
        //         //     let blocks = self.convert_to_two_level(data_block)?;
        //         //     for (bucket, block) in blocks.into_iter().enumerate() {
        //         //         if !block.is_empty() {
        //         //             match state.buckets_blocks.entry(bucket) {
        //         //                 Entry::Occupied(mut v) => {
        //         //                     v.get_mut().push(block);
        //         //                 }
        //         //                 Entry::Vacant(mut v) => {
        //         //                     v.insert(vec![block]);
        //         //                 }
        //         //             };
        //         //         }
        //         //     }
        //         // }
        //         unimplemented!()
        //     }
        //     State::Finished => State::Finished,
        // };

        Ok(())
    }
}
