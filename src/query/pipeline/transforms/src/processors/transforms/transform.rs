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

use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;

// TODO: maybe we also need async transform for `SELECT sleep(1)`?
pub trait Transform: Send {
    const NAME: &'static str;
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock>;

    fn name(&self) -> String {
        Self::NAME.to_string()
    }

    fn on_start(&mut self) -> Result<()> {
        Ok(())
    }

    fn on_finish(&mut self) -> Result<()> {
        Ok(())
    }
}

pub struct Transformer<T: Transform + 'static> {
    transform: T,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    called_on_start: bool,
    called_on_finish: bool,
    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
}

impl<T: Transform + 'static> Transformer<T> {
    pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>, inner: T) -> Box<dyn Processor> {
        Box::new(Self {
            input,
            output,
            transform: inner,
            input_data: None,
            output_data: None,
            called_on_start: false,
            called_on_finish: false,
        })
    }
}

#[async_trait::async_trait]
impl<T: Transform + 'static> Processor for Transformer<T> {
    fn name(&self) -> String {
        Transform::name(&self.transform)
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if !self.called_on_start {
            return Ok(Event::Sync);
        }

        match self.output.is_finished() {
            true => self.finish_input(),
            false if !self.output.can_push() => self.not_need_data(),
            false => match self.output_data.take() {
                None if self.input_data.is_some() => Ok(Event::Sync),
                None => self.pull_data(),
                Some(data) => {
                    self.output.push_data(Ok(data));
                    Ok(Event::NeedConsume)
                }
            },
        }
    }

    fn process(&mut self) -> Result<()> {
        if !self.called_on_start {
            self.called_on_start = true;
            self.transform.on_start()?;
            return Ok(());
        }

        if let Some(data_block) = self.input_data.take() {
            let data_block = self.transform.transform(data_block)?;
            if !data_block.is_empty() || data_block.get_meta().is_some() {
                self.output_data = Some(data_block);
            }
            return Ok(());
        }

        if !self.called_on_finish {
            self.called_on_finish = true;
            self.transform.on_finish()?;
        }

        Ok(())
    }
}

impl<T: Transform> Transformer<T> {
    fn pull_data(&mut self) -> Result<Event> {
        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            return match !self.called_on_finish {
                true => Ok(Event::Sync),
                false => {
                    self.output.finish();
                    Ok(Event::Finished)
                }
            };
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn not_need_data(&mut self) -> Result<Event> {
        self.input.set_not_need_data();
        Ok(Event::NeedConsume)
    }

    fn finish_input(&mut self) -> Result<Event> {
        match !self.called_on_finish {
            true => Ok(Event::Sync),
            false => {
                self.input.finish();
                Ok(Event::Finished)
            }
        }
    }
}

pub enum UnknownMode {
    Pass,
    Ignore,
    Error,
}

// Transform for block meta and ignoring the block columns.
pub trait BlockMetaTransform<B: BlockMetaInfo>: Send + 'static {
    const UNKNOWN_MODE: UnknownMode = UnknownMode::Ignore;
    const NAME: &'static str;

    fn transform(&mut self, meta: B) -> Result<DataBlock>;

    fn on_start(&mut self) -> Result<()> {
        Ok(())
    }

    fn on_finish(&mut self) -> Result<()> {
        Ok(())
    }
}

pub struct BlockMetaTransformer<B: BlockMetaInfo, T: BlockMetaTransform<B>> {
    transform: T,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    called_on_start: bool,
    called_on_finish: bool,
    input_data: Option<B>,
    output_data: Option<DataBlock>,
    _phantom_data: PhantomData<B>,
}

impl<B: BlockMetaInfo, T: BlockMetaTransform<B>> BlockMetaTransformer<B, T> {
    pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>, inner: T) -> Box<dyn Processor> {
        Box::new(Self {
            input,
            output,
            transform: inner,
            input_data: None,
            output_data: None,
            called_on_start: false,
            called_on_finish: false,
            _phantom_data: Default::default(),
        })
    }
}

#[async_trait::async_trait]
impl<B: BlockMetaInfo, T: BlockMetaTransform<B>> Processor for BlockMetaTransformer<B, T> {
    fn name(&self) -> String {
        String::from(T::NAME)
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if !self.called_on_start {
            return Ok(Event::Sync);
        }

        if self.output.is_finished() {
            if !self.called_on_finish {
                return Ok(Event::Sync);
            }

            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(output_data) = self.output_data.take() {
            self.output.push_data(Ok(output_data));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;

            if let Some(block_meta) = data_block.take_meta() {
                if B::downcast_ref_from(&block_meta).is_some() {
                    let block_meta = B::downcast_from(block_meta).unwrap();
                    if data_block.num_rows() != 0 {
                        return Err(ErrorCode::Internal("DataBlockMeta has rows"));
                    }

                    self.input_data = Some(block_meta);
                    return Ok(Event::Sync);
                }

                data_block = data_block.add_meta(Some(block_meta))?;
            }

            match T::UNKNOWN_MODE {
                UnknownMode::Ignore => { /* do nothing */ }
                UnknownMode::Pass => {
                    self.output.push_data(Ok(data_block));
                    return Ok(Event::NeedConsume);
                }
                UnknownMode::Error => {
                    return Err(ErrorCode::Internal(format!(
                        "{} only recv {}",
                        T::NAME,
                        std::any::type_name::<B>()
                    )));
                }
            }
        }

        if self.input.is_finished() {
            return match !self.called_on_finish {
                true => Ok(Event::Sync),
                false => {
                    self.output.finish();
                    Ok(Event::Finished)
                }
            };
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if !self.called_on_start {
            self.called_on_start = true;
            self.transform.on_start()?;
            return Ok(());
        }

        if let Some(block_meta) = self.input_data.take() {
            self.output_data = Some(self.transform.transform(block_meta)?);
            return Ok(());
        }

        if !self.called_on_finish {
            self.called_on_finish = true;
            self.transform.on_finish()?;
        }

        Ok(())
    }
}
