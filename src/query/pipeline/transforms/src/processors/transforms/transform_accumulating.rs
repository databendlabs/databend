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
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_base::runtime::drop_guard;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

#[async_trait::async_trait]
pub trait AccumulatingTransform: Send {
    const NAME: &'static str;

    const SUPPORT_SPILL: bool = false;

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>>;

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        Ok(vec![])
    }

    fn interrupt(&self) {}

    fn configure_peer_nodes(&mut self, _nodes: &[String]) {}

    fn need_spill(&self) -> bool {
        false
    }

    fn prepare_spill_payload(&mut self) -> Result<bool> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented prepare_spill_payload",
        ))
    }

    async fn flush_spill_payload(&mut self) -> Result<bool> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented flush_spill_payload",
        ))
    }
}

pub struct AccumulatingTransformer<T: AccumulatingTransform + 'static> {
    inner: T,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    called_on_finish: bool,
    input_data: Option<DataBlock>,
    output_data: VecDeque<DataBlock>,

    flush_spill_payload: bool,
    prepare_spill_payload: bool,
}

impl<T: AccumulatingTransform + 'static> AccumulatingTransformer<T> {
    pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>, inner: T) -> Box<dyn Processor> {
        Box::new(Self {
            inner,
            input,
            output,
            input_data: None,
            output_data: VecDeque::with_capacity(1),
            called_on_finish: false,
            flush_spill_payload: false,
            prepare_spill_payload: false,
        })
    }
}

impl<T: AccumulatingTransform + 'static> Drop for AccumulatingTransformer<T> {
    fn drop(&mut self) {
        drop_guard(move || {
            if !self.called_on_finish {
                self.called_on_finish = true;
                self.inner.on_finish(false).unwrap();
            }
        })
    }
}

#[async_trait::async_trait]
impl<T: AccumulatingTransform + 'static> Processor for AccumulatingTransformer<T> {
    fn name(&self) -> String {
        String::from(T::NAME)
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            if !self.called_on_finish {
                return Ok(Event::Sync);
            }

            self.input.finish();
            return Ok(Event::Finished);
        }

        if self.prepare_spill_payload {
            return Ok(Event::Sync);
        }

        if self.flush_spill_payload {
            return Ok(Event::Async);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.pop_front() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

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

    fn interrupt(&self) {
        self.inner.interrupt();
    }

    fn configure_peer_nodes(&mut self, nodes: &[String]) {
        self.inner.configure_peer_nodes(nodes)
    }

    fn process(&mut self) -> Result<()> {
        if self.prepare_spill_payload {
            self.prepare_spill_payload = false;
            self.flush_spill_payload = self.prepare_spill_payload()?;
            return Ok(());
        }

        if let Some(data_block) = self.input_data.take() {
            self.output_data.extend(self.inner.transform(data_block)?);
            self.prepare_spill_payload = self.inner.need_spill();
            return Ok(());
        }

        if !self.called_on_finish {
            self.called_on_finish = true;
            self.output_data.extend(self.inner.on_finish(true)?);
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if self.flush_spill_payload {
            self.flush_spill_payload = false;
            self.prepare_spill_payload = self.flush_spill_payload().await?;
        }

        Ok(())
    }

    fn prepare_spill_payload(&mut self) -> Result<bool> {
        self.inner.prepare_spill_payload()
    }

    async fn flush_spill_payload(&mut self) -> Result<bool> {
        self.inner.flush_spill_payload().await
    }
}

pub trait BlockMetaAccumulatingTransform<B: BlockMetaInfo>: Send + 'static {
    const NAME: &'static str;

    fn transform(&mut self, data: B) -> Result<Option<DataBlock>>;

    fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        Ok(None)
    }
}

pub struct BlockMetaAccumulatingTransformer<B: BlockMetaInfo, T: BlockMetaAccumulatingTransform<B>>
{
    inner: T,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    called_on_finish: bool,
    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
    _phantom_data: PhantomData<B>,
}

impl<B: BlockMetaInfo, T: BlockMetaAccumulatingTransform<B>>
    BlockMetaAccumulatingTransformer<B, T>
{
    pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>, inner: T) -> Box<dyn Processor> {
        Box::new(Self {
            inner,
            input,
            output,
            input_data: None,
            output_data: None,
            called_on_finish: false,
            _phantom_data: Default::default(),
        })
    }
}

impl<B: BlockMetaInfo, T: BlockMetaAccumulatingTransform<B>> Drop
    for BlockMetaAccumulatingTransformer<B, T>
{
    fn drop(&mut self) {
        drop_guard(move || {
            if !self.called_on_finish {
                self.inner.on_finish(false).unwrap();
            }
        })
    }
}

#[async_trait::async_trait]
impl<B: BlockMetaInfo, T: BlockMetaAccumulatingTransform<B>> Processor
    for BlockMetaAccumulatingTransformer<B, T>
{
    fn name(&self) -> String {
        String::from(T::NAME)
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
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

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

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

    fn process(&mut self) -> Result<()> {
        if let Some(mut data_block) = self.input_data.take() {
            debug_assert!(data_block.is_empty());

            if let Some(block_meta) = data_block.take_meta() {
                if let Some(block_meta) = B::downcast_from(block_meta) {
                    self.output_data = self.inner.transform(block_meta)?;
                }
            }

            return Ok(());
        }

        if !self.called_on_finish {
            self.called_on_finish = true;
            self.output_data = self.inner.on_finish(true)?;
        }

        Ok(())
    }
}
