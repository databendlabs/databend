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

use std::marker::PhantomData;
use std::sync::Arc;

use async_channel::Receiver;
use async_channel::Sender;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;

use super::core::Bounds;
use super::core::Rows;
use super::SortCollectedMeta;
use super::SortExchangeMeta;
use crate::HookTransform;
use crate::HookTransformer;

pub struct TransformSortBoundBroadcast<R: Rows, C: BroadcastChannel> {
    state: SortSampleState<C>,
    input_data: Vec<SortCollectedMeta>,
    output_data: Option<SortCollectedMeta>,
    called_on_finish: bool,

    _r: PhantomData<R>,
}

impl<R, C> TransformSortBoundBroadcast<R, C>
where
    R: Rows + 'static,
    C: BroadcastChannel,
{
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        state: SortSampleState<C>,
    ) -> Box<dyn Processor> {
        Box::new(HookTransformer::new(input, output, Self {
            state,
            input_data: Vec::new(),
            output_data: None,
            called_on_finish: false,
            _r: PhantomData,
        }))
    }
}

#[derive(Clone)]
pub struct SortSampleState<C: BroadcastChannel> {
    channel: C,
    batch_rows: usize,
}

pub trait BroadcastChannel: Clone + Send + 'static {
    fn sender(&self) -> Sender<BlockMetaInfoPtr>;
    fn receiver(&self) -> Receiver<BlockMetaInfoPtr>;
}

impl<C: BroadcastChannel> SortSampleState<C> {
    pub fn new(batch_rows: usize, channel: C) -> Self {
        Self {
            channel,
            batch_rows,
        }
    }

    #[fastrace::trace(name = "SortSampleState::commit_sample")]
    async fn commit_sample(
        &mut self,
        meta: Option<SortExchangeMeta>,
    ) -> Result<Vec<SortExchangeMeta>> {
        let sender = self.channel.sender();
        let is_empty = meta.is_none();
        let meta = meta.map(|meta| meta.boxed()).unwrap_or(().boxed());
        sender
            .send(meta)
            .await
            .map_err(|_| ErrorCode::TokioError("send sort bounds failed"))?;
        sender.close();
        log::debug!(is_empty; "sample has sent");

        let receiver = self.channel.receiver();
        let mut all = Vec::new();
        while let Ok(r) = receiver.recv().await {
            match SortExchangeMeta::downcast_from_err(r) {
                Ok(meta) => all.push(meta),
                Err(r) => {
                    debug_assert!(().boxed().equals(&r))
                }
            };
        }
        Ok(all)
    }
}

#[async_trait::async_trait]
impl<R, C> HookTransform for TransformSortBoundBroadcast<R, C>
where
    R: Rows + 'static,
    C: BroadcastChannel,
{
    const NAME: &'static str = "SortBoundBroadcast";

    fn on_input(&mut self, mut data: DataBlock) -> Result<()> {
        let meta = data
            .take_meta()
            .and_then(SortCollectedMeta::downcast_from)
            .expect("require a SortCollectedMeta");
        self.input_data.push(meta);
        Ok(())
    }

    fn on_output(&mut self) -> Result<Option<DataBlock>> {
        Ok(self.output_data.as_mut().and_then(|meta| {
            meta.sequences.pop().map(|seq| {
                DataBlock::empty_with_meta(Box::new(SortCollectedMeta {
                    params: meta.params,
                    bounds: meta.bounds.clone(),
                    sequences: vec![seq],
                }))
            })
        }))
    }

    fn need_process(&self, input_finished: bool) -> Option<Event> {
        if input_finished && !self.called_on_finish {
            Some(Event::Async)
        } else {
            None
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        self.called_on_finish = true;

        let bounds = Bounds::merge::<R>(
            self.input_data
                .iter_mut()
                .map(|meta| std::mem::take(&mut meta.bounds))
                .collect(),
            self.state.batch_rows,
        )?;

        let sequences: Vec<_> = self
            .input_data
            .iter_mut()
            .flat_map(|meta| meta.sequences.drain(..))
            .collect();

        if sequences.is_empty() {
            self.state.commit_sample(None).await?;
            return Ok(());
        }

        let params = self.input_data.first().unwrap().params;
        let local = SortCollectedMeta {
            params,
            bounds,
            sequences,
        };

        let bounds = local.normalize_bounds();
        let global = self
            .state
            .commit_sample(Some(SortExchangeMeta { params, bounds }))
            .await?;

        let bounds_vec = global
            .into_iter()
            .map(|meta| {
                assert!(!meta.bounds.is_empty());
                meta.bounds.clone()
            })
            .collect();

        let bounds = Bounds::merge::<R>(bounds_vec, self.state.batch_rows)?.dedup::<R>();
        log::debug!(global_bounds_len = bounds.len(); "bound has broadcasted");
        self.output_data = Some(SortCollectedMeta { bounds, ..local });
        Ok(())
    }
}

impl SortCollectedMeta {
    fn normalize_bounds(&self) -> Bounds {
        if self.bounds.len() > 1 {
            return self.bounds.clone();
        }

        let Some(seq) = self.sequences.get(self.sequences.len() / 2) else {
            unreachable!()
        };

        seq.get(seq.len() / 2)
            .map(|block| match block.domain.len() {
                0 => Bounds::default(),
                1 => Bounds::new_unchecked(block.domain.clone()),
                _ => Bounds::new_unchecked(block.domain.slice(0..1)),
            })
            .unwrap()
    }
}
