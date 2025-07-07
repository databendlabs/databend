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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::sort::Rows;
use databend_common_pipeline_transforms::HookTransform;
use databend_common_pipeline_transforms::HookTransformer;

use super::bounds::Bounds;
use super::SortCollectedMeta;
use crate::pipelines::processors::transforms::sort::SortExchangeMeta;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::sessions::QueryContext;

pub struct TransformSortBoundBroadcast<R: Rows> {
    state: SortSampleState,
    input_data: Option<SortCollectedMeta>,
    output_data: Option<DataBlock>,
    called_on_finish: bool,

    _r: PhantomData<R>,
}

impl<R: Rows + 'static> TransformSortBoundBroadcast<R> {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        state: SortSampleState,
    ) -> Box<dyn Processor> {
        Box::new(HookTransformer::new(input, output, Self {
            state,
            input_data: None,
            output_data: None,
            called_on_finish: false,
            _r: PhantomData,
        }))
    }
}

#[derive(Clone)]
pub struct SortSampleState {
    ctx: Arc<QueryContext>,
    broadcast_id: u32,
    #[expect(dead_code)]
    schema: DataSchemaRef,
    batch_rows: usize,
}

impl SortSampleState {
    pub fn new(
        schema: DataSchemaRef,
        batch_rows: usize,
        ctx: Arc<QueryContext>,
        broadcast_id: u32,
    ) -> SortSampleState {
        SortSampleState {
            ctx,
            broadcast_id,
            schema,
            batch_rows,
        }
    }

    async fn commit_sample(
        &mut self,
        meta: Option<SortExchangeMeta>,
    ) -> Result<Vec<SortExchangeMeta>> {
        let sender = self.ctx.broadcast_source_sender(self.broadcast_id);
        let meta = meta.map(|meta| meta.boxed()).unwrap_or(().boxed());
        sender
            .send(meta)
            .await
            .map_err(|_| ErrorCode::TokioError("send sort bounds failed"))?;
        sender.close();

        let receiver = self.ctx.broadcast_sink_receiver(self.broadcast_id);
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
impl<R: Rows + 'static> HookTransform for TransformSortBoundBroadcast<R> {
    const NAME: &'static str = "TransformSortBoundBroadcast";

    fn on_input(&mut self, mut data: DataBlock) -> Result<()> {
        let meta = data
            .take_meta()
            .and_then(SortCollectedMeta::downcast_from)
            .expect("require a SortCollectedMeta");
        debug_assert!(self.input_data.is_none());
        self.input_data = Some(meta);
        Ok(())
    }

    fn on_output(&mut self) -> Result<Option<DataBlock>> {
        Ok(self.output_data.take())
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
        let (metas, local) = match self.input_data.take() {
            Some(meta) => {
                let bounds = meta.normalize_bounds::<R>();
                let metas = self
                    .state
                    .commit_sample(Some(SortExchangeMeta {
                        params: meta.params,
                        bounds,
                    }))
                    .await?;
                (metas, Some(meta))
            }
            None => (self.state.commit_sample(None).await?, None),
        };

        let params = match local.as_ref().map(|meta| meta.params) {
            Some(params) => params,
            None => {
                let Some(meta) = metas.first() else {
                    return Ok(());
                };
                meta.params
            }
        };

        let bounds_vec = metas.iter().map(|meta| meta.bounds.clone()).collect();
        let global_bounds = Bounds::merge::<R>(bounds_vec, self.state.batch_rows)?.dedup::<R>();

        self.output_data = Some(DataBlock::empty_with_meta(Box::new(SortCollectedMeta {
            params,
            bounds: global_bounds,
            blocks: local.map(|meta| meta.blocks).unwrap_or_default(),
        })));
        Ok(())
    }
}

impl SortCollectedMeta {
    fn normalize_bounds<R: Rows>(&self) -> Bounds {
        if self.bounds.len() > 1 {
            return self.bounds.dedup::<R>();
        }

        let Some(blocks) = self.blocks.get(self.blocks.len() / 2) else {
            return Bounds::default();
        };

        blocks
            .get(blocks.len() / 2)
            .map(|block| match block.domain.len() {
                0 => Bounds::default(),
                1 => Bounds::new_unchecked(block.domain.clone()),
                _ => Bounds::new_unchecked(block.domain.slice(0..1)).dedup::<R>(),
            })
            .unwrap_or_default()
    }
}
