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
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::sort::Rows;
use databend_common_pipeline_transforms::AsyncAccumulatingTransform;
use databend_common_pipeline_transforms::AsyncAccumulatingTransformer;

use super::bounds::Bounds;
use super::SortCollectedMeta;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::sessions::QueryContext;

pub struct TransformSortBoundBroadcast<R: Rows> {
    buffer: Vec<SortCollectedMeta>,
    state: SortSampleState,
    _r: PhantomData<R>,
}

impl<R: Rows + 'static> TransformSortBoundBroadcast<R> {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        state: SortSampleState,
    ) -> Box<dyn Processor> {
        AsyncAccumulatingTransformer::create(input, output, Self {
            buffer: Vec::new(),
            state,
            _r: PhantomData,
        })
    }
}

#[derive(Clone)]
pub struct SortSampleState {
    ctx: Arc<QueryContext>,
    broadcast_id: u32,
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

    pub async fn commit_sample<R: Rows>(&mut self, bounds: Bounds) -> Result<Bounds> {
        let sender = self.ctx.broadcast_source_sender(self.broadcast_id);
        sender
            .send(Box::new(bounds))
            .await
            .map_err(|_| ErrorCode::TokioError("send sort bounds failed"))?;
        sender.close();

        let receiver = self.ctx.broadcast_sink_receiver(self.broadcast_id);
        let mut partials = Vec::new();
        while let Ok(r) = receiver.recv().await {
            partials.push(Bounds::downcast_from(r).unwrap());
        }

        Bounds::merge::<R>(partials, self.batch_rows)
    }
}

#[async_trait::async_trait]
impl<R: Rows + 'static> AsyncAccumulatingTransform for TransformSortBoundBroadcast<R> {
    const NAME: &'static str = "TransformSortBoundBroadcast";

    async fn transform(&mut self, mut data: DataBlock) -> Result<Option<DataBlock>> {
        let meta = data
            .take_meta()
            .and_then(SortCollectedMeta::downcast_from)
            .expect("require a SortCollectedMeta");
        self.buffer.push(meta);
        Ok(None)
    }

    async fn on_finish(&mut self, output: bool) -> Result<Option<DataBlock>> {
        if !output {
            return Ok(None);
        }

        let Some(params) = self.buffer.first().map(|meta| meta.params) else {
            return Ok(None);
        };

        let bounds_vec = self
            .buffer
            .iter()
            .map(|meta| meta.bounds.dedup::<R>())
            .collect();
        let bounds = Bounds::merge::<R>(bounds_vec, self.state.batch_rows)?;

        let blocks = self
            .buffer
            .drain(..)
            .flat_map(|meta| meta.blocks.into_iter())
            .collect();

        let local = SortCollectedMeta {
            params,
            bounds,
            blocks,
        };

        let global_bounds = self
            .state
            .commit_sample::<R>(local.generate_bounds())
            .await?;

        Ok(Some(DataBlock::empty_with_meta(Box::new(
            SortCollectedMeta {
                bounds: global_bounds,
                ..local
            },
        ))))
    }
}

impl SortCollectedMeta {
    fn generate_bounds(&self) -> Bounds {
        if self.bounds.len() > 1 {
            return self.bounds.clone();
        }

        let Some(blocks) = self.blocks.get(self.blocks.len() / 2) else {
            return Bounds::default();
        };

        blocks
            .get(blocks.len() / 2)
            .map(|block| match block.domain.len() {
                0 => Bounds::default(),
                1 => Bounds::new_unchecked(block.domain.clone()),
                _ => Bounds::new_unchecked(block.domain.slice(0..1)),
            })
            .unwrap_or_default()
    }
}
