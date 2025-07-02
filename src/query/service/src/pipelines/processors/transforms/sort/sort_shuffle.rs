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
use std::assert_matches::assert_matches;
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_transforms::processors::sort::Rows;

use super::bounds::Bounds;
use super::Base;
use super::SortCollectedMeta;
use super::SortScatteredMeta;
use crate::pipelines::processors::Event;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::Processor;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;

#[derive(Debug)]
enum Step {
    None,
    Meta(Box<SortCollectedMeta>),
    Scattered(Vec<Option<SortCollectedMeta>>),
}

pub struct TransformSortShuffle<R: Rows> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    step: Step,
    state: SortSampleState,
    spiller: Arc<Spiller>,
    _r: PhantomData<R>,
}

impl<R: Rows> TransformSortShuffle<R> {
    pub fn new(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        state: SortSampleState,
        spiller: Arc<Spiller>,
    ) -> Self {
        Self {
            input,
            output,
            state,
            spiller,
            step: Step::None,
            _r: PhantomData,
        }
    }

    async fn scatter(&mut self) -> Result<Vec<Option<SortCollectedMeta>>> {
        let SortCollectedMeta {
            params,
            bounds,
            blocks,
        } = match std::mem::replace(&mut self.step, Step::None) {
            Step::None => {
                return Ok(vec![]);
            }
            Step::Meta(box meta) => meta,
            _ => unreachable!(),
        };

        let scatter_bounds = self.state.bounds();
        if scatter_bounds.is_empty() {
            return Ok(vec![Some(SortCollectedMeta {
                params,
                bounds,
                blocks,
            })]);
        }

        let base = {
            let inner = &self.state.inner;
            Base {
                schema: inner.schema.clone(),
                spiller: self.spiller.clone(),
                sort_row_offset: inner.schema.fields.len() - 1,
                limit: None,
            }
        };

        let mut scattered_blocks = std::iter::repeat_with(Vec::new)
            .take(scatter_bounds.len() + 1)
            .collect::<Vec<_>>();
        for blocks in blocks {
            let scattered = base
                .scatter_stream::<R>(Vec::from(blocks).into(), scatter_bounds.clone())
                .await?;
            for (i, part) in scattered.into_iter().enumerate() {
                if !part.is_empty() {
                    scattered_blocks[i].push(part.into_boxed_slice());
                }
            }
        }

        let scattered_meta = scattered_blocks
            .into_iter()
            .map(|blocks| {
                (!blocks.is_empty()).then_some(SortCollectedMeta {
                    params,
                    bounds: bounds.clone(),
                    blocks,
                })
            })
            .collect();
        Ok(scattered_meta)
    }
}

#[async_trait::async_trait]
impl<R: Rows + 'static> Processor for TransformSortShuffle<R> {
    fn name(&self) -> String {
        "TransformSortShuffle".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if matches!(self.step, Step::Scattered(_)) {
            let Step::Scattered(scattered) = std::mem::replace(&mut self.step, Step::None) else {
                unreachable!()
            };

            let data = DataBlock::empty_with_meta(Box::new(SortScatteredMeta(scattered)));
            self.output.push_data(Ok(data));
            self.output.finish();
            return Ok(Event::Finished);
        }

        if let Some(mut block) = self.input.pull_data().transpose()? {
            assert_matches!(self.step, Step::None);
            let meta = block
                .take_meta()
                .and_then(SortCollectedMeta::downcast_from)
                .expect("require a SortCollectedMeta");

            self.step = Step::Meta(Box::new(meta));
            return Ok(Event::Async);
        }

        if self.input.is_finished() {
            if self.state.inner.bounds.is_some() {
                self.output.finish();
                Ok(Event::Finished)
            } else {
                Ok(Event::Async)
            }
        } else {
            self.input.set_need_data();
            Ok(Event::NeedData)
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let bounds = match &self.step {
            Step::None if self.input.is_finished() => Bounds::default(),
            Step::Meta(meta) => meta.generate_bounds(),
            _ => unreachable!(),
        };

        self.state.commit_sample::<R>(bounds).await?;
        self.step = Step::Scattered(self.scatter().await?);
        Ok(())
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

#[derive(Clone)]
pub struct SortSampleState {
    inner: StateInner,
    ctx: Arc<QueryContext>,
    broadcast_id: u32,
}

impl SortSampleState {
    pub fn new(
        inputs: usize,
        partitions: usize,
        schema: DataSchemaRef,
        batch_rows: usize,
        ctx: Arc<QueryContext>,
        broadcast_id: u32,
    ) -> SortSampleState {
        SortSampleState {
            inner: StateInner {
                partitions,
                schema,
                partial: vec![],
                bounds: None,
                batch_rows,
            },
            ctx,
            broadcast_id,
        }
    }

    pub async fn commit_sample<R: Rows>(&mut self, bounds: Bounds) -> Result<()> {
        let sender = self.ctx.broadcast_source_sender(self.broadcast_id);
        sender
            .send(Box::new(bounds))
            .await
            .map_err(|_| ErrorCode::TokioError("send sort bounds failed"))?;
        sender.close();

        let receiver = self.ctx.broadcast_sink_receiver(self.broadcast_id);
        while let Ok(r) = receiver.recv().await {
            self.inner.partial.push(Bounds::downcast_from(r).unwrap());
        }

        self.inner.determine_bounds::<R>()?;
        Ok(())
    }

    pub fn bounds(&self) -> Bounds {
        self.inner.bounds.clone().unwrap_or_default()
    }
}

#[derive(Clone)]

struct StateInner {
    // target partitions
    partitions: usize,
    schema: DataSchemaRef,
    partial: Vec<Bounds>,
    bounds: Option<Bounds>,
    batch_rows: usize,
}

impl StateInner {
    fn determine_bounds<R: Rows>(&mut self) -> Result<()> {
        let bounds = Bounds::merge::<R>(std::mem::take(&mut self.partial), self.batch_rows)?;

        let n = self.partitions - 1;
        let bounds = if bounds.len() < n {
            bounds
        } else {
            bounds.dedup_reduce::<R>(n)
        };
        assert!(bounds.len() < self.partitions);

        self.bounds = Some(bounds);
        Ok(())
    }
}
