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
use std::sync::RwLock;

use databend_common_base::base::WatchNotify;
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
    id: usize,
    step: Step,
    state: Arc<SortSampleState>,
    spiller: Arc<Spiller>,
    _r: PhantomData<R>,
}

impl<R: Rows> TransformSortShuffle<R> {
    pub fn new(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        id: usize,
        state: Arc<SortSampleState>,
        spiller: Arc<Spiller>,
    ) -> Self {
        Self {
            input,
            output,
            id,
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
            let inner = self.state.inner.read().unwrap();
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
            if self.state.done.has_notified() {
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
        self.state.commit_sample::<R>(self.id, bounds)?;
        self.state.done.notified().await;
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

pub struct SortSampleState {
    inner: RwLock<StateInner>,
    pub(super) done: WatchNotify,
}

impl SortSampleState {
    pub fn new(
        inputs: usize,
        partitions: usize,
        schema: DataSchemaRef,
        batch_rows: usize,
    ) -> Arc<SortSampleState> {
        Arc::new(SortSampleState {
            inner: RwLock::new(StateInner {
                partitions,
                schema,
                partial: vec![None; inputs],
                bounds: None,
                batch_rows,
            }),
            done: WatchNotify::new(),
        })
    }

    pub fn commit_sample<R: Rows>(&self, id: usize, bounds: Bounds) -> Result<bool> {
        let mut inner = self.inner.write().unwrap();

        let x = inner.partial[id].replace(bounds);
        assert!(x.is_none());
        let done = inner.partial.iter().all(Option::is_some);
        if done {
            inner.determine_bounds::<R>()?;
            self.done.notify_waiters();
        }
        Ok(done)
    }

    pub fn bounds(&self) -> Bounds {
        self.inner
            .read()
            .unwrap()
            .bounds
            .clone()
            .unwrap_or_default()
    }
}

struct StateInner {
    // target partitions
    partitions: usize,
    schema: DataSchemaRef,
    partial: Vec<Option<Bounds>>,
    bounds: Option<Bounds>,
    batch_rows: usize,
}

impl StateInner {
    fn determine_bounds<R: Rows>(&mut self) -> Result<()> {
        let v = self.partial.drain(..).map(Option::unwrap).collect();
        let bounds = Bounds::merge::<R>(v, self.batch_rows)?;

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
