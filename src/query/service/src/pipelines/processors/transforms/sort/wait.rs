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
use std::sync::Arc;
use std::sync::RwLock;

use databend_common_base::base::WatchNotify;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_transforms::processors::sort::select_row_type;
use databend_common_pipeline_transforms::processors::sort::Rows;
use databend_common_pipeline_transforms::processors::sort::RowsTypeVisitor;
use databend_common_pipeline_transforms::sort::RowConverter;

use super::bounds::Bounds;
use super::SortCollectedMeta;
use crate::pipelines::processors::Event;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::Processor;

pub struct TransformSortSampleWait {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    schema: DataSchemaRef,
    sort_desc: Arc<[SortColumnDescription]>,
    id: usize,
    meta: Option<Box<SortCollectedMeta>>,
    state: Arc<SortSampleState>,
}

impl TransformSortSampleWait {
    pub fn new(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        id: usize,
        schema: DataSchemaRef,
        sort_desc: Arc<[SortColumnDescription]>,
        state: Arc<SortSampleState>,
    ) -> Self {
        Self {
            input,
            output,
            id,
            state,
            schema,
            sort_desc,
            meta: None,
        }
    }
}

#[async_trait::async_trait]
impl Processor for TransformSortSampleWait {
    fn name(&self) -> String {
        "TransformSortSimpleWait".to_string()
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

        if let Some(meta) = self.meta.take() {
            self.output.push_data(Ok(DataBlock::empty_with_meta(meta)));
            self.output.finish();
            return Ok(Event::Finished);
        }

        if let Some(mut block) = self.input.pull_data().transpose()? {
            assert!(self.meta.is_none());
            let meta = block
                .take_meta()
                .and_then(SortCollectedMeta::downcast_from)
                .expect("require a SortCollectedMeta");

            self.meta = Some(Box::new(meta));
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
        let bounds = self
            .meta
            .as_ref()
            .map(|meta| meta.bounds.clone())
            .unwrap_or_default();

        let mut commit = CommitSample {
            inner: self,
            bounds: Some(bounds),
            result: Ok(false),
        };
        select_row_type(&mut commit);
        commit.result?;
        self.state.done.notified().await;
        Ok(())
    }
}

struct CommitSample<'a> {
    inner: &'a TransformSortSampleWait,
    bounds: Option<Bounds>,
    result: Result<bool>,
}

impl<'a> RowsTypeVisitor for CommitSample<'a> {
    fn schema(&self) -> DataSchemaRef {
        self.inner.schema.clone()
    }

    fn sort_desc(&self) -> &[SortColumnDescription] {
        &self.inner.sort_desc
    }

    fn visit_type<R, C>(&mut self)
    where
        R: Rows + 'static,
        C: RowConverter<R> + Send + 'static,
    {
        self.result = self
            .inner
            .state
            .commit_sample::<R>(self.inner.id, self.bounds.take().unwrap());
    }
}

pub struct SortSampleState {
    inner: RwLock<StateInner>,
    pub(super) done: WatchNotify,
}

impl SortSampleState {
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
}

struct StateInner {
    // target partitions
    partitions: usize,
    // schema for bounds DataBlock
    // schema: DataSchemaRef,
    // sort_desc for bounds DataBlock
    // sort_desc: Vec<SortColumnDescription>,
    partial: Vec<Option<Bounds>>,
    bounds: Option<Bounds>,
    batch_rows: usize,
}

impl StateInner {
    fn determine_bounds<R: Rows>(&mut self) -> Result<()> {
        let v = self.partial.drain(..).map(Option::unwrap).collect();
        let bounds = Bounds::merge::<R>(v, self.batch_rows)?;
        let bounds = bounds
            .reduce(self.partitions - 1, R::data_type())
            .unwrap_or(bounds);
        assert!(bounds.len() <= self.partitions - 1);

        self.bounds = Some(bounds);
        Ok(())
    }
}
