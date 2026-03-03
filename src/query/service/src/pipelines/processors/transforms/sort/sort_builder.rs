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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::sorts::Base;
use databend_common_pipeline_transforms::sorts::BoundedMultiSortMergeProcessor;
use databend_common_pipeline_transforms::sorts::BroadcastChannel;
use databend_common_pipeline_transforms::sorts::SortBoundEdge;
use databend_common_pipeline_transforms::sorts::SortDummyRoute;
use databend_common_pipeline_transforms::sorts::SortSampleState;
use databend_common_pipeline_transforms::sorts::TransformSort;
use databend_common_pipeline_transforms::sorts::TransformSortBoundBroadcast;
use databend_common_pipeline_transforms::sorts::TransformSortCollect;
use databend_common_pipeline_transforms::sorts::TransformSortRestore;
use databend_common_pipeline_transforms::sorts::TransformSortRoute;
use databend_common_pipeline_transforms::sorts::core::RowConverter;
use databend_common_pipeline_transforms::sorts::core::Rows;
use databend_common_pipeline_transforms::sorts::core::RowsTypeVisitor;
use databend_common_pipeline_transforms::sorts::core::SortKeyDescription;
use databend_common_pipeline_transforms::sorts::core::algorithm::HeapSort;
use databend_common_pipeline_transforms::sorts::core::algorithm::LoserTreeSort;
use databend_common_pipeline_transforms::sorts::core::algorithm::SortAlgorithm;
use databend_common_pipeline_transforms::sorts::core::select_row_type;
use databend_common_pipeline_transforms::traits::SortSpiller;

use super::*;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::sessions::QueryContext;

enum SortType {
    Sort(Arc<InputPort>),

    Collect {
        input: Arc<InputPort>,
        default_num_merge: usize,
    },
    BoundBroadcast {
        input: Arc<InputPort>,
        state: SortSampleState<ContextChannel>,
    },
    Restore(Arc<InputPort>),

    BoundedMergeSort(Vec<Arc<InputPort>>),
}

pub struct TransformSortBuilder<S: SortSpiller> {
    key_desc: SortKeyDescription,
    block_size: usize,
    order_col_generated: bool,
    output_order_col: bool,
    spiller: Option<S>,
    enable_loser_tree: bool,
    limit: Option<usize>,
    enable_fixed_rows: bool,
    enable_restore_prefetch: bool,
}

impl<S: SortSpiller> TransformSortBuilder<S> {
    pub fn new(
        key_desc: SortKeyDescription,
        block_size: usize,
        enable_fixed_rows: bool,
    ) -> TransformSortBuilder<S> {
        TransformSortBuilder {
            key_desc,
            block_size,
            spiller: None,
            order_col_generated: false,
            output_order_col: false,
            enable_loser_tree: false,
            limit: None,
            enable_fixed_rows,
            enable_restore_prefetch: false,
        }
    }

    pub fn with_spiller(mut self, spiller: S) -> Self {
        self.spiller = Some(spiller);
        self
    }

    pub fn with_order_column(mut self, generated: bool, output: bool) -> Self {
        self.order_col_generated = generated;
        self.output_order_col = output;
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_enable_restore_prefetch(mut self, enabled: bool) -> Self {
        self.enable_restore_prefetch = enabled;
        self
    }

    pub fn with_enable_loser_tree(mut self, enable_loser_tree: bool) -> Self {
        self.enable_loser_tree = enable_loser_tree;
        self
    }

    pub fn build(
        &self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<Box<dyn Processor>> {
        let mut build = Build {
            params: self,
            output,
            typ: Some(SortType::Sort(input)),
        };

        select_row_type(&mut build, self.enable_fixed_rows)
    }

    pub fn build_collect(
        &self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        default_num_merge: usize,
    ) -> Result<Box<dyn Processor>> {
        let mut build = Build {
            params: self,
            output,
            typ: Some(SortType::Collect {
                input,
                default_num_merge,
            }),
        };

        select_row_type(&mut build, self.enable_fixed_rows)
    }

    fn build_bound_broadcast(
        &self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        state: SortSampleState<ContextChannel>,
    ) -> Result<Box<dyn Processor>> {
        let mut build = Build {
            params: self,
            output,
            typ: Some(SortType::BoundBroadcast { input, state }),
        };

        select_row_type(&mut build, self.enable_fixed_rows)
    }

    pub fn build_restore(
        &self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<Box<dyn Processor>> {
        let mut build = Build {
            params: self,
            output,
            typ: Some(SortType::Restore(input)),
        };

        select_row_type(&mut build, self.enable_fixed_rows)
    }

    pub fn build_bound_edge(
        &self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<Box<dyn Processor>> {
        Ok(Box::new(SortBoundEdge::new(input, output)))
    }

    pub fn build_bounded_merge_sort(
        &self,
        inputs: Vec<Arc<InputPort>>,
        output: Arc<OutputPort>,
    ) -> Result<Box<dyn Processor>> {
        let mut build = Build {
            params: self,
            output,
            typ: Some(SortType::BoundedMergeSort(inputs)),
        };

        select_row_type(&mut build, self.enable_fixed_rows)
    }

    fn should_use_sort_limit(&self) -> bool {
        self.limit.map(|limit| limit < 10000).unwrap_or_default()
    }

    fn new_base(&self) -> Base<S> {
        Base {
            sort_row_offset: self.key_desc.sort_row_offset(),
            spiller: self.spiller.clone().unwrap(),
            limit: self.limit,
        }
    }

    pub fn add_bound_broadcast(
        &self,
        pipeline: &mut Pipeline,
        batch_rows: usize,
        ctx: Arc<QueryContext>,
        id: u32,
    ) -> Result<()> {
        let state = SortSampleState::new(batch_rows, ContextChannel { ctx, id });

        pipeline.resize(1, false)?;
        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(self.build_bound_broadcast(
                input,
                output,
                state.clone(),
            )?))
        })
    }

    pub fn add_route(pipeline: &mut Pipeline) -> Result<()> {
        let inputs = pipeline.output_len();
        let inputs_port: Vec<_> = (0..inputs).map(|_| InputPort::create()).collect();
        let output = OutputPort::create();

        let processor = ProcessorPtr::create(Box::new(TransformSortRoute::new(
            inputs_port.clone(),
            output.clone(),
        )));

        let pipe = Pipe::create(inputs, 1, vec![PipeItem::create(
            processor,
            inputs_port,
            vec![output],
        )]);

        pipeline.add_pipe(pipe);
        Ok(())
    }

    pub fn build_dummy_route() -> SortDummyRoute {
        SortDummyRoute
    }

    pub fn exchange_injector() -> Arc<dyn ExchangeInjector> {
        Arc::new(SortInjector {})
    }
}

struct Build<'a, S: SortSpiller> {
    params: &'a TransformSortBuilder<S>,
    typ: Option<SortType>,
    output: Arc<OutputPort>,
}

impl<S: SortSpiller> Build<'_, S> {
    fn build_sort<A>(
        &mut self,
        sort_limit: bool,
        input: Arc<InputPort>,
    ) -> Result<Box<dyn Processor>>
    where
        A: SortAlgorithm + 'static,
        <A::Rows as Rows>::Converter: Send + 'static,
    {
        let base_row_converter = self.params.key_desc.clone();
        let uses_source_sort_col = base_row_converter.uses_source_sort_col();
        let sort_row_offset = base_row_converter.sort_row_offset();
        let row_converter = <A::Rows as Rows>::Converter::new(base_row_converter)?;
        Ok(Box::new(TransformSort::<A, S>::new(
            input,
            self.output.clone(),
            sort_row_offset,
            row_converter,
            self.params.block_size,
            self.params.limit.map(|limit| (limit, sort_limit)),
            self.params.spiller.clone().unwrap(),
            self.params.output_order_col || uses_source_sort_col,
            self.params.order_col_generated || uses_source_sort_col,
            self.params.enable_restore_prefetch,
        )?))
    }

    fn build_sort_collect<A>(
        &mut self,
        input: Arc<InputPort>,
        sort_limit: bool,
        default_num_merge: usize,
    ) -> Result<Box<dyn Processor>>
    where
        A: SortAlgorithm + 'static,
        <A::Rows as Rows>::Converter: Send + 'static,
    {
        assert!(!self.params.order_col_generated);
        let uses_source_sort_col = self.params.key_desc.uses_source_sort_col();
        let row_converter = <A::Rows as Rows>::Converter::new(self.params.key_desc.clone())?;
        Ok(Box::new(TransformSortCollect::<A, S>::new(
            input,
            self.output.clone(),
            self.params.new_base(),
            self.params.block_size,
            default_num_merge,
            sort_limit,
            if uses_source_sort_col {
                None
            } else {
                Some(row_converter)
            },
            self.params.enable_restore_prefetch,
        )?))
    }

    fn build_sort_restore<A>(&mut self, input: Arc<InputPort>) -> Result<Box<dyn Processor>>
    where A: SortAlgorithm + 'static {
        let uses_source_sort_col = self.params.key_desc.uses_source_sort_col();
        Ok(Box::new(TransformSortRestore::<A, S>::create(
            input,
            self.output.clone(),
            self.params.new_base(),
            self.params.output_order_col || uses_source_sort_col,
        )?))
    }

    fn build_bound_broadcast<R, C>(
        &mut self,
        input: Arc<InputPort>,
        state: SortSampleState<C>,
    ) -> Result<Box<dyn Processor>>
    where
        R: Rows + 'static,
        C: BroadcastChannel,
    {
        Ok(TransformSortBoundBroadcast::<R, C>::create(
            input,
            self.output.clone(),
            state,
        ))
    }

    fn build_bounded_merge_sort<A>(
        &mut self,
        inputs: Vec<Arc<InputPort>>,
    ) -> Result<Box<dyn Processor>>
    where
        A: SortAlgorithm + 'static,
    {
        Ok(Box::new(BoundedMultiSortMergeProcessor::<A>::new(
            inputs,
            self.output.clone(),
            self.params.key_desc.clone(),
            self.params.block_size,
        )?))
    }
}

impl<S: SortSpiller> RowsTypeVisitor for Build<'_, S> {
    type Result = Result<Box<dyn Processor>>;
    fn sort_key_desc(&self) -> SortKeyDescription {
        self.params.key_desc.clone()
    }

    fn visit_type<R>(&mut self) -> Self::Result
    where
        R: Rows + 'static,
        <R as Rows>::Converter: Send + 'static,
    {
        let sort_limit = self.params.should_use_sort_limit();
        match self.typ.take().unwrap() {
            SortType::Sort(input) => match self.params.enable_loser_tree {
                true => self.build_sort::<LoserTreeSort<R>>(sort_limit, input),
                false => self.build_sort::<HeapSort<R>>(sort_limit, input),
            },

            SortType::Collect {
                input,
                default_num_merge,
            } => match self.params.enable_loser_tree {
                true => self.build_sort_collect::<LoserTreeSort<R>>(
                    input,
                    sort_limit,
                    default_num_merge,
                ),
                false => {
                    self.build_sort_collect::<HeapSort<R>>(input, sort_limit, default_num_merge)
                }
            },
            SortType::BoundBroadcast { input, state } => {
                self.build_bound_broadcast::<R, _>(input, state)
            }
            SortType::Restore(input) => match self.params.enable_loser_tree {
                true => self.build_sort_restore::<LoserTreeSort<R>>(input),
                false => self.build_sort_restore::<HeapSort<R>>(input),
            },

            SortType::BoundedMergeSort(inputs) => match self.params.enable_loser_tree {
                true => self.build_bounded_merge_sort::<LoserTreeSort<R>>(inputs),
                false => self.build_bounded_merge_sort::<HeapSort<R>>(inputs),
            },
        }
    }
}

#[derive(Clone)]
struct ContextChannel {
    ctx: Arc<QueryContext>,
    id: u32,
}

impl BroadcastChannel for ContextChannel {
    fn sender(&self) -> async_channel::Sender<DataBlock> {
        self.ctx.broadcast_source_sender(self.id)
    }

    fn receiver(&self) -> async_channel::Receiver<DataBlock> {
        self.ctx.broadcast_sink_receiver(self.id)
    }
}
