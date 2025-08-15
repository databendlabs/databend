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
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_transforms::processors::sort::algorithm::SortAlgorithm;
use databend_common_pipeline_transforms::sort::algorithm::HeapSort;
use databend_common_pipeline_transforms::sort::algorithm::LoserTreeSort;
use databend_common_pipeline_transforms::sort::select_row_type;
use databend_common_pipeline_transforms::sort::utils::add_order_field;
use databend_common_pipeline_transforms::sort::utils::ORDER_COL_NAME;
use databend_common_pipeline_transforms::sort::RowConverter;
use databend_common_pipeline_transforms::sort::Rows;
use databend_common_pipeline_transforms::sort::RowsTypeVisitor;
use databend_common_pipeline_transforms::MemorySettings;

use super::*;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;

enum SortType {
    Sort(Arc<InputPort>),

    Collect {
        input: Arc<InputPort>,
        default_num_merge: usize,
    },
    BoundBroadcast {
        input: Arc<InputPort>,
        state: SortSampleState,
    },
    Restore(Arc<InputPort>),

    BoundedMergeSort(Vec<Arc<InputPort>>),
}

pub struct TransformSortBuilder {
    schema: DataSchemaRef,
    block_size: usize,
    sort_desc: Arc<[SortColumnDescription]>,
    order_col_generated: bool,
    output_order_col: bool,
    memory_settings: MemorySettings,
    spiller: Option<Arc<Spiller>>,
    enable_loser_tree: bool,
    limit: Option<usize>,
}

impl TransformSortBuilder {
    pub fn new(
        schema: DataSchemaRef,
        sort_desc: Arc<[SortColumnDescription]>,
        block_size: usize,
    ) -> Self {
        TransformSortBuilder {
            block_size,
            schema,
            sort_desc,
            spiller: None,
            order_col_generated: false,
            output_order_col: false,
            enable_loser_tree: false,
            limit: None,
            memory_settings: MemorySettings::disable_spill(),
        }
    }

    pub fn with_spiller(mut self, spiller: Arc<Spiller>) -> Self {
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

    pub fn with_memory_settings(mut self, memory_settings: MemorySettings) -> Self {
        self.memory_settings = memory_settings;
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
        self.check();

        let mut build = Build {
            params: self,
            output,
            typ: Some(SortType::Sort(input)),
        };

        select_row_type(&mut build)
    }

    pub fn build_collect(
        &self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        default_num_merge: usize,
    ) -> Result<Box<dyn Processor>> {
        self.check();

        let mut build = Build {
            params: self,
            output,
            typ: Some(SortType::Collect {
                input,
                default_num_merge,
            }),
        };

        select_row_type(&mut build)
    }

    pub fn build_bound_broadcast(
        &self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        state: SortSampleState,
    ) -> Result<Box<dyn Processor>> {
        self.check();

        let mut build = Build {
            params: self,
            output,
            typ: Some(SortType::BoundBroadcast { input, state }),
        };

        select_row_type(&mut build)
    }

    pub fn build_restore(
        &self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<Box<dyn Processor>> {
        self.check();

        let mut build = Build {
            params: self,
            output,
            typ: Some(SortType::Restore(input)),
        };

        select_row_type(&mut build)
    }

    pub fn build_bound_edge(
        &self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<Box<dyn Processor>> {
        self.check();

        Ok(Box::new(SortBoundEdge::new(input, output)))
    }

    pub fn build_bounded_merge_sort(
        &self,
        inputs: Vec<Arc<InputPort>>,
        output: Arc<OutputPort>,
    ) -> Result<Box<dyn Processor>> {
        self.check();

        let mut build = Build {
            params: self,
            output,
            typ: Some(SortType::BoundedMergeSort(inputs)),
        };

        select_row_type(&mut build)
    }

    fn should_use_sort_limit(&self) -> bool {
        self.limit.map(|limit| limit < 10000).unwrap_or_default()
    }

    fn check(&self) {
        assert_eq!(self.schema.has_field(ORDER_COL_NAME), self.output_order_col)
    }

    fn new_base(&self) -> Base {
        let schema = self.inner_schema();
        let sort_row_offset = schema.fields().len() - 1;
        Base {
            sort_row_offset,
            schema,
            spiller: self.spiller.clone().unwrap(),
            limit: self.limit,
        }
    }

    fn inner_schema(&self) -> DataSchemaRef {
        add_order_field(self.schema.clone(), &self.sort_desc)
    }

    pub fn add_bound_broadcast(
        &self,
        pipeline: &mut Pipeline,
        batch_rows: usize,
        ctx: Arc<QueryContext>,
        broadcast_id: u32,
    ) -> Result<()> {
        let state = SortSampleState::new(batch_rows, ctx, broadcast_id);

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
        SortDummyRoute {}
    }

    pub fn exchange_injector() -> Arc<dyn ExchangeInjector> {
        Arc::new(SortInjector {})
    }
}

struct Build<'a> {
    params: &'a TransformSortBuilder,
    typ: Option<SortType>,
    output: Arc<OutputPort>,
}

impl Build<'_> {
    fn build_sort<A, C>(
        &mut self,
        sort_limit: bool,
        input: Arc<InputPort>,
    ) -> Result<Box<dyn Processor>>
    where
        A: SortAlgorithm + 'static,
        C: RowConverter<A::Rows> + Send + 'static,
    {
        let schema = add_order_field(self.params.schema.clone(), &self.params.sort_desc);
        Ok(Box::new(TransformSort::<A, C>::new(
            input,
            self.output.clone(),
            schema,
            self.params.sort_desc.clone(),
            self.params.block_size,
            self.params.limit.map(|limit| (limit, sort_limit)),
            self.params.spiller.clone().unwrap(),
            self.params.output_order_col,
            self.params.order_col_generated,
            self.params.memory_settings.clone(),
        )?))
    }

    fn build_sort_collect<A, C>(
        &mut self,
        input: Arc<InputPort>,
        sort_limit: bool,
        default_num_merge: usize,
    ) -> Result<Box<dyn Processor>>
    where
        A: SortAlgorithm + 'static,
        C: RowConverter<A::Rows> + Send + 'static,
    {
        Ok(Box::new(TransformSortCollect::<A, C>::new(
            input,
            self.output.clone(),
            self.params.new_base(),
            self.params.sort_desc.clone(),
            self.params.block_size,
            default_num_merge,
            sort_limit,
            self.params.order_col_generated,
            self.params.memory_settings.clone(),
        )?))
    }

    fn build_sort_restore<A>(&mut self, input: Arc<InputPort>) -> Result<Box<dyn Processor>>
    where A: SortAlgorithm + 'static {
        Ok(Box::new(TransformSortRestore::<A>::create(
            input,
            self.output.clone(),
            self.params.new_base(),
            self.params.output_order_col,
            self.params.memory_settings.clone(),
        )?))
    }

    fn build_bound_broadcast<R>(
        &mut self,
        input: Arc<InputPort>,
        state: SortSampleState,
    ) -> Result<Box<dyn Processor>>
    where
        R: Rows + 'static,
    {
        Ok(TransformSortBoundBroadcast::<R>::create(
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
            self.params.schema.clone(),
            self.params.block_size,
        )?))
    }
}

impl RowsTypeVisitor for Build<'_> {
    type Result = Result<Box<dyn Processor>>;
    fn schema(&self) -> DataSchemaRef {
        self.params.schema.clone()
    }

    fn sort_desc(&self) -> &[SortColumnDescription] {
        &self.params.sort_desc
    }

    fn visit_type<R, C>(&mut self) -> Self::Result
    where
        R: Rows + 'static,
        C: RowConverter<R> + Send + 'static,
    {
        let sort_limit = self.params.should_use_sort_limit();
        match self.typ.take().unwrap() {
            SortType::Sort(input) => match self.params.enable_loser_tree {
                true => self.build_sort::<LoserTreeSort<R>, C>(sort_limit, input),
                false => self.build_sort::<HeapSort<R>, C>(sort_limit, input),
            },

            SortType::Collect {
                input,
                default_num_merge,
            } => match self.params.enable_loser_tree {
                true => self.build_sort_collect::<LoserTreeSort<R>, C>(
                    input,
                    sort_limit,
                    default_num_merge,
                ),
                false => {
                    self.build_sort_collect::<HeapSort<R>, C>(input, sort_limit, default_num_merge)
                }
            },
            SortType::BoundBroadcast { input, state } => {
                self.build_bound_broadcast::<R>(input, state)
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
