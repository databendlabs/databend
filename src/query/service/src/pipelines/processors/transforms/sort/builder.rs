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

use super::collect::TransformSortCollect;
use super::execute::TransformSortExecute;
use super::merge_sort::TransformSort;
use super::shuffle::SortSampleState;
use super::shuffle::TransformSortShuffle;
use crate::spillers::Spiller;

enum SortType {
    Sort,
    Collect,
    Execute,
    Shuffle,
}

pub struct TransformSortBuilder {
    schema: DataSchemaRef,
    block_size: usize,
    sort_desc: Arc<[SortColumnDescription]>,
    order_col_generated: bool,
    output_order_col: bool,
    memory_settings: MemorySettings,
    spiller: Arc<Spiller>,
    enable_loser_tree: bool,
    limit: Option<usize>,
}

impl TransformSortBuilder {
    pub fn create(
        schema: DataSchemaRef,
        sort_desc: Arc<[SortColumnDescription]>,
        block_size: usize,
        spiller: Arc<Spiller>,
    ) -> Self {
        TransformSortBuilder {
            block_size,
            schema,
            sort_desc,
            spiller,
            order_col_generated: false,
            output_order_col: false,
            enable_loser_tree: false,
            limit: None,
            memory_settings: MemorySettings::disable_spill(),
        }
    }

    pub fn with_order_col_generated(mut self, order_col_generated: bool) -> Self {
        self.order_col_generated = order_col_generated;
        self
    }

    pub fn with_output_order_col(mut self, output_order_col: bool) -> Self {
        self.output_order_col = output_order_col;
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
            input,
            output,
            processor: None,
            typ: SortType::Sort,
            id: 0,
            state: None,
        };

        select_row_type(&mut build);
        build.processor.unwrap()
    }

    pub fn build_collect(
        &self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<Box<dyn Processor>> {
        self.check();

        let mut build = Build {
            params: self,
            input,
            output,
            processor: None,
            typ: SortType::Collect,
            id: 0,
            state: None,
        };

        select_row_type(&mut build);
        build.processor.unwrap()
    }

    pub fn build_exec(
        &self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<Box<dyn Processor>> {
        self.check();

        let mut build = Build {
            params: self,
            input,
            output,
            processor: None,
            typ: SortType::Execute,
            id: 0,
            state: None,
        };

        select_row_type(&mut build);
        build.processor.unwrap()
    }

    pub fn build_shuffle(
        &self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        id: usize,
        state: Arc<SortSampleState>,
    ) -> Result<Box<dyn Processor>> {
        self.check();

        let mut build = Build {
            params: self,
            input,
            output,
            processor: None,
            typ: SortType::Shuffle,
            id,
            state: Some(state),
        };

        select_row_type(&mut build);
        build.processor.unwrap()
    }

    fn should_use_sort_limit(&self) -> bool {
        self.limit.map(|limit| limit < 10000).unwrap_or_default()
    }

    fn check(&self) {
        assert!(if self.output_order_col {
            self.schema.has_field(ORDER_COL_NAME)
        } else {
            !self.schema.has_field(ORDER_COL_NAME)
        });
    }
}

pub struct Build<'a> {
    params: &'a TransformSortBuilder,
    typ: SortType,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    processor: Option<Result<Box<dyn Processor>>>,
    id: usize,
    state: Option<Arc<SortSampleState>>,
}

impl Build<'_> {
    fn build_sort<A, C>(&mut self) -> Result<Box<dyn Processor>>
    where
        A: SortAlgorithm + 'static,
        C: RowConverter<A::Rows> + Send + 'static,
    {
        let schema = add_order_field(self.params.schema.clone(), &self.params.sort_desc);
        Ok(Box::new(TransformSort::<A, C>::new(
            self.input.clone(),
            self.output.clone(),
            schema,
            self.params.sort_desc.clone(),
            self.params.block_size,
            self.params.limit.map(|limit| (limit, false)),
            self.params.spiller.clone(),
            self.params.output_order_col,
            self.params.order_col_generated,
            self.params.memory_settings.clone(),
        )?))
    }

    fn build_sort_limit<A, C>(&mut self) -> Result<Box<dyn Processor>>
    where
        A: SortAlgorithm + 'static,
        C: RowConverter<A::Rows> + Send + 'static,
    {
        let schema = add_order_field(self.params.schema.clone(), &self.params.sort_desc);
        Ok(Box::new(TransformSort::<A, C>::new(
            self.input.clone(),
            self.output.clone(),
            schema,
            self.params.sort_desc.clone(),
            self.params.block_size,
            Some((self.params.limit.unwrap(), true)),
            self.params.spiller.clone(),
            self.params.output_order_col,
            self.params.order_col_generated,
            self.params.memory_settings.clone(),
        )?))
    }

    fn build_sort_collect<A, C>(&mut self) -> Result<Box<dyn Processor>>
    where
        A: SortAlgorithm + 'static,
        C: RowConverter<A::Rows> + Send + 'static,
    {
        let schema = add_order_field(self.params.schema.clone(), &self.params.sort_desc);

        Ok(Box::new(TransformSortCollect::<A, C>::new(
            self.input.clone(),
            self.output.clone(),
            schema,
            self.params.sort_desc.clone(),
            self.params.block_size,
            self.params.limit.map(|limit| (limit, false)),
            self.params.spiller.clone(),
            self.params.order_col_generated,
            self.params.memory_settings.clone(),
        )?))
    }

    fn build_sort_limit_collect<A, C>(&mut self) -> Result<Box<dyn Processor>>
    where
        A: SortAlgorithm + 'static,
        C: RowConverter<A::Rows> + Send + 'static,
    {
        let schema = add_order_field(self.params.schema.clone(), &self.params.sort_desc);
        Ok(Box::new(TransformSortCollect::<A, C>::new(
            self.input.clone(),
            self.output.clone(),
            schema,
            self.params.sort_desc.clone(),
            self.params.block_size,
            Some((self.params.limit.unwrap(), true)),
            self.params.spiller.clone(),
            self.params.order_col_generated,
            self.params.memory_settings.clone(),
        )?))
    }

    fn build_sort_exec<A>(&mut self) -> Result<Box<dyn Processor>>
    where A: SortAlgorithm + 'static {
        let schema = add_order_field(self.params.schema.clone(), &self.params.sort_desc);

        Ok(Box::new(TransformSortExecute::<A>::new(
            self.input.clone(),
            self.output.clone(),
            schema,
            self.params.limit,
            self.params.spiller.clone(),
            self.params.output_order_col,
        )?))
    }

    fn build_sort_shuffle<R>(&mut self) -> Result<Box<dyn Processor>>
    where R: Rows + 'static {
        Ok(Box::new(TransformSortShuffle::<R>::new(
            self.input.clone(),
            self.output.clone(),
            self.id,
            self.state.clone().unwrap(),
            self.params.spiller.clone(),
        )))
    }
}

impl RowsTypeVisitor for Build<'_> {
    fn schema(&self) -> DataSchemaRef {
        self.params.schema.clone()
    }

    fn sort_desc(&self) -> &[SortColumnDescription] {
        &self.params.sort_desc
    }

    fn visit_type<R, C>(&mut self)
    where
        R: Rows + 'static,
        C: RowConverter<R> + Send + 'static,
    {
        let processor = match self.typ {
            SortType::Sort => match (
                self.params.should_use_sort_limit(),
                self.params.enable_loser_tree,
            ) {
                (true, true) => self.build_sort_limit::<LoserTreeSort<R>, C>(),
                (true, false) => self.build_sort_limit::<HeapSort<R>, C>(),
                (false, true) => self.build_sort::<LoserTreeSort<R>, C>(),
                (false, false) => self.build_sort::<HeapSort<R>, C>(),
            },
            SortType::Collect => match (
                self.params.should_use_sort_limit(),
                self.params.enable_loser_tree,
            ) {
                (true, true) => self.build_sort_limit_collect::<LoserTreeSort<R>, C>(),
                (true, false) => self.build_sort_limit_collect::<HeapSort<R>, C>(),
                (false, true) => self.build_sort_collect::<LoserTreeSort<R>, C>(),
                (false, false) => self.build_sort_collect::<HeapSort<R>, C>(),
            },
            SortType::Execute => match self.params.enable_loser_tree {
                true => self.build_sort_exec::<LoserTreeSort<R>>(),
                false => self.build_sort_exec::<HeapSort<R>>(),
            },
            SortType::Shuffle => self.build_sort_shuffle::<R>(),
        };
        self.processor = Some(processor)
    }
}
