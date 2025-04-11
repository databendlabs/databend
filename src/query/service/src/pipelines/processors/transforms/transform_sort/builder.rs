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

use databend_common_column::binary::BinaryColumn;
use databend_common_exception::Result;
use databend_common_expression::row::RowConverter as CommonConverter;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::sort::algorithm::SortAlgorithm;
use databend_common_pipeline_transforms::sort::algorithm::HeapSort;
use databend_common_pipeline_transforms::sort::algorithm::LoserTreeSort;
use databend_common_pipeline_transforms::sort::utils::ORDER_COL_NAME;
use databend_common_pipeline_transforms::sort::RowConverter;
use databend_common_pipeline_transforms::sort::SimpleRowConverter;
use databend_common_pipeline_transforms::sort::SimpleRowsAsc;
use databend_common_pipeline_transforms::sort::SimpleRowsDesc;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::TransformSortMergeLimit;
use match_template::match_template;

use super::TransformSort;
use crate::spillers::Spiller;

type CommonRows = BinaryColumn;

pub struct TransformSortBuilder {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    schema: DataSchemaRef,
    block_size: usize,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    order_col_generated: bool,
    output_order_col: bool,
    memory_settings: MemorySettings,
    spiller: Arc<Spiller>,
    enable_loser_tree: bool,
    limit: Option<usize>,
}

impl TransformSortBuilder {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        sort_desc: Arc<Vec<SortColumnDescription>>,
        block_size: usize,
        spiller: Arc<Spiller>,
    ) -> Self {
        Self {
            input,
            output,
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

    pub fn build(self) -> Result<Box<dyn Processor>> {
        debug_assert!(if self.output_order_col {
            self.schema.has_field(ORDER_COL_NAME)
        } else {
            !self.schema.has_field(ORDER_COL_NAME)
        });

        if self.limit.map(|limit| limit < 10000).unwrap_or_default() {
            self.build_sort_limit()
        } else {
            self.build_sort()
        }
    }

    fn build_sort(self) -> Result<Box<dyn Processor>> {
        if self.sort_desc.len() != 1 {
            if self.enable_loser_tree {
                return self.build_sort_rows::<LoserTreeSort<CommonRows>, CommonConverter>();
            } else {
                return self.build_sort_rows::<HeapSort<CommonRows>, CommonConverter>();
            }
        }
        let sort_type = self.schema.field(self.sort_desc[0].offset).data_type();
        let asc = self.sort_desc[0].asc;

        match_template! {
            T = [ Date => DateType, Timestamp => TimestampType, String => StringType ],
            match sort_type {
                DataType::T => self.build_sort_rows_simple::<T>(asc),
                DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                    NumberDataType::NUM_TYPE => {
                        self.build_sort_rows_simple::<NumberType<NUM_TYPE>>(asc)
                    }
                }),
                _ => {
                    if self.enable_loser_tree {
                         self.build_sort_rows::<LoserTreeSort<CommonRows>, CommonConverter>()
                    } else {
                         self.build_sort_rows::<HeapSort<CommonRows>, CommonConverter>()
                    }
                }
            }
        }
    }

    fn build_sort_rows_simple<T>(self, asc: bool) -> Result<Box<dyn Processor>>
    where
        T: ArgType + Send,
        T::Column: Send,
        for<'a> T::ScalarRef<'a>: Ord + Send,
    {
        match (asc, self.enable_loser_tree) {
            (true, true) => {
                self.build_sort_rows::<LoserTreeSort<SimpleRowsAsc<T>>, SimpleRowConverter<T>>()
            }
            (true, false) => {
                self.build_sort_rows::<HeapSort<SimpleRowsAsc<T>>, SimpleRowConverter<T>>()
            }
            (false, true) => {
                self.build_sort_rows::<LoserTreeSort<SimpleRowsDesc<T>>, SimpleRowConverter<T>>()
            }
            (false, false) => {
                self.build_sort_rows::<HeapSort<SimpleRowsDesc<T>>, SimpleRowConverter<T>>()
            }
        }
    }

    fn build_sort_rows<A, C>(self) -> Result<Box<dyn Processor>>
    where
        A: SortAlgorithm + 'static,
        C: RowConverter<A::Rows> + Send + 'static,
    {
        Ok(Box::new(TransformSort::<A, C>::new(
            self.input,
            self.output,
            self.schema,
            self.sort_desc,
            self.limit,
            self.spiller,
            self.output_order_col,
            None,
            self.order_col_generated,
            self.memory_settings,
        )?))
    }

    fn build_sort_limit(self) -> Result<Box<dyn Processor>> {
        if self.sort_desc.len() != 1 {
            if self.enable_loser_tree {
                return self.build_sort_limit_rows::<LoserTreeSort<CommonRows>, CommonConverter>();
            } else {
                return self.build_sort_limit_rows::<HeapSort<CommonRows>, CommonConverter>();
            }
        }

        let sort_type = self.schema.field(self.sort_desc[0].offset).data_type();
        let asc = self.sort_desc[0].asc;

        match_template! {
            T = [ Date => DateType, Timestamp => TimestampType, String => StringType ],
            match sort_type {
                DataType::T => self.build_sort_limit_simple::<T>(asc),
                DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                    NumberDataType::NUM_TYPE => {
                        self.build_sort_limit_simple::<NumberType<NUM_TYPE>>(asc)
                    }
                }),
                _ => {
                    if self.enable_loser_tree {
                         self.build_sort_limit_rows::<LoserTreeSort<CommonRows>, CommonConverter>()
                    } else {
                         self.build_sort_limit_rows::<HeapSort<CommonRows>, CommonConverter>()
                    }
                }
            }
        }
    }

    fn build_sort_limit_simple<T>(self, asc: bool) -> Result<Box<dyn Processor>>
    where
        T: ArgType + Send,
        T::Column: Send,
        for<'a> T::ScalarRef<'a>: Ord + Send,
    {
        match (asc, self.enable_loser_tree) {
            (true, true) => self
                .build_sort_limit_rows::<LoserTreeSort<SimpleRowsAsc<T>>, SimpleRowConverter<T>>(),
            (true, false) => {
                self.build_sort_limit_rows::<HeapSort<SimpleRowsAsc<T>>, SimpleRowConverter<T>>()
            }
            (false, true) => self
                .build_sort_limit_rows::<LoserTreeSort<SimpleRowsDesc<T>>, SimpleRowConverter<T>>(),
            (false, false) => {
                self.build_sort_limit_rows::<HeapSort<SimpleRowsDesc<T>>, SimpleRowConverter<T>>()
            }
        }
    }

    fn build_sort_limit_rows<A, C>(self) -> Result<Box<dyn Processor>>
    where
        A: SortAlgorithm + 'static,
        C: RowConverter<A::Rows> + Send + 'static,
    {
        let limt_sort = Some(TransformSortMergeLimit::create(
            self.block_size,
            self.limit.unwrap(),
        ));
        Ok(Box::new(TransformSort::<A, C>::new(
            self.input,
            self.output,
            self.schema,
            self.sort_desc,
            self.limit,
            self.spiller,
            self.output_order_col,
            limt_sort,
            self.order_col_generated,
            self.memory_settings,
        )?))
    }
}
