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
use databend_common_expression::row::RowConverter as CommonConverter;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::sort::CommonRows;
use databend_common_pipeline_transforms::sort::RowConverter;
use databend_common_pipeline_transforms::sort::Rows;
use databend_common_pipeline_transforms::sort::SimpleRowConverter;
use databend_common_pipeline_transforms::sort::SimpleRowsAsc;
use databend_common_pipeline_transforms::AccumulatingTransformer;
use databend_common_pipeline_transforms::Transformer;
use match_template::match_template;

use crate::pipelines::processors::transforms::recluster::transform_add_order_column::TransformAddOrderColumn;
use crate::pipelines::processors::transforms::recluster::TransformRangePartitionIndexer;
use crate::pipelines::processors::transforms::SampleState;
use crate::pipelines::processors::transforms::TransformReclusterCollect;

pub struct TransformReclusterBuilder {
    schema: DataSchemaRef,
    sort_desc: Arc<[SortColumnDescription]>,
    sample_rate: f64,
    seed: u64,
}

impl TransformReclusterBuilder {
    pub fn build_recluster_sample(
        &self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<Box<dyn Processor>> {
        self.build_inner(BuilderType::ReclusterSample, input, output, None)
    }

    pub fn build_range_partition_indexer(
        &self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        state: Arc<SampleState>,
    ) -> Result<Box<dyn Processor>> {
        self.build_inner(
            BuilderType::RangePartitionIndexer,
            input,
            output,
            Some(state),
        )
    }

    pub fn build_add_order_column(
        &self,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<Box<dyn Processor>> {
        self.build_inner(BuilderType::AddOrderColumn, input, output, None)
    }

    fn build_inner(
        &self,
        typ: BuilderType,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        state: Option<Arc<SampleState>>,
    ) -> Result<Box<dyn Processor>> {
        let mut build = BuilderInner {
            input,
            output,
            typ,
            base: self,
            state,
        };
        build.select_row_type()
    }
}

enum BuilderType {
    AddOrderColumn,
    ReclusterSample,
    RangePartitionIndexer,
}

struct BuilderInner<'a> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    typ: BuilderType,
    base: &'a TransformReclusterBuilder,
    state: Option<Arc<SampleState>>,
}

impl BuilderInner<'_> {
    pub fn select_row_type(&mut self) -> Result<Box<dyn Processor>> {
        match self.base.sort_desc.as_ref() {
            [desc] => {
                let schema = self.base.schema.clone();
                let sort_type = schema.field(desc.offset).data_type();
                assert!(desc.asc);

                match_template! {
                    T = [ Date => DateType, Timestamp => TimestampType, String => StringType ],
                    match sort_type {
                        DataType::T => {
                            self.visit_type::<SimpleRowsAsc<T>, SimpleRowConverter<T>>()
                        },
                        DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                            NumberDataType::NUM_TYPE => {
                                self.visit_type::<SimpleRowsAsc<NumberType<NUM_TYPE>>, SimpleRowConverter<NumberType<NUM_TYPE>>>()
                            }
                        }),
                        _ => self.visit_type::<CommonRows, CommonConverter>()
                    }
                }
            }
            _ => self.visit_type::<CommonRows, CommonConverter>(),
        }
    }

    fn visit_type<R, C>(&mut self) -> Result<Box<dyn Processor>>
    where
        R: Rows + 'static,
        C: RowConverter<R> + Send + 'static,
        R::Type: ArgType + Send + Sync,
        <R::Type as AccessType>::Scalar: Ord + Send + Sync,
    {
        match self.typ {
            BuilderType::AddOrderColumn => self.build_add_order_column::<R, C>(),
            BuilderType::ReclusterSample => self.build_recluster_sample::<R::Type>(),
            BuilderType::RangePartitionIndexer => self.build_range_partition_indexer::<R::Type>(),
        }
    }

    fn build_add_order_column<R, C>(&mut self) -> Result<Box<dyn Processor>>
    where
        R: Rows + 'static,
        C: RowConverter<R> + Send + 'static,
    {
        let inner = TransformAddOrderColumn::<R, C>::try_new(
            self.base.sort_desc.clone(),
            self.base.schema.clone(),
        )?;
        Ok(Transformer::create(
            self.input.clone(),
            self.output.clone(),
            inner,
        ))
    }

    fn build_range_partition_indexer<T>(&mut self) -> Result<Box<dyn Processor>>
    where
        T: ArgType + Send + Sync,
        T::Scalar: Ord + Send + Sync,
    {
        Ok(TransformRangePartitionIndexer::<T>::create(
            self.input.clone(),
            self.output.clone(),
            self.state.clone().unwrap(),
        ))
    }

    fn build_recluster_sample<T>(&mut self) -> Result<Box<dyn Processor>>
    where
        T: ArgType + Send + Sync,
        T::Scalar: Ord + Send + Sync,
    {
        let offset = self.base.schema.fields().len();
        Ok(AccumulatingTransformer::create(
            self.input.clone(),
            self.output.clone(),
            TransformReclusterCollect::<T>::new(offset, self.base.sample_rate, self.base.seed),
        ))
    }
}
