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
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockMetaInfoPtr;
use common_expression::DataBlock;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::TableField;
use common_expression::TableSchemaRef;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::AccumulatingTransform;
use common_pipeline_transforms::processors::transforms::AsyncTransform;
use common_pipeline_transforms::processors::transforms::AsyncTransformer;
use common_pipeline_transforms::processors::transforms::BlockMetaAccumulatingTransform;
use common_pipeline_transforms::processors::transforms::BlockMetaAccumulatingTransformer;
use common_pipeline_transforms::processors::transforms::Transform;
use opendal::Operator;
use serde::Deserializer;
use serde::Serializer;
use storages_common_index::filters::BlockFilter;
use storages_common_index::BloomIndex;
use storages_common_index::FilterEvalResult;

use crate::index_analyzer::block_filter_meta::BlockFilterMeta;
use crate::index_analyzer::bloom_filter_meta::BloomFilterMeta;
use crate::io::BloomBlockFilterReader;
use crate::pruning::SegmentLocation;

pub struct BloomFilterReadTransform {
    operator: Operator,
    index_fields: Vec<TableField>,
}

impl BloomFilterReadTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        operator: Operator,
        index_fields: Vec<TableField>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncTransformer::create(
            input,
            output,
            BloomFilterReadTransform {
                operator,
                index_fields,
            },
        )))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for BloomFilterReadTransform {
    const NAME: &'static str = "BlockFilterReadTransform";

    async fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        let meta = data.take_meta().unwrap();
        if let Some(meta) = BlockFilterMeta::downcast_from(meta) {
            let block_meta = &meta.block_meta;

            let (version, block_filter) = match &block_meta.bloom_filter_index_location {
                None => (0, None),
                Some(loc) => {
                    // filter out columns that no longer exist in the indexed block
                    let index_columns = self.index_fields.iter().try_fold(
                        Vec::with_capacity(self.index_fields.len()),
                        |mut acc, field| {
                            if block_meta.col_metas.contains_key(&field.column_id()) {
                                acc.push(BloomIndex::build_filter_column_name(loc.1, field)?);
                            }
                            Ok::<_, ErrorCode>(acc)
                        },
                    )?;

                    let operator = self.operator.clone();
                    let size = block_meta.bloom_filter_index_size;

                    (
                        loc.1,
                        match loc.read_block_filter(operator, &index_columns, size).await {
                            Ok(filter) => Some(filter),
                            Err(e) if e.code() == ErrorCode::DEPRECATED_INDEX_FORMAT => None,
                            Err(e) => {
                                return Err(e);
                            }
                        },
                    )
                }
            };

            return Ok(DataBlock::empty_with_meta(BloomFilterMeta::create(
                meta,
                version,
                block_filter,
            )));
        }

        Err(ErrorCode::Internal(
            "BlockFilterReadTransform only recv BlockFilterMeta",
        ))
    }
}

pub struct BloomFilterTransform {
    func_ctx: FunctionContext,
    data_schema: TableSchemaRef,
    filter_expression: Expr<String>,
    scalar_map: HashMap<Scalar, u64>,
}

impl BloomFilterTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        func_ctx: FunctionContext,
        data_schema: TableSchemaRef,
        filter_expression: Expr<String>,
    ) -> Result<ProcessorPtr> {
        let point_query_cols = BloomIndex::find_eq_columns(&filter_expression)?;
        let mut scalar_map = HashMap::<Scalar, u64>::new();
        for (_, scalar, ty) in point_query_cols.iter() {
            if !scalar_map.contains_key(scalar) {
                let digest = BloomIndex::calculate_scalar_digest(&func_ctx, scalar, ty)?;
                scalar_map.insert(scalar.clone(), digest);
            }
        }

        Ok(ProcessorPtr::create(
            BlockMetaAccumulatingTransformer::create(input, output, BloomFilterTransform {
                func_ctx,
                scalar_map,
                data_schema,
                filter_expression,
            }),
        ))
    }
}

impl BlockMetaAccumulatingTransform<BloomFilterMeta> for BloomFilterTransform {
    const NAME: &'static str = "BloomFilterTransform";

    fn transform(&mut self, data: BloomFilterMeta) -> Result<Option<DataBlock>> {
        match data.block_filter {
            None => Ok(Some(DataBlock::empty_with_meta(Box::new(data.inner)))),
            Some(filter) => {
                let bloom_index = BloomIndex::from_filter_block(
                    self.func_ctx.clone(),
                    self.data_schema.clone(),
                    filter.filter_schema,
                    filter.filters,
                    data.filter_version,
                )?;

                match bloom_index.apply(self.filter_expression.clone(), &self.scalar_map) {
                    Ok(FilterEvalResult::MustFalse) => Ok(None),
                    Ok(FilterEvalResult::Uncertain) | Err(_) => {
                        Ok(Some(DataBlock::empty_with_meta(Box::new(data.inner))))
                    }
                }
            }
        }
    }
}

pub fn bloom_fields(expr: &Expr<String>, schema: &TableSchemaRef) -> Result<Vec<TableField>> {
    let point_query_cols = BloomIndex::find_eq_columns(expr)?;
    let mut filter_fields = Vec::with_capacity(point_query_cols.len());
    for (col_name, _, _) in point_query_cols.iter() {
        if let Ok(field) = schema.field_with_name(col_name) {
            filter_fields.push(field.clone());
        }
    }

    Ok(filter_fields)
}
