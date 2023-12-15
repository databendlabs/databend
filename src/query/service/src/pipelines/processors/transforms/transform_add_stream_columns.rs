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

use databend_common_catalog::plan::StreamColumn;
use databend_common_catalog::plan::StreamColumnMeta;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;
use databend_common_sql::evaluator::CompoundBlockOperator;

pub struct TransformAddStreamColumns {
    expression_transform: CompoundBlockOperator,
    stream_columns: Vec<StreamColumn>,
}

impl TransformAddStreamColumns
where Self: Transform
{
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        expression_transform: CompoundBlockOperator,
        stream_columns: Vec<StreamColumn>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Transformer::create(
            input,
            output,
            Self {
                expression_transform,
                stream_columns,
            },
        )))
    }
}

impl Transform for TransformAddStreamColumns {
    const NAME: &'static str = "AddStreamColumnsTransform";

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        let num_rows = block.num_rows();
        if num_rows != 0 {
            if let Some(meta) = block.take_meta() {
                let meta = StreamColumnMeta::downcast_from(meta)
                    .ok_or_else(|| ErrorCode::Internal("It's a bug"))?;

                for stream_column in self.stream_columns.iter() {
                    let entry = stream_column.generate_column_values(&meta, num_rows);
                    block.add_column(entry);
                }

                block = self
                    .expression_transform
                    .transform(block)?
                    .add_meta(meta.inner)?;
            }
        }

        Ok(block)
    }
}
