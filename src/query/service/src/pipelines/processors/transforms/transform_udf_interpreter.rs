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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::variant_transform::contains_variant;
use databend_common_expression::variant_transform::transform_variant;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::FunctionContext;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::AsyncTransformer;
use databend_common_sql::executor::physical_plans::UdfFunctionDesc;

use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::Processor;

pub struct TransformUdfInterpreter {
    func_ctx: FunctionContext,
    funcs: Vec<UdfFunctionDesc>,
}

impl TransformUdfInterpreter {
    pub fn try_create(
        func_ctx: FunctionContext,
        funcs: Vec<UdfFunctionDesc>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<Box<dyn Processor>> {
        Ok(AsyncTransformer::create(input, output, Self {
            func_ctx,
            funcs,
        }))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for TransformUdfInterpreter {
    const NAME: &'static str = "UdfInterpreterTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, mut data_block: DataBlock) -> Result<DataBlock> {
        let num_rows = data_block.num_rows();
        for func in &self.funcs {
            let (lang, version, code) = func.udf_type.as_interepter().unwrap();

            // construct input record_batch
            let block_entries = func
                .arg_indices
                .iter()
                .map(|i| {
                    let arg = data_block.get_by_offset(*i).clone();
                    if contains_variant(&arg.data_type) {
                        let new_arg = BlockEntry::new(
                            arg.data_type.clone(),
                            transform_variant(&arg.value, true)?,
                        );
                        Ok(new_arg)
                    } else {
                        Ok(arg)
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            let fields = block_entries
                .iter()
                .enumerate()
                .map(|(idx, arg)| DataField::new(&format!("arg{}", idx + 1), arg.data_type.clone()))
                .collect::<Vec<_>>();
            let data_schema = DataSchema::new(fields);

            let input_batch = DataBlock::new(block_entries, num_rows)
                .to_record_batch(&data_schema)
                .map_err(|err| ErrorCode::from_string(format!("{err}")))?;

            todo!()
        }
        Ok(data_block)
    }
}
