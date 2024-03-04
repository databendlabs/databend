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

use arrow_schema::Schema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::variant_transform::contains_variant;
use databend_common_expression::variant_transform::transform_variant;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::FunctionContext;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;
use databend_common_sql::executor::physical_plans::UdfFunctionDesc;

use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::Processor;
pub struct TransformUdfScript {
    funcs: Vec<UdfFunctionDesc>,
    js_runtime: Arc<arrow_udf_js::Runtime>,
    // TODO:
    // py_runtime: Arc<arrow_udf_python::Runtime>,
}

unsafe impl Send for TransformUdfScript {}

impl TransformUdfScript {
    pub fn try_create(
        _func_ctx: FunctionContext,
        funcs: Vec<UdfFunctionDesc>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<Box<dyn Processor>> {
        let mut js_runtime = arrow_udf_js::Runtime::new()
            .map_err(|err| ErrorCode::UDFDataError(format!("Cannot create js runtime: {err}")))?;

        for func in funcs.iter() {
            let tmp_schema =
                DataSchema::new(vec![DataField::new("tmp", func.data_type.as_ref().clone())]);
            let arrow_schema = Schema::from(&tmp_schema);

            let (_, _, code) = func.udf_type.as_interepter().unwrap();
            js_runtime
                .add_function_with_handler(
                    &func.name,
                    arrow_schema.field(0).data_type().clone(),
                    arrow_udf_js::CallMode::ReturnNullOnNullInput,
                    code,
                    &func.func_name,
                )
                .map_err(|err| ErrorCode::UDFDataError(format!("Cannot add js function: {err}")))?;
        }

        Ok(Transformer::create(input, output, Self {
            funcs,
            js_runtime: Arc::new(js_runtime),
        }))
    }
}

impl Transform for TransformUdfScript {
    const NAME: &'static str = "UDFScriptTransform";

    fn transform(&mut self, mut data_block: DataBlock) -> Result<DataBlock> {
        let num_rows = data_block.num_rows();
        for func in &self.funcs {
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
                .to_record_batch_with_dataschema(&data_schema)
                .map_err(|err| ErrorCode::from_string(format!("{err}")))?;

            let result_batch = self
                .js_runtime
                .call(&func.name, &input_batch)
                .map_err(|err| ErrorCode::from_string(format!("{err}")))?;

            let schema = DataSchema::try_from(&(*result_batch.schema()))?;
            let (result_block, _result_schema) =
                DataBlock::from_record_batch(&schema, &result_batch).map_err(|err| {
                    ErrorCode::UDFDataError(format!(
                        "Cannot convert arrow record batch to data block: {err}"
                    ))
                })?;

            let col = if contains_variant(&func.data_type) {
                let value = transform_variant(&result_block.get_by_offset(0).value, false)?;
                BlockEntry {
                    data_type: func.data_type.as_ref().clone(),
                    value,
                }
            } else {
                result_block.get_by_offset(0).clone()
            };

            data_block.add_column(col);
        }
        Ok(data_block)
    }
}
