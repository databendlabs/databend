//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::any::Any;
use std::sync::Arc;

use common_base::base::Progress;
use common_catalog::plan::VirtualColumnInfo;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::eval_function;
use common_expression::types::number::NumberScalar;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::Value;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use jsonb::JsonPath;

pub struct VirtualColumnTransform {
    func_ctx: FunctionContext,
    _scan_progress: Arc<Progress>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
    virtual_columns: Vec<VirtualColumnInfo>,
}

unsafe impl Send for VirtualColumnTransform {}

impl VirtualColumnTransform {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        virtual_columns: Vec<VirtualColumnInfo>,
    ) -> Result<ProcessorPtr> {
        let func_ctx = ctx.get_function_context()?;
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(VirtualColumnTransform {
            func_ctx,
            _scan_progress: scan_progress,
            input,
            output,
            input_data: None,
            output_data: None,
            virtual_columns,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for VirtualColumnTransform {
    fn name(&self) -> String {
        String::from("VirtualColumnTransform")
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

        if let Some(output_data) = self.output_data.take() {
            self.output.push_data(Ok(output_data));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data) = self.input_data.take() {
            let mut new_columns =
                Vec::with_capacity(data.num_columns() + self.virtual_columns.len());
            for column in data.columns() {
                new_columns.push(column.clone());
            }

            for virtual_column in &self.virtual_columns {
                let source = data.get_by_offset(0);

                let mut src_arg = (source.value.clone(), source.data_type.clone());
                for json_path in virtual_column.json_paths.iter() {
                    let path_arg = match json_path {
                        JsonPath::String(s) => (
                            Value::Scalar(Scalar::String(s.as_bytes().to_vec())),
                            DataType::String,
                        ),
                        JsonPath::UInt64(n) => (
                            Value::Scalar(Scalar::Number(NumberScalar::UInt64(*n))),
                            DataType::Number(NumberDataType::UInt64),
                        ),
                    };
                    let (value, data_type) = eval_function(
                        None,
                        "get",
                        [src_arg, path_arg],
                        self.func_ctx,
                        data.num_rows(),
                        &BUILTIN_FUNCTIONS,
                    )?;
                    src_arg = (value, data_type);
                }

                let new_column = BlockEntry {
                    data_type: DataType::Nullable(Box::new(DataType::Variant)),
                    value: src_arg.0,
                };
                new_columns.push(new_column)
            }

            let new_data = DataBlock::new(new_columns, data.num_rows());
            self.output_data = Some(new_data);
        }

        Ok(())
    }
}
