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

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
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
use databend_common_sql::plans::UDFType;
use parking_lot::RwLock;

use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::Processor;

pub enum ScriptRuntime {
    JavaScript(Arc<RwLock<arrow_udf_js::Runtime>>),
    WebAssembly(Arc<RwLock<arrow_udf_wasm::Runtime>>),
}

impl ScriptRuntime {
    pub fn try_create(lang: &str, code: Option<Vec<u8>>) -> Result<Self, ErrorCode> {
        match lang {
            "javascript" => arrow_udf_js::Runtime::new()
                .map(|runtime| ScriptRuntime::JavaScript(Arc::new(RwLock::new(runtime))))
                .map_err(|err| {
                    ErrorCode::UDFDataError(format!("Cannot create js runtime: {}", err))
                }),
            "wasm" => Self::create_wasm_runtime(code),
            _ => Err(ErrorCode::from_string(format!(
                "Invalid {} lang Runtime not supported",
                lang
            ))),
        }
    }

    fn create_wasm_runtime(code_blob: Option<Vec<u8>>) -> Result<Self, ErrorCode> {
        let decoded_code_blob = code_blob
            .ok_or_else(|| ErrorCode::UDFDataError("WASM module not provided".to_string()))?;

        let runtime = arrow_udf_wasm::Runtime::new(&decoded_code_blob).map_err(|err| {
            ErrorCode::UDFDataError(format!("Failed to create WASM runtime for module: {}", err))
        })?;

        Ok(ScriptRuntime::WebAssembly(Arc::new(RwLock::new(runtime))))
    }

    pub fn add_function_with_handler(
        &self,
        func: &UdfFunctionDesc,
        code: &str,
    ) -> Result<(), ErrorCode> {
        let tmp_schema =
            DataSchema::new(vec![DataField::new("tmp", func.data_type.as_ref().clone())]);
        let arrow_schema = Schema::from(&tmp_schema);

        match self {
            ScriptRuntime::JavaScript(runtime) => {
                let mut runtime = runtime.write();
                runtime.add_function_with_handler(
                    &func.name,
                    arrow_schema.field(0).data_type().clone(),
                    arrow_udf_js::CallMode::ReturnNullOnNullInput,
                    code,
                    &func.func_name,
                )
            }
            // Ignore the execution for WASM context
            ScriptRuntime::WebAssembly(_) => Ok(()),
        }?;

        Ok(())
    }

    pub fn handle_execution(
        &self,
        func: &UdfFunctionDesc,
        input_batch: &RecordBatch,
    ) -> Result<RecordBatch> {
        let result_batch = match self {
            ScriptRuntime::JavaScript(runtime) => {
                let runtime = runtime.read();
                runtime.call(&func.name, input_batch).map_err(|err| {
                    ErrorCode::UDFDataError(format!(
                        "JavaScript UDF '{}' execution failed: {}",
                        func.name, err
                    ))
                })?
            }
            ScriptRuntime::WebAssembly(runtime) => {
                let runtime = runtime.read();
                runtime.call(&func.func_name, input_batch).map_err(|err| {
                    ErrorCode::UDFDataError(format!(
                        "WASM UDF '{}' execution failed: {}",
                        func.func_name, err
                    ))
                })?
            }
        };
        Ok(result_batch)
    }
}

pub struct TransformUdfScript {
    funcs: Vec<UdfFunctionDesc>,
    script_runtimes: BTreeMap<String, Arc<ScriptRuntime>>,
}

unsafe impl Send for TransformUdfScript {}

impl TransformUdfScript {
    pub fn try_create(
        _func_ctx: FunctionContext,
        funcs: Vec<UdfFunctionDesc>,
        script_runtimes: BTreeMap<String, Arc<ScriptRuntime>>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<Box<dyn Processor>> {
        Ok(Transformer::create(input, output, Self {
            funcs,
            script_runtimes,
        }))
    }
}

impl Transform for TransformUdfScript {
    const NAME: &'static str = "UDFScriptTransform";

    fn transform(&mut self, mut data_block: DataBlock) -> Result<DataBlock> {
        for func in &self.funcs {
            let num_rows = data_block.num_rows();
            let block_entries = self.prepare_block_entries(func, &data_block)?;
            let input_batch = self.create_input_batch(block_entries, num_rows)?;
            let runtime_key = Self::get_runtime_key(func)?;

            if let Some(runtime) = self.script_runtimes.get(&runtime_key) {
                let result_batch = runtime.handle_execution(func, &input_batch)?;
                self.update_datablock(func, result_batch, &mut data_block)?;
            } else {
                return Err(ErrorCode::UDFDataError(format!(
                    "Failed to find runtime for function '{}' with key: {}",
                    func.name, runtime_key
                )));
            }
        }
        Ok(data_block)
    }
}

impl TransformUdfScript {
    fn get_runtime_key(func: &UdfFunctionDesc) -> Result<String, ErrorCode> {
        let (lang, func_name) = match &func.udf_type {
            UDFType::Script((lang, _, _)) | UDFType::WasmScript((lang, _, _)) => {
                (lang, &func.func_name)
            }
            _ => {
                return Err(ErrorCode::UDFDataError(format!(
                    "Unsupported UDFType variant for function '{}'",
                    func.name
                )));
            }
        };

        let runtime_key = format!("{}-{}", lang.trim(), func_name.trim());
        Ok(runtime_key)
    }

    pub fn init_runtime(
        funcs: &[UdfFunctionDesc],
    ) -> Result<BTreeMap<String, Arc<ScriptRuntime>>, ErrorCode> {
        let mut script_runtimes: BTreeMap<String, Arc<ScriptRuntime>> = BTreeMap::new();

        let start = std::time::Instant::now();
        for func in funcs {
            let (lang, code_opt) = match &func.udf_type {
                UDFType::Script((lang, _, _code)) => (lang, None),
                UDFType::WasmScript((lang, _, code)) => (lang, Some(code.clone())),
                _ => continue,
            };

            let runtime_key = Self::get_runtime_key(func)?;
            let runtime = match script_runtimes.entry(runtime_key.clone()) {
                Entry::Occupied(entry) => entry.into_mut().clone(),
                Entry::Vacant(entry) => {
                    let new_runtime = ScriptRuntime::try_create(lang.trim(), code_opt)
                        .map(Arc::new)
                        .map_err(|err| {
                            ErrorCode::UDFDataError(format!(
                                "Failed to create UDF runtime for language '{}' with error: {}",
                                lang, err
                            ))
                        })?;
                    entry.insert(new_runtime).clone()
                }
            };

            if let UDFType::Script((_, _, code)) = &func.udf_type {
                runtime.add_function_with_handler(func, code)?;
            }
        }

        log::info!("Init UDF runtimes took: {:?}", start.elapsed());
        Ok(script_runtimes)
    }

    fn prepare_block_entries(
        &self,
        func: &UdfFunctionDesc,
        data_block: &DataBlock,
    ) -> Result<Vec<BlockEntry>> {
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
        Ok(block_entries)
    }

    fn create_input_batch(
        &self,
        block_entries: Vec<BlockEntry>,
        num_rows: usize,
    ) -> Result<RecordBatch> {
        let fields = block_entries
            .iter()
            .enumerate()
            .map(|(idx, arg)| DataField::new(&format!("arg{}", idx + 1), arg.data_type.clone()))
            .collect::<Vec<_>>();
        let data_schema = DataSchema::new(fields);
        let num_columns = block_entries.len();

        let input_batch = DataBlock::new(block_entries, num_rows)
            .to_record_batch_with_dataschema(&data_schema)
            .map_err(|err| {
                ErrorCode::UDFDataError(format!(
                    "Failed to create input batch with {} rows and {} columns: {}",
                    num_rows, num_columns, err
                ))
            })?;

        Ok(input_batch)
    }

    fn update_datablock(
        &self,
        func: &UdfFunctionDesc,
        result_batch: RecordBatch,
        data_block: &mut DataBlock,
    ) -> Result<()> {
        // Convert to DataBlock and add column to data_block
        let schema = DataSchema::try_from(&(*result_batch.schema())).map_err(|err| {
            ErrorCode::UDFDataError(format!(
                "Failed to create schema from record batch for function '{}': {}",
                func.name, err
            ))
        })?;

        let (result_block, _) =
            DataBlock::from_record_batch(&schema, &result_batch).map_err(|err| {
                ErrorCode::UDFDataError(format!(
                    "Failed to create data block from record batch for function '{}': {}",
                    func.name, err
                ))
            })?;

        let col = if contains_variant(&func.data_type) {
            let value =
                transform_variant(&result_block.get_by_offset(0).value, false).map_err(|err| {
                    ErrorCode::UDFDataError(format!(
                        "Failed to transform variant for function '{}': {}",
                        func.name, err
                    ))
                })?;
            BlockEntry {
                data_type: func.data_type.as_ref().clone(),
                value,
            }
        } else {
            result_block.get_by_offset(0).clone()
        };

        if col.data_type != func.data_type.as_ref().clone() {
            return Err(ErrorCode::UDFDataError(format!(
                "Function '{}' returned column with data type {:?} but expected {:?}",
                func.name, col.data_type, func.data_type
            )));
        }
        data_block.add_column(col);
        Ok(())
    }
}
