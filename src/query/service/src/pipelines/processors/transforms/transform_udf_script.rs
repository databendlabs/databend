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
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::DataType as ArrowType;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::converts::arrow::ARROW_EXT_TYPE_VARIANT;
use databend_common_expression::converts::arrow::EXTENSION_KEY;
use databend_common_expression::types::DataType;
use databend_common_expression::variant_transform::contains_variant;
use databend_common_expression::variant_transform::transform_variant;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::FunctionContext;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_sql::executor::physical_plans::UdfFunctionDesc;
use databend_common_sql::plans::UDFLanguage;
use databend_common_sql::plans::UDFScriptCode;
use databend_common_sql::plans::UDFType;

use super::runtime_pool::Pool;
use super::runtime_pool::RuntimeBuilder;

pub enum ScriptRuntime {
    JavaScript(JsRuntimePool),
    WebAssembly(arrow_udf_wasm::Runtime),
    #[cfg(feature = "python-udf")]
    Python(python_pool::PyRuntimePool),
}

impl ScriptRuntime {
    pub fn try_create(func: &UdfFunctionDesc) -> Result<Self> {
        let UDFType::Script(UDFScriptCode { language, code, .. }) = &func.udf_type else {
            unreachable!()
        };
        match language {
            UDFLanguage::JavaScript => {
                let builder = JsRuntimeBuilder {
                    name: func.name.clone(),
                    handler: func.func_name.clone(),
                    code: String::from_utf8(code.to_vec())?,
                    output_type: func.data_type.as_ref().clone(),
                    counter: Default::default(),
                };
                Ok(Self::JavaScript(JsRuntimePool::new(builder)))
            }
            UDFLanguage::WebAssembly => {
                let start = std::time::Instant::now();
                let runtime = arrow_udf_wasm::Runtime::new(code).map_err(|err| {
                    ErrorCode::UDFRuntimeError(format!(
                        "Failed to create WASM runtime for module: {err}"
                    ))
                })?;
                log::info!(
                    "Init WebAssembly UDF runtime for {:?} took: {:?}",
                    func.name,
                    start.elapsed()
                );
                Ok(ScriptRuntime::WebAssembly(runtime))
            }
            #[cfg(feature = "python-udf")]
            UDFLanguage::Python => {
                let builder = PyRuntimeBuilder {
                    name: func.name.clone(),
                    handler: func.func_name.clone(),
                    code: String::from_utf8(code.to_vec())?,
                    output_type: func.data_type.as_ref().clone(),
                    counter: Default::default(),
                };
                Ok(Self::Python(python_pool::PyRuntimePool::new(builder)))
            }
            #[cfg(not(feature = "python-udf"))]
            UDFLanguage::Python => Err(ErrorCode::EnterpriseFeatureNotEnable(
                "Failed to create python script udf",
            )),
        }
    }

    pub fn handle_execution(
        &self,
        func: &UdfFunctionDesc,
        input_batch: &RecordBatch,
    ) -> Result<RecordBatch> {
        let result_batch = match self {
            ScriptRuntime::JavaScript(pool) => pool
                .call(|runtime| runtime.call(&func.name, input_batch))
                .map_err(|err| {
                    ErrorCode::UDFRuntimeError(format!(
                        "JavaScript UDF {:?} execution failed: {err}",
                        func.name
                    ))
                })?,
            #[cfg(feature = "python-udf")]
            ScriptRuntime::Python(pool) => pool
                .call(|runtime| runtime.call(&func.name, input_batch))
                .map_err(|err| {
                    ErrorCode::UDFRuntimeError(format!(
                        "Python UDF {:?} execution failed: {err}",
                        func.name
                    ))
                })?,
            ScriptRuntime::WebAssembly(runtime) => {
                runtime.call(&func.func_name, input_batch).map_err(|err| {
                    ErrorCode::UDFRuntimeError(format!(
                        "WASM UDF {:?} execution failed: {err}",
                        func.func_name
                    ))
                })?
            }
        };
        Ok(result_batch)
    }
}

pub struct JsRuntimeBuilder {
    name: String,
    handler: String,
    code: String,
    output_type: DataType,

    counter: AtomicUsize,
}

impl RuntimeBuilder<arrow_udf_js::Runtime> for JsRuntimeBuilder {
    type Error = ErrorCode;

    fn build(&self) -> Result<arrow_udf_js::Runtime> {
        let start = std::time::Instant::now();
        let mut runtime = arrow_udf_js::Runtime::new()
            .map_err(|e| ErrorCode::UDFDataError(format!("Cannot create js runtime: {e}")))?;

        let output_type: ArrowType = (&self.output_type).into();
        runtime.add_function_with_handler(
            &self.name,
            output_type,
            arrow_udf_js::CallMode::ReturnNullOnNullInput,
            &self.code,
            &self.handler,
        )?;

        let converter = runtime.converter_mut();
        converter.set_arrow_extension_key(EXTENSION_KEY);
        converter.set_json_extension_name(ARROW_EXT_TYPE_VARIANT);

        log::info!(
            "Init JavaScript UDF runtime for {:?} #{} took: {:?}",
            self.name,
            self.counter
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            start.elapsed()
        );

        Ok(runtime)
    }
}

type JsRuntimePool = Pool<arrow_udf_js::Runtime, JsRuntimeBuilder>;

#[cfg(feature = "python-udf")]
pub struct PyRuntimeBuilder {
    name: String,
    handler: String,
    code: String,
    output_type: DataType,

    counter: AtomicUsize,
}

#[cfg(feature = "python-udf")]
mod python_pool {
    use super::*;

    impl RuntimeBuilder<arrow_udf_python::Runtime> for PyRuntimeBuilder {
        type Error = ErrorCode;

        fn build(&self) -> Result<arrow_udf_python::Runtime> {
            let start = std::time::Instant::now();
            let mut runtime = arrow_udf_python::Builder::default()
                .sandboxed(true)
                .build()?;
            let output_type: ArrowType = (&self.output_type).into();
            runtime.add_function_with_handler(
                &self.name,
                output_type,
                arrow_udf_python::CallMode::CalledOnNullInput,
                &self.code,
                &self.handler,
            )?;

            log::info!(
                "Init Python UDF runtime for {:?} #{} took: {:?}",
                self.name,
                self.counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                start.elapsed()
            );

            Ok(runtime)
        }
    }

    pub type PyRuntimePool = Pool<arrow_udf_python::Runtime, PyRuntimeBuilder>;
}

pub struct TransformUdfScript {
    funcs: Vec<UdfFunctionDesc>,
    script_runtimes: BTreeMap<String, Arc<ScriptRuntime>>,
}

impl TransformUdfScript {
    pub fn new(
        _func_ctx: FunctionContext,
        funcs: Vec<UdfFunctionDesc>,
        script_runtimes: BTreeMap<String, Arc<ScriptRuntime>>,
    ) -> Self {
        Self {
            funcs,
            script_runtimes,
        }
    }
}

impl Transform for TransformUdfScript {
    const NAME: &'static str = "UDFScriptTransform";

    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, mut data_block: DataBlock) -> Result<DataBlock> {
        if data_block.is_empty() {
            return Ok(data_block);
        }

        for func in &self.funcs {
            let num_rows = data_block.num_rows();
            let block_entries = self.prepare_block_entries(func, &data_block)?;
            let input_batch = self.create_input_batch(block_entries, num_rows)?;
            let runtime = self.script_runtimes.get(&func.name).unwrap();
            let result_batch = runtime.handle_execution(func, &input_batch)?;
            self.update_datablock(func, result_batch, &mut data_block)?;
        }
        Ok(data_block)
    }
}

impl TransformUdfScript {
    pub fn init_runtime(funcs: &[UdfFunctionDesc]) -> Result<BTreeMap<String, Arc<ScriptRuntime>>> {
        let mut script_runtimes: BTreeMap<String, Arc<ScriptRuntime>> = BTreeMap::new();

        for func in funcs {
            let code = match &func.udf_type {
                UDFType::Script(code) => code,
                _ => continue,
            };

            if let Entry::Vacant(entry) = script_runtimes.entry(func.name.clone()) {
                let runtime = ScriptRuntime::try_create(func).map_err(|err| {
                    ErrorCode::UDFDataError(format!(
                        "Failed to create UDF runtime for language {:?} with error: {err}",
                        code.language
                    ))
                })?;
                entry.insert(Arc::new(runtime));
            };
        }

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
                "Function {:?} returned column with data type {:?} but expected {:?}",
                func.name, col.data_type, func.data_type
            )));
        }
        data_block.add_column(col);
        Ok(())
    }
}
