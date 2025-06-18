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
use std::sync::LazyLock;

use arrow_array::RecordBatch;
use arrow_udf_runtime::javascript::FunctionOptions;
use databend_common_base::runtime::GlobalIORuntime;
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
use databend_common_expression::Value;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_sql::executor::physical_plans::UdfFunctionDesc;
use databend_common_sql::plans::UDFLanguage;
use databend_common_sql::plans::UDFScriptCode;
use databend_common_sql::plans::UDFType;
use tempfile::TempDir;

use super::runtime_pool::Pool;
use super::runtime_pool::RuntimeBuilder;

pub enum ScriptRuntime {
    JavaScript(JsRuntimePool),
    WebAssembly(arrow_udf_runtime::wasm::Runtime),
    #[cfg(feature = "python-udf")]
    Python(python_pool::PyRuntimePool),
}

static PY_VERSION: LazyLock<String> =
    LazyLock::new(|| uv::detect_python_version().unwrap_or("3.12".to_string()));

impl ScriptRuntime {
    pub fn try_create(func: &UdfFunctionDesc, _temp_dir: &Option<TempDir>) -> Result<Self> {
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
                let runtime = arrow_udf_runtime::wasm::Runtime::new(code).map_err(|err| {
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
                let code = String::from_utf8(code.to_vec())?;
                let code = if let Some(temp_dir) = _temp_dir {
                    format!(
                        "import sys\nsys.path.append('{}/.venv/lib/python{}/site-packages')\n{}",
                        temp_dir.path().display(),
                        PY_VERSION.as_str(),
                        code
                    )
                } else {
                    code
                };

                let builder = PyRuntimeBuilder {
                    name: func.name.clone(),
                    handler: func.func_name.clone(),
                    code,
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
            ScriptRuntime::JavaScript(pool) => pool.call(|runtime| {
                GlobalIORuntime::instance().block_on(async move {
                    runtime.call(&func.name, input_batch).await.map_err(|err| {
                        ErrorCode::UDFRuntimeError(format!(
                            "JavaScript UDF {:?} execution failed: {err}",
                            func.name
                        ))
                    })
                })
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
                let args_types: Vec<_> = input_batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.as_ref().clone())
                    .collect();
                let return_type = func.data_type.as_ref().clone();
                let f = DataField::new(&func.func_name, return_type);
                let return_f = arrow_schema::Field::from(&f);

                let handle = runtime
                    .find_function(&func.func_name, args_types, return_f)
                    .map_err(|err| {
                        ErrorCode::UDFRuntimeError(format!(
                            "WASM UDF {:?} execution failed: {err}",
                            func.func_name
                        ))
                    })?;

                runtime.call(&handle, input_batch).map_err(|err| {
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

impl RuntimeBuilder<arrow_udf_runtime::javascript::Runtime> for JsRuntimeBuilder {
    type Error = ErrorCode;

    fn build(&self) -> Result<arrow_udf_runtime::javascript::Runtime> {
        let start = std::time::Instant::now();
        let mut runtime = GlobalIORuntime::instance().block_on(async move {
            arrow_udf_runtime::javascript::Runtime::new()
                .await
                .map_err(|e| ErrorCode::UDFDataError(format!("Cannot create js runtime: {e}")))
        })?;

        let converter = runtime.converter_mut();
        converter.set_arrow_extension_key(EXTENSION_KEY);
        converter.set_json_extension_name(ARROW_EXT_TYPE_VARIANT);

        let rt = GlobalIORuntime::instance().block_on(async move {
            runtime
                .add_function(
                    &self.name,
                    // we pass the field instead of the data type because arrow-udf-js
                    // now takes the field as an argument here so that it can get any
                    // metadata associated with the field
                    arrow_field_from_data_type(&self.name, self.output_type.clone()),
                    &self.code,
                    FunctionOptions::default()
                        .return_null_on_null_input()
                        .handler(self.handler.clone()),
                )
                .await?;

            Ok(runtime)
        });

        log::info!(
            "Init JavaScript UDF runtime for {:?} #{} took: {:?}",
            self.name,
            self.counter
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            start.elapsed()
        );
        rt
    }
}

fn arrow_field_from_data_type(name: &str, dt: DataType) -> arrow_schema::Field {
    let field = DataField::new(name, dt);
    (&field).into()
}

type JsRuntimePool = Pool<arrow_udf_runtime::javascript::Runtime, JsRuntimeBuilder>;

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

    impl RuntimeBuilder<arrow_udf_runtime::python::Runtime> for PyRuntimeBuilder {
        type Error = ErrorCode;

        fn build(&self) -> Result<arrow_udf_runtime::python::Runtime> {
            let start = std::time::Instant::now();
            let mut runtime = arrow_udf_runtime::python::Builder::default().build()?;
            runtime.add_function_with_handler(
                &self.name,
                arrow_field_from_data_type(&self.name, self.output_type.clone()),
                arrow_udf_runtime::CallMode::CalledOnNullInput,
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

    pub type PyRuntimePool = Pool<arrow_udf_runtime::python::Runtime, PyRuntimeBuilder>;
}

pub struct TransformUdfScript {
    funcs: Vec<UdfFunctionDesc>,
    script_runtimes: BTreeMap<String, Arc<ScriptRuntime>>,
    py_temp_dir: Arc<Option<TempDir>>,
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
            py_temp_dir: Arc::new(None),
        }
    }

    pub fn with_py_temp_dir(mut self, py_temp_dir: Arc<Option<TempDir>>) -> Self {
        self.py_temp_dir = py_temp_dir;
        self
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

type RuntimeTimeRes = (BTreeMap<String, Arc<ScriptRuntime>>, Option<TempDir>);
impl TransformUdfScript {
    pub fn init_runtime(funcs: &[UdfFunctionDesc]) -> Result<RuntimeTimeRes> {
        let mut script_runtimes: BTreeMap<String, Arc<ScriptRuntime>> = BTreeMap::new();
        let temp_dir = Self::prepare_py_env(funcs)?;
        for func in funcs {
            let code = match &func.udf_type {
                UDFType::Script(code) => code,
                _ => continue,
            };

            if let Entry::Vacant(entry) = script_runtimes.entry(func.name.clone()) {
                let runtime = ScriptRuntime::try_create(func, &temp_dir).map_err(|err| {
                    ErrorCode::UDFDataError(format!(
                        "Failed to create UDF runtime for language {:?} with error: {err}",
                        code.language
                    ))
                })?;
                entry.insert(Arc::new(runtime));
            };
        }

        Ok((script_runtimes, temp_dir))
    }

    // returns the injection codes for python
    fn prepare_py_env(funcs: &[UdfFunctionDesc]) -> Result<Option<TempDir>> {
        let mut dependencies = Vec::new();
        for func in funcs {
            match &func.udf_type {
                UDFType::Script(UDFScriptCode {
                    language: UDFLanguage::Python,
                    code,
                    ..
                }) => {
                    let code = String::from_utf8(code.to_vec())?;
                    dependencies.extend_from_slice(&Self::extract_deps(&code));
                }
                _ => continue,
            };
        }

        // use uv to install dependencies
        if !dependencies.is_empty() {
            dependencies.dedup();

            let temp_dir = uv::create_uv_env(PY_VERSION.as_str())?;
            uv::install_deps(temp_dir.path(), &dependencies)?;
            return Ok(Some(temp_dir));
        }

        Ok(None)
    }

    fn extract_deps(script: &str) -> Vec<String> {
        let mut ss = String::new();
        let mut meta_start = false;
        for line in script.lines() {
            if meta_start {
                if line.starts_with("# ///") {
                    break;
                }
                ss.push_str(line.trim_start_matches('#').trim());
                ss.push('\n');
            }
            if !meta_start && line.starts_with("# /// script") {
                meta_start = true;
            }
        }

        let parsed = ss.parse::<toml::Value>().unwrap();
        if let Some(deps) = parsed["dependencies"].as_array() {
            deps.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        } else {
            Vec::new()
        }
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
                if contains_variant(&arg.data_type()) {
                    let new_arg = match arg {
                        BlockEntry::Const(scalar, data_type, n) => {
                            let scalar = transform_variant(&Value::Scalar(scalar), true)?
                                .into_scalar()
                                .unwrap();
                            BlockEntry::new_const_column(data_type, scalar, n)
                        }
                        BlockEntry::Column(column) => {
                            transform_variant(&Value::Column(column), true)?
                                .into_column()
                                .unwrap()
                                .into()
                        }
                    };
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
            .map(|(idx, arg)| DataField::new(&format!("arg{}", idx + 1), arg.data_type()))
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

        let entry = if contains_variant(&func.data_type) {
            let value = transform_variant(&result_block.get_by_offset(0).value(), false).map_err(
                |err| {
                    ErrorCode::UDFDataError(format!(
                        "Failed to transform variant for function '{}': {}",
                        func.name, err
                    ))
                },
            )?;
            BlockEntry::new(value, || {
                (*func.data_type.to_owned(), data_block.num_rows())
            })
        } else {
            result_block.get_by_offset(0).clone()
        };

        if entry.data_type() != func.data_type.as_ref().clone() {
            return Err(ErrorCode::UDFDataError(format!(
                "Function {:?} returned column with data type {:?} but expected {:?}",
                func.name,
                entry.data_type(),
                func.data_type
            )));
        }
        data_block.add_entry(entry);
        Ok(())
    }
}

mod uv {
    use std::path::Path;
    use std::process::Command;

    use tempfile::TempDir;

    pub fn install_deps(temp_dir_path: &Path, deps: &[String]) -> Result<(), String> {
        let status = Command::new("uv")
            .current_dir(temp_dir_path.join(".venv"))
            .args(["pip", "install"])
            .args(deps)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| format!("Failed to install dependencies: {}", e))?;

        log::info!("Dependency installation success {}", deps.join(", "));

        if status.success() {
            Ok(())
        } else {
            Err("Dependency installation failed".into())
        }
    }

    pub fn create_uv_env(python_version: &str) -> Result<TempDir, String> {
        let temp_dir =
            tempfile::tempdir().map_err(|e| format!("Failed to create temp dir: {}", e))?;
        let env_path = temp_dir.path().join(".venv");

        Command::new("uv")
            .args([
                "venv",
                "--python",
                python_version,
                env_path.to_str().unwrap(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| format!("Failed to create UV env: {}", e))?;

        Ok(temp_dir)
    }

    pub fn detect_python_version() -> Result<String, String> {
        let output = Command::new("python")
            .arg("--version")
            .output()
            .map_err(|e| format!("Failed to detect python version: {}", e))?;

        if output.status.success() {
            let version = String::from_utf8_lossy(&output.stdout);
            let version = version
                .trim()
                .to_string()
                .replace("Python ", "")
                .split('.')
                .take(2)
                .collect::<Vec<_>>()
                .join(".");
            Ok(version)
        } else {
            Err("Failed to detect python version".into())
        }
    }
}
