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
use std::str;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::sync::LazyLock;

use arrow_array::RecordBatch;
use arrow_udf_runtime::javascript::FunctionOptions;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_cache::Cache;
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
#[cfg_attr(not(feature = "python-udf"), allow(unused_imports))]
use databend_common_meta_app::principal::StageInfo;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_sql::plans::UDFLanguage;
use databend_common_sql::plans::UDFScriptCode;
use databend_common_sql::plans::UDFType;
use databend_common_storage::init_stage_operator;
use self::venv::TempDir;

use super::runtime_pool::Pool;
use super::runtime_pool::RuntimeBuilder;
use crate::physical_plans::UdfFunctionDesc;

pub enum ScriptRuntime {
    JavaScript(JsRuntimePool),
    WebAssembly(arrow_udf_runtime::wasm::Runtime),
    #[cfg(feature = "python-udf")]
    Python(python_pool::PyRuntimePool),
}

static PY_VERSION: LazyLock<String> =
    LazyLock::new(|| venv::detect_python_version().unwrap_or("3.12".to_string()));

impl ScriptRuntime {
    pub fn try_create(func: &UdfFunctionDesc, _temp_dir: Option<TempDir>) -> Result<Self> {
        let UDFType::Script(box UDFScriptCode { language, code, .. }) = &func.udf_type else {
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
                let user_code = String::from_utf8(code.to_vec())?;
                let code = if let Some(temp_dir) = &_temp_dir {
                    let import_dir = temp_dir.path().display();
                    let mut script = String::from("import sys\n");
                    script.push_str(&format!(
                        r#"sys._xoptions['databend_import_directory'] = '{dir}'
if '{dir}' not in sys.path:
    sys.path.append('{dir}')
"#,
                        dir = import_dir,
                    ));

                    let stage_paths = Self::collect_stage_sys_paths(func, temp_dir);
                    if !stage_paths.is_empty() {
                        script.push_str("for _databend_zip in (");
                        for (idx, path) in stage_paths.iter().enumerate() {
                            if idx > 0 {
                                script.push_str(", ");
                            }
                            script.push_str(&format!("{path:?}"));
                        }
                        script.push_str("):\n    if _databend_zip not in sys.path:\n        sys.path.append(_databend_zip)\ndel _databend_zip\n");
                    }

                    script.push_str(&user_code);
                    script
                } else {
                    user_code
                };

                if let Some(temp_dir) = &_temp_dir {
                    let stage_paths = Self::collect_stage_sys_paths(func, temp_dir);
                    if !stage_paths.is_empty() {
                        log::info!(
                            "Python UDF {:?} added stage artifacts to sys.path: {:?}",
                            func.name,
                            stage_paths
                        );
                    }
                }

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
            ScriptRuntime::Python(pool) => {
                let batch = pool
                    .call(|runtime| runtime.call(&func.name, input_batch))
                    .map_err(|err| {
                        ErrorCode::UDFRuntimeError(format!(
                            "Python UDF {:?} execution failed: {err}",
                            func.name
                        ))
                    })?;
                if let Some(message) = Self::python_error_from_batch(func, &batch)? {
                    return Err(ErrorCode::UDFRuntimeError(format!(
                        "Python UDF {:?} execution failed: {}",
                        func.name, message
                    )));
                }
                batch
            }
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

    #[cfg(feature = "python-udf")]
    fn python_error_from_batch(
        func: &UdfFunctionDesc,
        batch: &RecordBatch,
    ) -> Result<Option<String>> {
        let schema = DataSchema::try_from(&(*batch.schema())).map_err(|err| {
            ErrorCode::UDFDataError(format!(
                "Failed to create schema from record batch for function '{}': {}",
                func.name, err
            ))
        })?;

        if schema.fields().len() > 1 {
            let error_field = &schema.fields()[1];
            if error_field.name().eq_ignore_ascii_case("error") {
                let result_block = DataBlock::from_record_batch(&schema, batch).map_err(|err| {
                    ErrorCode::UDFDataError(format!(
                        "Failed to create data block from record batch for function '{}': {}",
                        func.name, err
                    ))
                })?;
                let error_entry = result_block.get_by_offset(1);
                if let Some(message) = Self::first_non_null_error(error_entry) {
                    return Ok(Some(message));
                }
            }
        }
        Ok(None)
    }

    #[cfg(feature = "python-udf")]
    fn first_non_null_error(entry: &BlockEntry) -> Option<String> {
        use databend_common_expression::ScalarRef;

        for row in 0..entry.len() {
            if let Some(value) = entry.index(row) {
                match value {
                    ScalarRef::Null => continue,
                    ScalarRef::String(message) => {
                        if !message.is_empty() {
                            return Some(message.to_string());
                        }
                    }
                    ScalarRef::Binary(bytes) => {
                        if let Ok(text) = str::from_utf8(bytes) {
                            if !text.is_empty() {
                                return Some(text.to_string());
                            }
                        } else {
                            return Some("Python UDF returned non UTF-8 error payload".to_string());
                        }
                    }
                    other => {
                        return Some(format!("{other:?}"));
                    }
                }
            }
        }
        None
    }

    #[cfg(feature = "python-udf")]
    fn collect_stage_sys_paths(func: &UdfFunctionDesc, temp_dir: &TempDir) -> Vec<String> {
        match &func.udf_type {
            UDFType::Script(box UDFScriptCode {
                imports_stage_info, ..
            }) => imports_stage_info
                .iter()
                .filter_map(|(_, stage_path)| {
                    let name = stage_path
                        .trim_end_matches('/')
                        .rsplit('/')
                        .next()
                        .unwrap_or(stage_path);
                    let local_path = temp_dir.path().join(name);
                    let ext = local_path
                        .extension()
                        .and_then(|s| s.to_str())
                        .map(|s| s.to_ascii_lowercase());
                    match ext.as_deref() {
                        Some("zip") | Some("whl") | Some("egg") => {
                            Some(local_path.display().to_string())
                        }
                        _ => None,
                    }
                })
                .collect(),
            _ => Vec::new(),
        }
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

    const RESTRICTED_PYTHON_CODE: &str = r#"
import os
import sys
import sysconfig
from pathlib import Path

import builtins, sys
if "DATABEND_RESTRICTED_PYTHON" not in sys._xoptions:
    sys._xoptions['DATABEND_RESTRICTED_PYTHON'] = '1'

    ALLOWED_BASES = {Path("/tmp")}
    for key in ("stdlib", "platstdlib", "purelib", "platlib"):
        path = sysconfig.get_path(key)
        if path:
            ALLOWED_BASES.add(Path(path))
    for prefix in (sys.prefix, sys.exec_prefix, sys.base_prefix, sys.base_exec_prefix):
        if prefix:
            base_path = Path(prefix)
            ALLOWED_BASES.add(base_path)
            ALLOWED_BASES.add(base_path / f"lib/python{sys.version_info.major}.{sys.version_info.minor}")

    _original_open = open
    _original_os_open = os.open if hasattr(os, 'open') else None

    def _ensure_allowed(file_path: Path, target: str):
        for base in ALLOWED_BASES:
            try:
                file_path.relative_to(base)
                return
            except ValueError:
                continue
        raise PermissionError(f"Access denied: {target} is outside allowed directories")

    def safe_open(file, mode='r', **kwargs):
        file_path = Path(file).resolve()
        _ensure_allowed(file_path, file)
        return _original_open(file, mode, **kwargs)

    def safe_os_open(path, flags, mode=0o777):
        file_path = Path(path).resolve()
        _ensure_allowed(file_path, path)
        return _original_os_open(path, flags, mode)

    builtins.open = safe_open
    if _original_os_open:
        os.open = safe_os_open

    dangerous_modules = ['subprocess', 'os.system', 'eval', 'exec', 'compile']
    for module in dangerous_modules:
        if module in sys.modules:
            del sys.modules[module]

    try:
        import _contextvars  # noqa: F401
    except ModuleNotFoundError:
        import threading
        import types
        import weakref

        _MISSING = object()
        _REGISTERED_VARS = weakref.WeakSet()

        class Token:
            __slots__ = ("var", "old")

            def __init__(self, var, old):
                self.var = var
                self.old = old

        class ContextVar:
            __slots__ = ("name", "default", "_local")

            def __init__(self, name, *, default=_MISSING):
                self.name = name
                self.default = default
                self._local = threading.local()
                _REGISTERED_VARS.add(self)

            def get(self, default=_MISSING):
                value = getattr(self._local, "value", _MISSING)
                if value is _MISSING:
                    if default is not _MISSING:
                        return default
                    if self.default is _MISSING:
                        raise LookupError(f"ContextVar {self.name} has no value")
                    return self.default
                return value

            def set(self, value):
                old = getattr(self._local, "value", _MISSING)
                self._local.value = value
                return Token(self, old)

            def reset(self, token):
                if token.var is not self:
                    raise ValueError("Token does not belong to this ContextVar")
                if token.old is _MISSING:
                    if hasattr(self._local, "value"):
                        del self._local.value
                else:
                    self._local.value = token.old

        class Context:
            def __init__(self, values=None):
                self._values = values or {}

            def __setitem__(self, key, value):
                self._values[key] = value

            def items(self):
                return self._values.items()

            def run(self, callable, *args, **kwargs):
                tokens = []
                try:
                    for var, value in self._values.items():
                        tokens.append(var.set(value))
                    return callable(*args, **kwargs)
                finally:
                    for token in reversed(tokens):
                        token.var.reset(token)

        def copy_context():
            ctx = Context()
            for var in list(_REGISTERED_VARS):
                try:
                    ctx[var] = var.get()
                except LookupError:
                    continue
            return ctx

        module = types.ModuleType("_contextvars")
        module.ContextVar = ContextVar
        module.Context = Context
        module.Token = Token
        module.copy_context = copy_context
        sys.modules["_contextvars"] = module
"#;

    impl RuntimeBuilder<arrow_udf_runtime::python::Runtime> for PyRuntimeBuilder {
        type Error = ErrorCode;

        fn build(&self) -> Result<arrow_udf_runtime::python::Runtime> {
            let start = std::time::Instant::now();
            let mut runtime = arrow_udf_runtime::python::Builder::default()
                .safe_codes(RESTRICTED_PYTHON_CODE.to_string())
                .build()?;
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
    script_runtimes: RuntimeTimeRes,
}

impl TransformUdfScript {
    pub fn new(
        _func_ctx: FunctionContext,
        funcs: Vec<UdfFunctionDesc>,
        script_runtimes: RuntimeTimeRes,
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
            let (runtime, _) = self.script_runtimes.get(&func.name).unwrap();
            let result_batch = runtime.handle_execution(func, &input_batch)?;
            self.update_datablock(func, result_batch, &mut data_block)?;
        }
        Ok(data_block)
    }
}

type RuntimeTimeRes = BTreeMap<String, (Arc<ScriptRuntime>, Option<TempDir>)>;

impl TransformUdfScript {
    pub fn init_runtime(funcs: &[UdfFunctionDesc]) -> Result<RuntimeTimeRes> {
        let mut script_runtimes = BTreeMap::new();
        for func in funcs {
            let (code, code_str) = match &func.udf_type {
                UDFType::Script(box script_code) => {
                    (script_code, String::from_utf8(script_code.code.to_vec())?)
                }
                _ => continue,
            };

            let temp_dir = match &func.udf_type {
                UDFType::Script(box UDFScriptCode {
                    language: UDFLanguage::Python,
                    packages,
                    imports_stage_info,
                    ..
                }) => {
                    let mut dependencies = Self::extract_deps(&code_str);
                    dependencies.extend_from_slice(packages.as_slice());

                    let stage_fingerprints = Self::collect_stage_fingerprints(imports_stage_info)?;

                    let temp_dir = if !dependencies.is_empty() || !stage_fingerprints.is_empty() {
                        let key =
                            venv::PyVenvKeyEntry::new(&dependencies, stage_fingerprints.clone());
                        let mut w = venv::PY_VENV_CACHE.write();
                        if let Some(entry) = w.get(&key) {
                            Some(entry.materialize().map_err(ErrorCode::from_string)?)
                        } else {
                            let temp_dir = venv::create_venv(PY_VERSION.as_str())?;
                            venv::install_deps(temp_dir.path(), &dependencies)?;

                            if !imports_stage_info.is_empty() {
                                let imports_stage_info = imports_stage_info.clone();
                                let temp_dir_path = temp_dir.path();
                                databend_common_base::runtime::block_on(async move {
                                    let mut fts = Vec::with_capacity(imports_stage_info.len());
                                    for (stage, path) in imports_stage_info.iter() {
                                        let op = init_stage_operator(stage)?;
                                        let name = path
                                            .trim_end_matches('/')
                                            .split('/')
                                            .next_back()
                                            .unwrap();
                                        let temp_file = temp_dir_path.join(name);
                                        fts.push(async move {
                                            let buffer = op.read(path).await?;
                                            databend_common_base::base::tokio::fs::write(
                                                &temp_file,
                                                buffer.to_bytes().as_ref(),
                                            )
                                            .await
                                        });
                                    }
                                    let _ = futures::future::join_all(fts).await;
                                    Ok::<(), ErrorCode>(())
                                })?;
                            }

                            let archive_path = venv::archive_env(temp_dir.path())
                                .map_err(ErrorCode::from_string)?;
                            let cache_entry =
                                venv::PyVenvCacheEntry::new(temp_dir.clone(), archive_path);
                            w.insert(key, cache_entry);

                            Some(temp_dir)
                        }
                    } else {
                        None
                    };

                    temp_dir
                }
                _ => None,
            };

            if let Entry::Vacant(entry) = script_runtimes.entry(func.name.clone()) {
                let runtime = ScriptRuntime::try_create(func, temp_dir.clone()).map_err(|err| {
                    ErrorCode::UDFDataError(format!(
                        "Failed to create UDF runtime for language {:?} with error: {err}",
                        code.language
                    ))
                })?;
                entry.insert((Arc::new(runtime), temp_dir));
            };
        }

        Ok(script_runtimes)
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

        if parsed.get("dependencies").is_none() {
            return Vec::new();
        }

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

    fn collect_stage_fingerprints(
        imports_stage_info: &[(StageInfo, String)],
    ) -> Result<Vec<venv::StageImportFingerprint>> {
        #[cfg(feature = "python-udf")]
        {
            if imports_stage_info.is_empty() {
                return Ok(Vec::new());
            }

            let mut tasks = Vec::with_capacity(imports_stage_info.len());
            for (stage, path) in imports_stage_info.iter() {
                let op = init_stage_operator(stage)?;
                let stage_name = stage.stage_name.clone();
                let path = path.clone();
                tasks.push(async move {
                    let meta = op.stat(&path).await.map_err(|e| {
                        ErrorCode::StorageUnavailable(format!(
                            "Failed to stat stage file '{path}': {e}"
                        ))
                    })?;
                    Ok(venv::StageImportFingerprint {
                        stage_name,
                        path,
                        content_length: meta.content_length(),
                        etag: meta.etag().map(str::to_string),
                    })
                });
            }

            databend_common_base::runtime::block_on(async {
                futures::future::try_join_all(tasks).await
            })
        }
        #[cfg(not(feature = "python-udf"))]
        {
            let _ = imports_stage_info;
            Ok(Vec::new())
        }
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

        let result_block = DataBlock::from_record_batch(&schema, &result_batch).map_err(|err| {
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

mod venv {
    use std::fs;
    use std::fs::File;
    use std::io;
    use std::path::Path;
    use std::path::PathBuf;
    use std::process::Command;
    use std::sync::Arc;
    use std::sync::LazyLock;
    use std::sync::Weak;

    use databend_common_cache::LruCache;
    use databend_common_cache::MemSized;
    use parking_lot::Mutex;
    use parking_lot::RwLock;
    use uuid::Uuid;
    use walkdir::WalkDir;
    use zip::write::FileOptions;
    use zip::ZipArchive;
    use zip::ZipWriter;

    static PY_VENV_ARCHIVE_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
        let base = std::env::var("DATABEND_PY_UDF_CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                std::env::temp_dir()
                    .join("databend")
                    .join("python_udf_cache")
            });
        if let Err(e) = fs::create_dir_all(&base) {
            panic!("Failed to create python udf cache dir {:?}: {}", base, e);
        }
        base
    });

    static PY_VENV_WORK_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
        let base = std::env::temp_dir()
            .join("databend")
            .join("python_udf_env");
        if let Err(e) = fs::create_dir_all(&base) {
            panic!("Failed to create python udf work dir {:?}: {}", base, e);
        }
        base
    });

    #[derive(Clone)]
    pub struct TempDir {
        inner: Arc<TempDirInner>,
    }

    struct TempDirInner {
        path: PathBuf,
    }

    #[derive(Default)]
    struct WeakTempDir {
        inner: Weak<TempDirInner>,
    }

    impl TempDir {
        fn new_impl(path: PathBuf) -> Result<Self, String> {
            if path.exists() {
                fs::remove_dir_all(&path).map_err(|e| {
                    format!("Failed to clean python udf temp dir {:?}: {}", path, e)
                })?;
            }
            fs::create_dir_all(&path).map_err(|e| {
                format!("Failed to create python udf temp dir {:?}: {}", path, e)
            })?;
            Ok(Self {
                inner: Arc::new(TempDirInner { path }),
            })
        }

        pub fn new() -> Result<Self, String> {
            let path = PY_VENV_WORK_DIR.join(Uuid::now_v7().to_string());
            Self::new_impl(path)
        }

        pub fn new_with_path(path: PathBuf) -> Result<Self, String> {
            Self::new_impl(path)
        }

        pub fn path(&self) -> &Path {
            &self.inner.path
        }

        pub fn downgrade(&self) -> WeakTempDir {
            WeakTempDir {
                inner: Arc::downgrade(&self.inner),
            }
        }
    }

    impl WeakTempDir {
        pub fn upgrade(&self) -> Option<TempDir> {
            self.inner
                .upgrade()
                .map(|inner| TempDir { inner })
        }

        pub fn replace(&mut self, temp_dir: &TempDir) {
            self.inner = Arc::downgrade(&temp_dir.inner);
        }
    }

    impl Drop for TempDirInner {
        fn drop(&mut self) {
            if let Err(e) = fs::remove_dir_all(&self.path) {
                if !matches!(e.kind(), io::ErrorKind::NotFound) {
                    log::warn!(
                        "Failed to remove python udf temp dir {:?}: {}",
                        self.path,
                        e
                    );
                }
            }
        }
    }

    pub fn install_deps(temp_dir_path: &Path, deps: &[String]) -> Result<(), String> {
        if deps.is_empty() {
            return Ok(());
        }

        let target_path = temp_dir_path.display().to_string();
        let status = Command::new("python")
            .args(["-m", "pip", "install"])
            .args(deps)
            .args(["--target", &target_path])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| format!("Failed to install dependencies: {}", e))?;

        if status.success() {
            log::info!("Dependency installation success {}", deps.join(", "));
            Ok(())
        } else {
            Err("Dependency installation failed".into())
        }
    }

    pub fn archive_env(temp_dir_path: &Path) -> Result<PathBuf, String> {
        let archive_path = PY_VENV_ARCHIVE_DIR.join(format!("{}.zip", Uuid::now_v7()));
        let writer = File::create(&archive_path)
            .map_err(|e| format!("Failed to create python deps archive: {}", e))?;
        let mut zip = ZipWriter::new(writer);
        let options = FileOptions::<'static, ()>::default()
            .compression_method(zip::CompressionMethod::Deflated)
            .unix_permissions(0o755);

        for entry in WalkDir::new(temp_dir_path).min_depth(1) {
            let entry = entry.map_err(|e| format!("Failed to scan python deps: {}", e))?;
            let path = entry.path();
            let rel_path = path
                .strip_prefix(temp_dir_path)
                .map_err(|e| format!("Failed to strip python deps prefix: {}", e))?;
            let rel_path = rel_path
                .to_str()
                .ok_or_else(|| "Python dependency path is not valid UTF-8".to_string())?;
            let rel_path = rel_path.replace('\\', "/");

            if entry.file_type().is_dir() {
                zip.add_directory(&rel_path, options)
                    .map_err(|e| format!("Failed to add directory to archive: {}", e))?;
                continue;
            }

            if entry.file_type().is_file() || entry.file_type().is_symlink() {
                zip.start_file(&rel_path, options)
                    .map_err(|e| format!("Failed to add file to archive: {}", e))?;
                let mut file = File::open(path)
                    .map_err(|e| format!("Failed to read file {:?}: {}", path, e))?;
                io::copy(&mut file, &mut zip)
                    .map_err(|e| format!("Failed to write archive entry {:?}: {}", path, e))?;
            }
        }

        zip.finish()
            .map_err(|e| format!("Failed to finalize python deps archive: {}", e))?;
        Ok(archive_path)
    }

    pub fn restore_env_into(archive_path: &Path, temp_dir_path: &Path) -> Result<(), String> {
        let reader = File::open(archive_path)
            .map_err(|e| format!("Failed to read python deps archive: {}", e))?;
        let mut archive =
            ZipArchive::new(reader).map_err(|e| format!("Failed to open archive: {}", e))?;
        archive
            .extract(temp_dir_path)
            .map_err(|e| format!("Failed to extract python deps: {}", e))?;
        Ok(())
    }

    pub fn create_venv(_python_version: &str) -> Result<TempDir, String> {
        // let env_path = temp_dir.path().join(".venv");
        // Command::new("python")
        //     .args(["-m", "venv", env_path.to_str().unwrap()])
        //     .stdout(std::process::Stdio::null())
        //     .stderr(std::process::Stdio::null())
        //     .status()
        //     .map_err(|e| format!("Failed to create venv: {}", e))?;

        TempDir::new()
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

    // cached temp dir for python udf
    // Add this after the PY_VERSION LazyLock declaration
    // A simple LRU cache for Python virtual environments
    pub(crate) struct PyVenvCacheEntry {
        temp_dir: Mutex<WeakTempDir>,
        temp_dir_path: PathBuf,
        archive_path: PathBuf,
    }

    #[derive(Clone, Eq, Hash, PartialEq)]
    pub(crate) struct StageImportFingerprint {
        pub(crate) stage_name: String,
        pub(crate) path: String,
        pub(crate) content_length: u64,
        pub(crate) etag: Option<String>,
    }

    #[derive(Eq, Hash, PartialEq)]
    pub(crate) struct PyVenvKeyEntry {
        pub(crate) dependencies: Vec<String>,
        pub(crate) stage_fingerprints: Vec<StageImportFingerprint>,
    }

    impl PyVenvKeyEntry {
        pub fn new(
            dependencies: &[String],
            stage_fingerprints: Vec<StageImportFingerprint>,
        ) -> Self {
            let mut deps = dependencies.to_vec();
            deps.sort();
            Self {
                dependencies: deps,
                stage_fingerprints,
            }
        }
    }

    impl MemSized for PyVenvKeyEntry {
        fn mem_bytes(&self) -> usize {
            self.dependencies
                .iter()
                .map(MemSized::mem_bytes)
                .sum::<usize>()
                + self
                    .stage_fingerprints
                    .iter()
                    .map(MemSized::mem_bytes)
                    .sum::<usize>()
        }
    }

    impl MemSized for StageImportFingerprint {
        fn mem_bytes(&self) -> usize {
            self.stage_name.mem_bytes()
                + self.path.mem_bytes()
                + std::mem::size_of::<u64>()
                + self.etag.mem_bytes()
        }
    }

    impl MemSized for PyVenvCacheEntry {
        fn mem_bytes(&self) -> usize {
            std::mem::size_of::<Mutex<WeakTempDir>>() + 2 * std::mem::size_of::<PathBuf>()
        }
    }

    impl PyVenvCacheEntry {
        pub fn new(temp_dir: TempDir, archive_path: PathBuf) -> Self {
            Self {
                temp_dir: Mutex::new(temp_dir.downgrade()),
                temp_dir_path: temp_dir.path().to_path_buf(),
                archive_path,
            }
        }

        pub fn materialize(&self) -> Result<TempDir, String> {
            if let Some(existing) = self.temp_dir.lock().upgrade() {
                return Ok(existing);
            }

            let temp_dir = TempDir::new_with_path(self.temp_dir_path.clone())?;
            restore_env_into(&self.archive_path, temp_dir.path())?;
            self.temp_dir.lock().replace(&temp_dir);
            Ok(temp_dir)
        }
    }

    impl Drop for PyVenvCacheEntry {
        fn drop(&mut self) {
            if let Err(e) = fs::remove_file(&self.archive_path) {
                if !matches!(e.kind(), io::ErrorKind::NotFound) {
                    log::warn!(
                        "Failed to remove python udf cache archive {:?}: {}",
                        self.archive_path,
                        e
                    );
                }
            }
        }
    }

    pub static PY_VENV_CACHE: LazyLock<RwLock<LruCache<PyVenvKeyEntry, PyVenvCacheEntry>>> =
        LazyLock::new(|| RwLock::new(LruCache::with_items_capacity(64)));
}
