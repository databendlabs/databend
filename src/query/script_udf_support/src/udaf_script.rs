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

use std::alloc::Layout;
#[cfg(feature = "python-udf")]
use std::collections::HashMap;
use std::fmt;
use std::io::BufRead;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::OnceLock;

use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use arrow_schema::DataType as ArrowType;
use arrow_udf_runtime::javascript::AggregateOptions;
use databend_common_base::runtime::GlobalIORuntime;
#[cfg(feature = "python-udf")]
use databend_common_config::GlobalConfig;
#[cfg(feature = "python-udf")]
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::aggregate::aggregate_function::*;
use databend_common_expression::aggregate_function::AggregateCallInstance;
use databend_common_expression::aggregate_function::AggregateFeatures;
use databend_common_expression::converts::arrow::ARROW_EXT_TYPE_VARIANT;
use databend_common_expression::converts::arrow::EXTENSION_KEY;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UnaryType;
#[cfg(feature = "python-udf")]
use databend_common_sql::planner::UDFValidator;
#[cfg(feature = "python-udf")]
use databend_common_sql::planner::UdfValidationConfig;
use databend_common_sql::plans::UDFLanguage;
use databend_common_sql::plans::UDFScriptCode;

#[cfg(feature = "python-udf")]
use super::python_worker::AggregateOp;
#[cfg(feature = "python-udf")]
use super::python_worker::PythonFunction;
#[cfg(feature = "python-udf")]
use super::python_worker::PythonQueryWorkers;
#[cfg(feature = "python-udf")]
use super::python_worker::PythonRuntime;
#[cfg(feature = "python-udf")]
use super::python_worker::PythonWorkerManager;
#[cfg(feature = "python-udf")]
use super::python_worker::array_batch;
#[cfg(feature = "python-udf")]
use super::python_worker::decode_batch;
#[cfg(feature = "python-udf")]
use super::python_worker::encode_aggregate_request;
use super::runtime_pool::Pool;
use super::runtime_pool::RuntimeBuilder;
use super::script_udf_limits::ScriptUdfLimits;
use super::script_udf_limits::limit_js_runtime_calls;
use super::script_udf_limits::limit_js_runtime_init;

pub struct AggregateUdfScript {
    display_name: String,
    runtime: UDAFRuntime,
    argument_schema: DataSchema,
    /// Created on first use. Python creates it in a worker, which must not
    /// happen while the pipeline is still being built: the first worker call
    /// admits the query, after which no further Python runtime may register.
    init_state: OnceLock<Arc<dyn Array>>,
}

impl AggregateEval for AggregateUdfScript {
    fn init_state(&self, place: AggrState<'_>) {
        place.write(|| UdfAggState(self.init_state.get().cloned()));
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let input_batch = self.create_input_batch(input.columns, input.validity)?;
        let state = input.state.get::<UdfAggState>();
        let new_state = self
            .runtime
            .accumulate(&self.state_array(state)?, &input_batch)
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to accumulate: {e}")))?;
        *state = new_state;
        Ok(())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        #[cfg(feature = "python-udf")]
        if let UDAFRuntime::Python(python) = &self.runtime {
            return self.accumulate_keys_python(python, input);
        }
        for (row, state) in input.states.iter().enumerate() {
            if input
                .validity
                .is_some_and(|validity| !validity.get(row).unwrap())
            {
                continue;
            }
            self.accumulate_row(AccumulateRowInput {
                state,
                columns: input.columns,
                row,
            })?;
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let input_batch = self.create_input_batch_row(input.columns, input.row)?;
        let state = input.state.get::<UdfAggState>();
        let new_state = self
            .runtime
            .accumulate(&self.state_array(state)?, &input_batch)
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to accumulate_row: {e}")))?;
        *state = new_state;
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        let binary_builder = input.builders[0].as_binary_mut().unwrap();
        for state in input.states.iter() {
            let state = self.state_array(state.get::<UdfAggState>())?;
            serialize_state(&state, &mut binary_builder.data)
                .map_err(|e| ErrorCode::Internal(format!("state failed to serialize: {e}")))?;
            binary_builder.commit_row();
        }
        Ok(())
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        #[cfg(feature = "python-udf")]
        if let UDAFRuntime::Python(python) = &self.runtime {
            return self.merge_serialized_python(python, input);
        }
        let view = input.state.downcast::<UnaryType<BinaryType>>().unwrap();
        let iter = input.states.iter().zip(view.iter());

        if let Some(filter) = input.filter {
            for (state, mut data) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v)) {
                self.merge_one(state, &mut data)?;
            }
        } else {
            for (state, mut data) in iter {
                self.merge_one(state, &mut data)?;
            }
        }
        Ok(())
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        let state = input.state.get::<UdfAggState>();
        let other = self.state_array(input.rhs.get::<UdfAggState>())?;
        let states = arrow_select::concat::concat(&[&self.state_array(state)?, &other])
            .map_err(|e| ErrorCode::Internal(e.to_string()))?;
        let new_state = self
            .runtime
            .merge(&states)
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to merge_states: {e}")))?;
        *state = new_state;
        Ok(())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        let state = self.state_array(input.state.get::<UdfAggState>())?;
        let array = self
            .runtime
            .finish(&state)
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to merge_result: {e}")))?;
        let result = Column::from_arrow_rs(array, &self.runtime.return_type())?;
        input.builder.append_column(&result);
        Ok(())
    }

    unsafe fn drop_state(&self, place: AggrState<'_>) {
        unsafe {
            let state = place.get::<UdfAggState>();
            std::ptr::drop_in_place(state);
        }
    }
}

impl fmt::Display for AggregateUdfScript {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl AggregateUdfScript {
    fn initial_state(&self) -> Result<Arc<dyn Array>> {
        if let Some(state) = self.init_state.get() {
            return Ok(state.clone());
        }
        let state = self
            .runtime
            .create_state()
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to create state: {e}")))?;
        // Concurrent first calls may each create one; all are equivalent.
        Ok(self.init_state.get_or_init(|| state.0.unwrap()).clone())
    }

    fn state_array(&self, state: &UdfAggState) -> Result<Arc<dyn Array>> {
        match &state.0 {
            Some(state) => Ok(state.clone()),
            None => self.initial_state(),
        }
    }

    fn merge_one(&self, state: AggrState<'_>, data: &mut &[u8]) -> Result<()> {
        let current = state.get::<UdfAggState>();
        let rhs = deserialize_state(data).map_err(|e| ErrorCode::Internal(e.to_string()))?;
        let states = arrow_select::concat::concat(&[&self.state_array(current)?, &rhs])?;
        let merged_state = self
            .runtime
            .merge(&states)
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to merge: {e}")))?;
        *current = merged_state;
        Ok(())
    }

    /// One worker request per block instead of one per row: rows are grouped
    /// by state and applied in row order inside the worker.
    #[cfg(feature = "python-udf")]
    fn accumulate_keys_python(
        &self,
        python: &PythonUdaf,
        input: AccumulateKeysInput<'_>,
    ) -> Result<()> {
        let mut places = PlaceGroups::default();
        for (row, state) in input.states.iter().enumerate() {
            if input
                .validity
                .is_none_or(|validity| validity.get(row).unwrap())
            {
                places.push(state);
            }
        }
        if places.groups.is_empty() {
            return Ok(());
        }
        // Same rows as `places`: the batch is filtered by the same validity.
        let arguments = self.create_input_batch(input.columns, input.validity)?;
        let states = places.state_arrays(self)?;
        let updated = python
            .accumulate(states, std::mem::take(&mut places.groups), arguments)
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to accumulate: {e}")))?;
        places.write(&updated);
        Ok(())
    }

    #[cfg(feature = "python-udf")]
    fn merge_serialized_python(
        &self,
        python: &PythonUdaf,
        input: MergeSerializedInput<'_>,
    ) -> Result<()> {
        let view = input.state.downcast::<UnaryType<BinaryType>>().unwrap();
        let mut places = PlaceGroups::default();
        let mut others = Vec::new();
        for (row, (state, mut data)) in input.states.iter().zip(view.iter()).enumerate() {
            if input.filter.is_some_and(|filter| !filter.get_bit(row)) {
                continue;
            }
            places.push(state);
            others.push(deserialize_state(&mut data).map_err(|e| ErrorCode::Internal(e.to_string()))?);
        }
        if others.is_empty() {
            return Ok(());
        }
        let others = others.iter().map(AsRef::as_ref).collect::<Vec<_>>();
        let others = arrow_select::concat::concat(&others)?;
        let states = places.state_arrays(self)?;
        let updated = python
            .merge(states, std::mem::take(&mut places.groups), others)
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to merge: {e}")))?;
        places.write(&updated);
        Ok(())
    }

    #[cfg(debug_assertions)]
    fn check_columns(&self, columns: ProjectedBlock) {
        let fields = self.argument_schema.fields();
        assert_eq!(columns.len(), fields.len());
        for (i, (col, field)) in columns.iter().zip(fields).enumerate() {
            assert_eq!(&col.data_type(), field.data_type(), "args {}", i)
        }
    }

    fn create_input_batch(
        &self,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
    ) -> Result<RecordBatch> {
        #[cfg(debug_assertions)]
        self.check_columns(columns);

        let num_columns = columns.len();
        let num_rows = if num_columns > 0 { columns[0].len() } else { 0 };

        let columns = columns.iter().cloned().collect();
        match validity {
            Some(bitmap) => DataBlock::new(columns, num_rows).filter_with_bitmap(bitmap)?,
            None => DataBlock::new(columns, num_rows),
        }
        .to_record_batch_with_dataschema(&self.argument_schema)
        .map_err(|err| {
            ErrorCode::UDFDataError(format!(
                "Failed to create input batch with {} columns: {}",
                num_columns, err
            ))
        })
    }

    fn create_input_batch_row(&self, columns: ProjectedBlock, row: usize) -> Result<RecordBatch> {
        #[cfg(debug_assertions)]
        self.check_columns(columns);

        let num_columns = columns.len();
        let num_rows = if num_columns > 0 { columns[0].len() } else { 0 };

        let entries = columns.iter().cloned().collect();
        DataBlock::new(entries, num_rows)
            .slice(row..row + 1)
            .to_record_batch_with_dataschema(&self.argument_schema)
            .map_err(|err| {
                ErrorCode::UDFDataError(format!(
                    "Failed to create input batch with {} columns: {}",
                    num_columns, err
                ))
            })
    }
}

/// `None` is the initial state, resolved through `AggregateUdfScript::state_array`.
#[derive(Debug)]
pub struct UdfAggState(Option<Arc<dyn Array>>);

/// Distinct states touched by a block, and the state index of every row.
/// A place may repeat within a block; it is dereferenced mutably only once.
#[cfg(feature = "python-udf")]
#[derive(Default)]
struct PlaceGroups {
    index: HashMap<*const UdfAggState, u32>,
    places: Vec<*mut UdfAggState>,
    groups: Vec<u32>,
}

#[cfg(feature = "python-udf")]
impl PlaceGroups {
    fn push(&mut self, state: AggrState<'_>) {
        let next = self.places.len() as u32;
        let key = state.get_ref::<UdfAggState>() as *const UdfAggState;
        let group = *self.index.entry(key).or_insert(next);
        if group == next {
            self.places.push(state.get::<UdfAggState>());
        }
        self.groups.push(group);
    }

    fn state_arrays(&self, script: &AggregateUdfScript) -> Result<Vec<Arc<dyn Array>>> {
        self.places
            .iter()
            // SAFETY: initialized states of this aggregate, owned by the caller
            // for the duration of the call and distinct within `places`.
            .map(|place| script.state_array(unsafe { &**place }))
            .collect()
    }

    fn write(self, updated: &Arc<dyn Array>) {
        for (group, place) in self.places.into_iter().enumerate() {
            // SAFETY: as in `state_arrays`; the old state is dropped.
            unsafe { *place = UdfAggState(Some(updated.slice(group, 1))) };
        }
    }
}

fn serialize_state(state: &Arc<dyn Array>, writer: &mut Vec<u8>) -> std::result::Result<(), ArrowError> {
    let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "state",
        state.data_type().clone(),
        true,
    )]);
    let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(
        writer,
        &schema,
        arrow_ipc::writer::IpcWriteOptions::default(),
    )?;
    let batch = RecordBatch::try_new(Arc::new(schema), vec![state.clone()])?;
    writer.write(&batch)?;
    writer.finish()
}

fn deserialize_state(bytes: &mut &[u8]) -> std::result::Result<Arc<dyn Array>, ArrowError> {
    let mut cursor = Cursor::new(&bytes);
    let mut reader = arrow_ipc::reader::FileReaderBuilder::new().build(&mut cursor)?;
    let array = reader
        .next()
        .ok_or(ArrowError::ComputeError(
            "expected one arrow array".to_string(),
        ))??
        .remove_column(0);
    bytes.consume(cursor.position() as usize);
    Ok(array)
}

pub fn create_udaf_script_function(
    code: &UDFScriptCode,
    name: String,
    display_name: String,
    state_fields: Vec<DataField>,
    arguments: Vec<DataField>,
    output_type: DataType,
    #[cfg(feature = "python-udf")] python_workers: Option<Arc<PythonQueryWorkers>>,
) -> Result<AggregateCallRef> {
    let UDFScriptCode { language, code, .. } = code;
    let state_type = ArrowType::Struct(
        state_fields
            .iter()
            .map(|f| f.into())
            .collect::<Vec<arrow_schema::Field>>()
            .into(),
    );
    let runtime = match language {
        UDFLanguage::JavaScript => {
            let builder = JsRuntimeBuilder {
                name: name.clone(),
                code: String::from_utf8(code.to_vec())?,
                state_type,
                output_type: output_type.clone(),
            };
            UDAFRuntime::JavaScript(JsRuntimePool::new(builder))
        }
        UDFLanguage::WebAssembly => {
            return Err(ErrorCode::Unimplemented(
                "WebAssembly aggregate UDF is not supported",
            ));
        }
        #[cfg(not(feature = "python-udf"))]
        UDFLanguage::Python => {
            return Err(ErrorCode::EnterpriseFeatureNotEnable(
                "Failed to create python script udf",
            ));
        }
        #[cfg(feature = "python-udf")]
        UDFLanguage::Python => {
            // Re-check on the executing node: plans may be built by another
            // cluster node with a different configuration.
            let config = GlobalConfig::try_get_instance()
                .unwrap_or_else(|| Arc::new(InnerConfig::default()));
            UDFValidator::is_udaf_script_allowed_with_config(
                language,
                &UdfValidationConfig::from_inner_config(&config),
            )?;
            // Like scalar Python UDFs, aggregates run in sandboxed worker
            // processes owned by the query.
            let query = python_workers
                .unwrap_or_else(|| PythonWorkerManager::instance().query("default", 1));
            let function = PythonFunction {
                name: name.clone(),
                handler: String::new(),
                code: String::from_utf8(code.to_vec())?,
                return_field: arrow_field_from_data_type(&name, output_type.clone()),
                aggregate_state: Some(arrow_schema::Field::new(&name, state_type, true)),
            };
            UDAFRuntime::Python(PythonUdaf {
                runtime: PythonRuntime::new(function, query, None)?,
                handle: tokio::runtime::Handle::try_current()
                    .unwrap_or_else(|_| GlobalIORuntime::instance().inner()),
                output_type: output_type.clone(),
            })
        }
    };
    let argument_schema = DataSchema::new(arguments);
    let init_state = OnceLock::new();
    if !runtime.is_python() {
        // In-process engines report broken code while the pipeline is built.
        let state = runtime
            .create_state()
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to create state: {e}")))?;
        let _ = init_state.set(state.0.unwrap());
    }

    Ok(Arc::new(AggregateCallInstance::new(
        AggregateSignature {
            name,
            params: vec![],
            args_type: argument_schema
                .fields()
                .iter()
                .map(|field| field.data_type().clone())
                .collect(),
            distinct: false,
            order_by: vec![],
            return_type: output_type,
        },
        FunctionInputLayout::Identity,
        AggregateFeatures::default(),
        AggregateStateDescription::new(
            vec![AggrStateType::Custom(Layout::new::<UdfAggState>())],
            vec![StateSerdeItem::Binary(None)],
        )
        .with_manual_drop(true),
        AggregateUdfScript {
            display_name,
            runtime,
            argument_schema,
            init_state,
        },
    )))
}

struct JsRuntimeBuilder {
    name: String,
    code: String,
    state_type: ArrowType,
    output_type: DataType,
}

impl RuntimeBuilder<arrow_udf_runtime::javascript::Runtime> for JsRuntimeBuilder {
    type Error = ErrorCode;

    fn build(&self) -> std::result::Result<arrow_udf_runtime::javascript::Runtime, Self::Error> {
        let limits = ScriptUdfLimits::current();
        let mut runtime = GlobalIORuntime::instance().block_on(async move {
            let runtime = arrow_udf_runtime::javascript::Runtime::new()
                .await
                .map_err(|e| ErrorCode::UDFDataError(format!("Cannot create js runtime: {e}")))?;
            limit_js_runtime_init(&runtime, &limits).await;
            Ok(runtime)
        })?;
        let converter = runtime.converter_mut();
        converter.set_arrow_extension_key(EXTENSION_KEY);
        converter.set_json_extension_name(ARROW_EXT_TYPE_VARIANT);

        GlobalIORuntime::instance().block_on(async move {
            runtime
                .add_aggregate(
                    &self.name,
                    self.state_type.clone(),
                    // we pass the field instead of the data type because arrow-udf-js
                    // now takes the field as an argument here so that it can get any
                    // metadata associated with the field
                    arrow_field_from_data_type(&self.name, self.output_type.clone()),
                    &self.code,
                    AggregateOptions::default().return_null_on_null_input(),
                )
                .await
                .map_err(|e| ErrorCode::UDFDataError(format!("Cannot add aggregate: {e}")))?;
            limit_js_runtime_calls(&mut runtime, &limits).await;

            Ok(runtime)
        })
    }
}

fn arrow_field_from_data_type(name: &str, dt: DataType) -> arrow_schema::Field {
    let field = DataField::new(name, dt);
    (&field).into()
}

type JsRuntimePool = Pool<arrow_udf_runtime::javascript::Runtime, JsRuntimeBuilder>;

/// Aggregate requests to the query's Python workers. Pipeline processors
/// call aggregate functions synchronously, so each request blocks on the
/// runtime that built the pipeline; worker sockets stay on that runtime.
#[cfg(feature = "python-udf")]
struct PythonUdaf {
    runtime: Arc<PythonRuntime>,
    handle: tokio::runtime::Handle,
    output_type: DataType,
}

#[cfg(feature = "python-udf")]
impl PythonUdaf {
    fn request(&self, op: AggregateOp, parts: &[RecordBatch]) -> Result<Arc<dyn Array>> {
        let request = encode_aggregate_request(op, parts)?;
        let response = databend_common_base::runtime::block_on_with_handle(
            &self.handle,
            self.runtime.request(&request),
        )?;
        Ok(decode_batch(&response)?.column(0).clone())
    }

    fn states(states: Vec<Arc<dyn Array>>) -> Result<RecordBatch> {
        let states = states.iter().map(AsRef::as_ref).collect::<Vec<_>>();
        array_batch(arrow_select::concat::concat(&states)?)
    }

    fn groups(groups: Vec<u32>) -> Result<RecordBatch> {
        array_batch(Arc::new(arrow_array::UInt32Array::from(groups)))
    }

    fn create_state(&self) -> Result<Arc<dyn Array>> {
        self.request(AggregateOp::CreateState, &[])
    }

    fn accumulate(
        &self,
        states: Vec<Arc<dyn Array>>,
        groups: Vec<u32>,
        arguments: RecordBatch,
    ) -> Result<Arc<dyn Array>> {
        self.request(AggregateOp::Accumulate, &[
            Self::states(states)?,
            Self::groups(groups)?,
            arguments,
        ])
    }

    fn merge(
        &self,
        states: Vec<Arc<dyn Array>>,
        groups: Vec<u32>,
        others: Arc<dyn Array>,
    ) -> Result<Arc<dyn Array>> {
        self.request(AggregateOp::Merge, &[
            Self::states(states)?,
            Self::groups(groups)?,
            array_batch(others)?,
        ])
    }

    fn finish(&self, states: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
        self.request(AggregateOp::Finish, &[array_batch(states)?])
    }
}

enum UDAFRuntime {
    JavaScript(JsRuntimePool),
    #[cfg(feature = "python-udf")]
    Python(PythonUdaf),
}

#[cfg(feature = "python-udf")]
fn worker_error(error: ErrorCode) -> anyhow::Error {
    anyhow::anyhow!("{}", error.message())
}

impl UDAFRuntime {
    fn is_python(&self) -> bool {
        match self {
            UDAFRuntime::JavaScript(_) => false,
            #[cfg(feature = "python-udf")]
            UDAFRuntime::Python(_) => true,
        }
    }

    fn return_type(&self) -> DataType {
        match self {
            UDAFRuntime::JavaScript(pool) => pool.builder.output_type.clone(),
            #[cfg(feature = "python-udf")]
            UDAFRuntime::Python(python) => python.output_type.clone(),
        }
    }

    fn create_state(&self) -> anyhow::Result<UdfAggState> {
        let state = match self {
            UDAFRuntime::JavaScript(pool) => pool.call(|runtime| {
                GlobalIORuntime::instance()
                    .block_on(async move { Ok(runtime.create_state(&pool.builder.name).await?) })
                    .map_err(|err| anyhow::anyhow!(err))
            }),
            #[cfg(feature = "python-udf")]
            UDAFRuntime::Python(python) => python.create_state().map_err(worker_error),
        }?;
        Ok(UdfAggState(Some(state)))
    }

    fn accumulate(
        &self,
        state: &Arc<dyn Array>,
        input: &RecordBatch,
    ) -> anyhow::Result<UdfAggState> {
        let state = match self {
            UDAFRuntime::JavaScript(pool) => pool.call(|runtime| {
                GlobalIORuntime::instance()
                    .block_on(async move {
                        Ok(runtime
                            .accumulate(&pool.builder.name, state, input)
                            .await?)
                    })
                    .map_err(|err| anyhow::anyhow!(err))
            }),
            #[cfg(feature = "python-udf")]
            UDAFRuntime::Python(python) => python
                .accumulate(vec![state.clone()], vec![0; input.num_rows()], input.clone())
                .map_err(worker_error),
        }?;
        Ok(UdfAggState(Some(state)))
    }

    /// Merge `states[1..]` into `states[0]`.
    fn merge(&self, states: &Arc<dyn Array>) -> anyhow::Result<UdfAggState> {
        let state = match self {
            UDAFRuntime::JavaScript(pool) => pool.call(|runtime| {
                GlobalIORuntime::instance()
                    .block_on(async move { Ok(runtime.merge(&pool.builder.name, states).await?) })
                    .map_err(|err| anyhow::anyhow!(err))
            }),
            #[cfg(feature = "python-udf")]
            UDAFRuntime::Python(python) => python
                .merge(
                    vec![states.slice(0, 1)],
                    vec![0; states.len() - 1],
                    states.slice(1, states.len() - 1),
                )
                .map_err(worker_error),
        }?;
        Ok(UdfAggState(Some(state)))
    }

    fn finish(&self, state: &Arc<dyn Array>) -> anyhow::Result<Arc<dyn Array>> {
        match self {
            UDAFRuntime::JavaScript(pool) => pool.call(|runtime| {
                GlobalIORuntime::instance()
                    .block_on(async move { Ok(runtime.finish(&pool.builder.name, state).await?) })
                    .map_err(|err| anyhow::anyhow!(err))
            }),
            #[cfg(feature = "python-udf")]
            UDAFRuntime::Python(python) => python.finish(state.clone()).map_err(worker_error),
        }
    }
}
