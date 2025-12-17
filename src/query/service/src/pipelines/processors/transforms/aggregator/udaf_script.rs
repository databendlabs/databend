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
use std::fmt;
use std::io::BufRead;
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use arrow_schema::DataType as ArrowType;
use arrow_udf_runtime::javascript::AggregateOptions;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::converts::arrow::ARROW_EXT_TYPE_VARIANT;
use databend_common_expression::converts::arrow::EXTENSION_KEY;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UnaryType;
use databend_common_functions::aggregates::AggrStateLoc;
use databend_common_functions::aggregates::AggregateFunction;
use databend_common_functions::aggregates::StateAddr;
use databend_common_sql::plans::UDFLanguage;
use databend_common_sql::plans::UDFScriptCode;

use super::runtime_pool::Pool;
use super::runtime_pool::RuntimeBuilder;

pub struct AggregateUdfScript {
    display_name: String,
    runtime: UDAFRuntime,
    argument_schema: DataSchema,
    init_state: UdfAggState,
}

impl AggregateFunction for AggregateUdfScript {
    fn name(&self) -> &str {
        self.runtime.name()
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.runtime.return_type())
    }

    fn init_state(&self, place: AggrState) {
        place.write(|| UdfAggState(self.init_state.0.clone()));
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<UdfAggState>()));
    }

    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let input_batch = self.create_input_batch(columns, validity)?;
        let state = place.get::<UdfAggState>();
        let state = self
            .runtime
            .accumulate(state, &input_batch)
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to accumulate: {e}")))?;
        place.write(|| state);
        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        let input_batch = self.create_input_batch_row(columns, row)?;
        let state = place.get::<UdfAggState>();
        let state = self
            .runtime
            .accumulate(state, &input_batch)
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to accumulate_row: {e}")))?;
        place.write(|| state);
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(None)]
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let binary_builder = builders[0].as_binary_mut().unwrap();
        for place in places {
            let state = AggrState::new(*place, loc).get::<UdfAggState>();
            state
                .serialize(&mut binary_builder.data)
                .map_err(|e| ErrorCode::Internal(format!("state failed to serialize: {e}")))?;
            binary_builder.commit_row();
        }
        Ok(())
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        let view = state.downcast::<UnaryType<BinaryType>>().unwrap();
        let iter = places.iter().zip(view.iter());

        if let Some(filter) = filter {
            for (place, mut data) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v)) {
                let state = AggrState::new(*place, loc).get::<UdfAggState>();
                let rhs = UdfAggState::deserialize(&mut data)
                    .map_err(|e| ErrorCode::Internal(e.to_string()))?;
                let states = arrow_select::concat::concat(&[&state.0, &rhs.0])?;
                let merged_state = self
                    .runtime
                    .merge(&states)
                    .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to merge: {e}")))?;
                AggrState::new(*place, loc).write(|| merged_state);
            }
        } else {
            for (place, mut data) in iter {
                let state = AggrState::new(*place, loc).get::<UdfAggState>();
                let rhs = UdfAggState::deserialize(&mut data)
                    .map_err(|e| ErrorCode::Internal(e.to_string()))?;
                let states = arrow_select::concat::concat(&[&state.0, &rhs.0])?;
                let merged_state = self
                    .runtime
                    .merge(&states)
                    .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to merge: {e}")))?;
                AggrState::new(*place, loc).write(|| merged_state);
            }
        }
        Ok(())
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<UdfAggState>();
        let other = rhs.get::<UdfAggState>();
        let states = arrow_select::concat::concat(&[&state.0, &other.0])
            .map_err(|e| ErrorCode::Internal(e.to_string()))?;
        let state = self
            .runtime
            .merge(&states)
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to merge_states: {e}")))?;
        place.write(|| state);
        Ok(())
    }

    fn merge_result(
        &self,
        place: AggrState,
        _read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let state = place.get::<UdfAggState>();
        let array = self
            .runtime
            .finish(state)
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to merge_result: {e}")))?;
        let result = Column::from_arrow_rs(array, &self.runtime.return_type())?;
        builder.append_column(&result);
        Ok(())
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
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

#[derive(Debug)]
pub struct UdfAggState(Arc<dyn Array>);

impl UdfAggState {
    fn serialize(&self, writer: &mut Vec<u8>) -> std::result::Result<(), ArrowError> {
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "state",
            self.0.data_type().clone(),
            true,
        )]);
        let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(
            writer,
            &schema,
            arrow_ipc::writer::IpcWriteOptions::default(),
        )?;
        let batch = RecordBatch::try_new(Arc::new(schema), vec![self.0.clone()])?;
        writer.write(&batch)?;
        writer.finish()
    }

    fn deserialize(bytes: &mut &[u8]) -> std::result::Result<Self, ArrowError> {
        let mut cursor = Cursor::new(&bytes);
        let mut reader = arrow_ipc::reader::FileReaderBuilder::new().build(&mut cursor)?;
        let array = reader
            .next()
            .ok_or(ArrowError::ComputeError(
                "expected one arrow array".to_string(),
            ))??
            .remove_column(0);
        bytes.consume(cursor.position() as usize);
        Ok(Self(array))
    }
}

pub fn create_udaf_script_function(
    code: &UDFScriptCode,
    name: String,
    display_name: String,
    state_fields: Vec<DataField>,
    arguments: Vec<DataField>,
    output_type: DataType,
) -> Result<Arc<dyn AggregateFunction>> {
    let UDFScriptCode { language, code, .. } = code;
    let runtime = match language {
        UDFLanguage::JavaScript => {
            let builder = JsRuntimeBuilder {
                name,
                code: String::from_utf8(code.to_vec())?,
                state_type: ArrowType::Struct(
                    state_fields
                        .iter()
                        .map(|f| f.into())
                        .collect::<Vec<arrow_schema::Field>>()
                        .into(),
                ),
                output_type,
            };
            UDAFRuntime::JavaScript(JsRuntimePool::new(builder))
        }
        UDFLanguage::WebAssembly => unimplemented!(),
        #[cfg(not(feature = "python-udf"))]
        UDFLanguage::Python => {
            return Err(ErrorCode::EnterpriseFeatureNotEnable(
                "Failed to create python script udf",
            ));
        }
        #[cfg(feature = "python-udf")]
        UDFLanguage::Python => {
            let builder = python_pool::PyRuntimeBuilder {
                name,
                code: String::from_utf8(code.to_vec())?,
                state_type: ArrowType::Struct(
                    state_fields
                        .iter()
                        .map(|f| f.into())
                        .collect::<Vec<arrow_schema::Field>>()
                        .into(),
                ),
                output_type,
            };
            UDAFRuntime::Python(Pool::new(builder))
        }
    };
    let init_state = runtime
        .create_state()
        .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to create state: {e}")))?;

    Ok(Arc::new(AggregateUdfScript {
        display_name,
        runtime,
        argument_schema: DataSchema::new(arguments),
        init_state,
    }))
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
        let mut runtime = GlobalIORuntime::instance().block_on(async move {
            arrow_udf_runtime::javascript::Runtime::new()
                .await
                .map_err(|e| ErrorCode::UDFDataError(format!("Cannot create js runtime: {e}")))
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

            Ok(runtime)
        })
    }
}

fn arrow_field_from_data_type(name: &str, dt: DataType) -> arrow_schema::Field {
    let field = DataField::new(name, dt);
    (&field).into()
}

type JsRuntimePool = Pool<arrow_udf_runtime::javascript::Runtime, JsRuntimeBuilder>;

#[cfg(feature = "python-udf")]
mod python_pool {
    use super::*;

    pub(super) struct PyRuntimeBuilder {
        pub name: String,
        pub code: String,
        pub state_type: ArrowType,
        pub output_type: DataType,
    }

    impl RuntimeBuilder<arrow_udf_runtime::python::Runtime> for PyRuntimeBuilder {
        type Error = ErrorCode;

        fn build(&self) -> std::result::Result<arrow_udf_runtime::python::Runtime, Self::Error> {
            let mut runtime = arrow_udf_runtime::python::Builder::default().build()?;
            runtime.add_aggregate(
                &self.name,
                self.state_type.clone(),
                arrow_field_from_data_type(&self.name, self.output_type.clone()),
                arrow_udf_runtime::CallMode::CalledOnNullInput,
                &self.code,
            )?;
            Ok(runtime)
        }
    }

    pub type PyRuntimePool = Pool<arrow_udf_runtime::python::Runtime, PyRuntimeBuilder>;
}

enum UDAFRuntime {
    JavaScript(JsRuntimePool),
    #[expect(unused)]
    WebAssembly,
    #[cfg(feature = "python-udf")]
    Python(python_pool::PyRuntimePool),
}

impl UDAFRuntime {
    fn name(&self) -> &str {
        match self {
            UDAFRuntime::JavaScript(pool) => &pool.builder.name,
            #[cfg(feature = "python-udf")]
            UDAFRuntime::Python(info) => &info.builder.name,
            _ => unimplemented!(),
        }
    }

    fn return_type(&self) -> DataType {
        match self {
            UDAFRuntime::JavaScript(pool) => pool.builder.output_type.clone(),
            #[cfg(feature = "python-udf")]
            UDAFRuntime::Python(info) => info.builder.output_type.clone(),
            _ => unimplemented!(),
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
            UDAFRuntime::Python(pool) => {
                pool.call(|runtime| runtime.create_state(&pool.builder.name))
            }
            _ => unimplemented!(),
        }?;
        Ok(UdfAggState(state))
    }

    fn accumulate(&self, state: &UdfAggState, input: &RecordBatch) -> anyhow::Result<UdfAggState> {
        let state = match self {
            UDAFRuntime::JavaScript(pool) => pool.call(|runtime| {
                GlobalIORuntime::instance()
                    .block_on(async move {
                        Ok(runtime
                            .accumulate(&pool.builder.name, &state.0, input)
                            .await?)
                    })
                    .map_err(|err| anyhow::anyhow!(err))
            }),
            #[cfg(feature = "python-udf")]
            UDAFRuntime::Python(pool) => {
                pool.call(|runtime| runtime.accumulate(&pool.builder.name, &state.0, input))
            }
            _ => unimplemented!(),
        }?;
        Ok(UdfAggState(state))
    }

    fn merge(&self, states: &Arc<dyn Array>) -> anyhow::Result<UdfAggState> {
        let state = match self {
            UDAFRuntime::JavaScript(pool) => pool.call(|runtime| {
                GlobalIORuntime::instance()
                    .block_on(async move { Ok(runtime.merge(&pool.builder.name, states).await?) })
                    .map_err(|err| anyhow::anyhow!(err))
            }),
            #[cfg(feature = "python-udf")]
            UDAFRuntime::Python(pool) => {
                pool.call(|runtime| runtime.merge(&pool.builder.name, states))
            }
            _ => unimplemented!(),
        }?;
        Ok(UdfAggState(state))
    }

    fn finish(&self, state: &UdfAggState) -> anyhow::Result<Arc<dyn Array>> {
        match self {
            UDAFRuntime::JavaScript(pool) => pool.call(|runtime| {
                GlobalIORuntime::instance()
                    .block_on(
                        async move { Ok(runtime.finish(&pool.builder.name, &state.0).await?) },
                    )
                    .map_err(|err| anyhow::anyhow!(err))
            }),
            #[cfg(feature = "python-udf")]
            UDAFRuntime::Python(pool) => {
                pool.call(|runtime| runtime.finish(&pool.builder.name, &state.0))
            }
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use arrow_array::Int32Array;
    use arrow_array::Int64Array;
    use arrow_array::StructArray;
    use arrow_schema::DataType as ArrowType;
    use arrow_schema::Field;
    use databend_common_expression::types::ArgType;
    use databend_common_expression::types::Float32Type;

    use super::*;
    use crate::test_kits::TestFixture;

    #[test]
    fn test_serialize() {
        let want: Arc<dyn Array> = Arc::new(StructArray::new(
            vec![
                Field::new("a", ArrowType::Int32, false),
                Field::new("b", ArrowType::Int64, false),
            ]
            .into(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![4, 5, 6])),
            ],
            None,
        ));

        let state = UdfAggState(want.clone());
        let mut buf = Vec::new();
        state.serialize(&mut buf).unwrap();

        let state = UdfAggState::deserialize(&mut buf.as_slice()).unwrap();
        assert_eq!(&want, &state.0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_js_pool() -> Result<()> {
        let _fixture = TestFixture::setup().await?;
        let agg_name = "weighted_avg".to_string();
        let fields = vec![
            Field::new("sum", ArrowType::Int64, false),
            Field::new("weight", ArrowType::Int64, false),
        ];
        let builder = JsRuntimeBuilder {
            name: agg_name.clone(),
            code: r#"
export function create_state() {
    return {sum: 0, weight: 0};
}
export function accumulate(state, value, weight) {
    state.sum += value * weight;
    state.weight += weight;
    return state;
}
export function retract(state, value, weight) {
    state.sum -= value * weight;
    state.weight -= weight;
    return state;
}
export function merge(state1, state2) {
    state1.sum += state2.sum;
    state1.weight += state2.weight;
    return state1;
}
export function finish(state) {
    return state.sum / state.weight;
}
            "#
            .to_string(),
            state_type: ArrowType::Struct(fields.clone().into()),
            output_type: Float32Type::data_type(),
        };
        let pool = JsRuntimePool::new(builder);

        let state = pool.call(|runtime| {
            GlobalIORuntime::instance()
                .block_on(async move { Ok(runtime.create_state(&agg_name).await?) })
                .map_err(|e| anyhow::anyhow!(e))
        })?;

        let want: Arc<dyn arrow_array::Array> = Arc::new(StructArray::new(
            fields.into(),
            vec![
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(Int64Array::from(vec![0])),
            ],
            None,
        ));

        assert_eq!(&want, &state);
        Ok(())
    }

    #[cfg(feature = "python-udf")]
    #[test]
    fn test_python_runtime() -> Result<()> {
        use databend_common_expression::types::Int32Type;

        let code = Vec::from(
            r#"
class State:
    def __init__(self):
        self.sum = 0
        self.weight = 0

def create_state():
    return State()

def accumulate(state, value, weight):
    state.sum += value * weight
    state.weight += weight
    return state

def merge(state1, state2):
    state1.sum += state2.sum
    state1.weight += state2.weight
    return state1

def finish(state):
    if state.weight == 0:
        return None
    else:
        return state.sum / state.weight
"#,
        )
        .into_boxed_slice();

        let script = UDFScriptCode {
            language: UDFLanguage::Python,
            code: code.into(),
            runtime_version: "3.12".to_string(),
            imports: vec![],
            imports_stage_info: vec![],
            packages: vec![],
        };
        let name = "test".to_string();
        let display_name = "test".to_string();
        let state_fields = vec![
            DataField::new("sum", Int32Type::data_type()),
            DataField::new("weight", Int32Type::data_type()),
        ];
        let arguments = vec![DataField::new("value", Int32Type::data_type())];
        let output_type = Float32Type::data_type();
        create_udaf_script_function(
            &script,
            name,
            display_name,
            state_fields,
            arguments,
            output_type,
        )?;
        Ok(())
    }
}
