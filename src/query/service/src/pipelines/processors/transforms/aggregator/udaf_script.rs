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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::converts::arrow::ARROW_EXT_TYPE_VARIANT;
use databend_common_expression::converts::arrow::EXTENSION_KEY;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::InputColumns;
use databend_common_expression::StateAddr;
use databend_common_functions::aggregates::AggregateFunction;
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

    fn init_state(&self, place: StateAddr) {
        place.write_state(UdfAggState(self.init_state.0.clone()));
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<UdfAggState>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: InputColumns,
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let input_batch = self.create_input_batch(columns, validity)?;
        let state = place.get::<UdfAggState>();
        let state = self
            .runtime
            .accumulate(state, &input_batch)
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to accumulate: {e}")))?;
        place.write_state(state);
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: InputColumns, row: usize) -> Result<()> {
        let input_batch = self.create_input_batch_row(columns, row)?;
        let state = place.get::<UdfAggState>();
        let state = self
            .runtime
            .accumulate(state, &input_batch)
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to accumulate_row: {e}")))?;
        place.write_state(state);
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<UdfAggState>();
        state
            .serialize(writer)
            .map_err(|e| ErrorCode::Internal(format!("state failed to serialize: {e}")))
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<UdfAggState>();
        let rhs =
            UdfAggState::deserialize(reader).map_err(|e| ErrorCode::Internal(e.to_string()))?;
        let states = arrow_select::concat::concat(&[&state.0, &rhs.0])?;
        let state = self
            .runtime
            .merge(&states)
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to merge: {e}")))?;
        place.write_state(state);
        Ok(())
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<UdfAggState>();
        let other = rhs.get::<UdfAggState>();
        let states = arrow_select::concat::concat(&[&state.0, &other.0])
            .map_err(|e| ErrorCode::Internal(e.to_string()))?;
        let state = self
            .runtime
            .merge(&states)
            .map_err(|e| ErrorCode::UDFRuntimeError(format!("failed to merge_states: {e}")))?;
        place.write_state(state);
        Ok(())
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
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

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<UdfAggState>();
        std::ptr::drop_in_place(state);
    }
}

impl fmt::Display for AggregateUdfScript {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl AggregateUdfScript {
    #[cfg(debug_assertions)]
    fn check_columns(&self, columns: InputColumns) {
        let fields = self.argument_schema.fields();
        assert_eq!(columns.len(), fields.len());
        for (i, (col, field)) in columns.iter().zip(fields).enumerate() {
            assert_eq!(&col.data_type(), field.data_type(), "args {}", i)
        }
    }

    fn create_input_batch(
        &self,
        columns: InputColumns,
        validity: Option<&Bitmap>,
    ) -> Result<RecordBatch> {
        #[cfg(debug_assertions)]
        self.check_columns(columns);

        let num_columns = columns.len();

        let columns = columns.iter().cloned().collect();
        match validity {
            Some(bitmap) => DataBlock::new_from_columns(columns).filter_with_bitmap(bitmap)?,
            None => DataBlock::new_from_columns(columns),
        }
        .to_record_batch_with_dataschema(&self.argument_schema)
        .map_err(|err| {
            ErrorCode::UDFDataError(format!(
                "Failed to create input batch with {} columns: {}",
                num_columns, err
            ))
        })
    }

    fn create_input_batch_row(&self, columns: InputColumns, row: usize) -> Result<RecordBatch> {
        #[cfg(debug_assertions)]
        self.check_columns(columns);

        let num_columns = columns.len();

        let columns = columns.iter().cloned().collect();
        DataBlock::new_from_columns(columns)
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

impl RuntimeBuilder<arrow_udf_js::Runtime> for JsRuntimeBuilder {
    type Error = ErrorCode;

    fn build(&self) -> std::result::Result<arrow_udf_js::Runtime, Self::Error> {
        let mut runtime = match arrow_udf_js::Runtime::new() {
            Ok(runtime) => runtime,
            Err(e) => {
                return Err(ErrorCode::UDFDataError(format!(
                    "Cannot create js runtime: {e}"
                )))
            }
        };

        let converter = runtime.converter_mut();
        converter.set_arrow_extension_key(EXTENSION_KEY);
        converter.set_json_extension_name(ARROW_EXT_TYPE_VARIANT);

        let output_type: ArrowType = (&self.output_type).into();
        runtime
            .add_aggregate(
                &self.name,
                self.state_type.clone(),
                output_type,
                arrow_udf_js::CallMode::CalledOnNullInput,
                &self.code,
            )
            .map_err(|e| ErrorCode::UDFDataError(format!("Cannot add aggregate: {e}")))?;

        Ok(runtime)
    }
}

type JsRuntimePool = Pool<arrow_udf_js::Runtime, JsRuntimeBuilder>;

#[cfg(feature = "python-udf")]
mod python_pool {
    use super::*;

    pub(super) struct PyRuntimeBuilder {
        pub name: String,
        pub code: String,
        pub state_type: ArrowType,
        pub output_type: DataType,
    }

    impl RuntimeBuilder<arrow_udf_python::Runtime> for PyRuntimeBuilder {
        type Error = ErrorCode;

        fn build(&self) -> std::result::Result<arrow_udf_python::Runtime, Self::Error> {
            let mut runtime = arrow_udf_python::Builder::default()
                .sandboxed(true)
                .build()?;
            let output_type: ArrowType = (&self.output_type).into();
            runtime.add_aggregate(
                &self.name,
                self.state_type.clone(),
                output_type,
                arrow_udf_python::CallMode::CalledOnNullInput,
                &self.code,
            )?;
            Ok(runtime)
        }
    }

    pub type PyRuntimePool = Pool<arrow_udf_python::Runtime, PyRuntimeBuilder>;
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
            UDAFRuntime::JavaScript(pool) => {
                pool.call(|runtime| runtime.create_state(&pool.builder.name))
            }
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
            UDAFRuntime::JavaScript(pool) => {
                pool.call(|runtime| runtime.accumulate(&pool.builder.name, &state.0, input))
            }
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
            UDAFRuntime::JavaScript(pool) => {
                pool.call(|runtime| runtime.merge(&pool.builder.name, states))
            }
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
            UDAFRuntime::JavaScript(pool) => {
                pool.call(|runtime| runtime.finish(&pool.builder.name, &state.0))
            }
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

    #[test]
    fn test_js_pool() -> Result<()> {
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

        let state = pool.call(|runtime| runtime.create_state(&agg_name))?;

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
