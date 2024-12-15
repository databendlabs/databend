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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::InputColumns;
use databend_common_expression::StateAddr;
use databend_common_expression::TableField;

use crate::aggregates::AggregateFunction;

pub struct AggregateUdfScript {
    runtime: arrow_udf_js::Runtime,
    argument_schema: DataSchema,
    return_type: DataType,
}

impl AggregateFunction for AggregateUdfScript {
    fn name(&self) -> &str {
        "udf" // todo
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn init_state(&self, place: StateAddr) {
        let state = self.runtime.create_state(self.agg_name()).unwrap(); // todo
        place.write_state(UdfAggState(state));
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
            .accumulate(self.agg_name(), &state.0, &input_batch)
            .unwrap();
        place.write_state(UdfAggState(state));
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: InputColumns, row: usize) -> Result<()> {
        let input_batch = self.create_input_batch_row(columns, row)?;
        let state = place.get::<UdfAggState>();
        let state = self
            .runtime
            .accumulate(self.agg_name(), &state.0, &input_batch)
            .unwrap();
        place.write_state(UdfAggState(state));
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<UdfAggState>();
        state.serialize(writer).unwrap();
        Ok(())
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<UdfAggState>();
        let rhs = UdfAggState::deserialize(reader).unwrap();
        let states = arrow_select::concat::concat(&[&state.0, &rhs.0]).unwrap(); // todo
        let state = self.runtime.merge(self.agg_name(), &states).unwrap();
        place.write_state(UdfAggState(state));
        Ok(())
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<UdfAggState>();
        let other = rhs.get::<UdfAggState>();
        let states = arrow_select::concat::concat(&[&state.0, &other.0]).unwrap(); // todo
        let state = self.runtime.merge(self.agg_name(), &states).unwrap();
        place.write_state(UdfAggState(state));
        Ok(())
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<UdfAggState>();
        let array = self.runtime.finish(self.agg_name(), &state.0).unwrap();
        let result = Column::from_arrow_rs(array, &self.return_type()?)?;
        builder.append_column(&result);
        Ok(())
    }
}

impl fmt::Display for AggregateUdfScript {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "udf")
    }
}

impl AggregateUdfScript {
    fn agg_name(&self) -> &str {
        "weighted_avg"
    }

    #[allow(dead_code)]
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

pub fn create_aggregate_udf_function(
    name: &str,
    _lang: &str,
    state_fields: Vec<DataField>,
    arguments: Vec<DataField>,
    return_type: DataType,
    code: &str,
) -> Result<Arc<dyn AggregateFunction>> {
    use arrow_schema::DataType as ArrowType;
    use arrow_schema::Field;
    use arrow_udf_js::CallMode;
    let mut runtime = arrow_udf_js::Runtime::new().unwrap();

    let state_type = ArrowType::Struct(
        state_fields
            .iter()
            .map(|f| {
                let table_field: TableField = f.into();
                (&table_field).into()
            })
            .collect::<Vec<Field>>()
            .into(),
    );

    runtime
        .add_aggregate(
            name,
            state_type,
            ArrowType::Float32,
            CallMode::CalledOnNullInput,
            code,
        )
        .unwrap();

    Ok(Arc::new(AggregateUdfScript {
        runtime,
        argument_schema: DataSchema::new(arguments),
        return_type,
    }))
}

#[cfg(test)]
mod tests {

    use arrow_array::ArrayRef;
    use arrow_array::Int32Array;
    use arrow_cast::pretty::pretty_format_columns;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::Schema;
    use arrow_udf_js::CallMode;

    use super::*;

    #[test]
    fn test_weighted_avg() {
        let mut runtime = arrow_udf_js::Runtime::new().unwrap();
        runtime
            .add_aggregate(
                "weighted_avg",
                DataType::Struct(
                    vec![
                        Field::new("sum", DataType::Int32, false),
                        Field::new("weight", DataType::Int32, false),
                    ]
                    .into(),
                ),
                DataType::Float32,
                CallMode::ReturnNullOnNullInput,
                r#"
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
    "#,
            )
            .unwrap();

        let schema = Schema::new(vec![
            Field::new("value", DataType::Int32, true),
            Field::new("weight", DataType::Int32, true),
        ]);
        let arg0 = Int32Array::from(vec![Some(1), None, Some(3), Some(5)]);
        let arg1 = Int32Array::from(vec![Some(2), None, Some(4), Some(6)]);
        let input =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

        let state = runtime.create_state("weighted_avg").unwrap();
        check_array(
            std::slice::from_ref(&state),
            r#"+---------------------+
| array               |
+---------------------+
| {sum: 0, weight: 0} |
+---------------------+"#,
        );

        let state = runtime.accumulate("weighted_avg", &state, &input).unwrap();
        check_array(
            std::slice::from_ref(&state),
            r#"+-----------------------+
| array                 |
+-----------------------+
| {sum: 44, weight: 12} |
+-----------------------+"#,
        );

        let states = arrow_select::concat::concat(&[&state, &state]).unwrap();
        let state = runtime.merge("weighted_avg", &states).unwrap();
        check_array(
            std::slice::from_ref(&state),
            r#"+-----------------------+
| array                 |
+-----------------------+
| {sum: 88, weight: 24} |
+-----------------------+"#,
        );

        let output = runtime.finish("weighted_avg", &state).unwrap();
        check_array(
            &[output],
            r#"+-----------+
| array     |
+-----------+
| 3.6666667 |
+-----------+"#,
        );
    }

    /// Compare the actual output with the expected output.
    #[track_caller]
    fn check_array(actual: &[ArrayRef], expect: &str) {
        assert_eq!(
            expect,
            &pretty_format_columns("array", actual).unwrap().to_string(),
        );
    }
}
