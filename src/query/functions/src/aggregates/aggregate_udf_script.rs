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
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::InputColumns;
use databend_common_expression::StateAddr;

use crate::aggregates::AggregateFunction;

pub struct AggregateUdfScript {
    name: String,
    runtime: arrow_udf_js::Runtime,
    argument_schema: DataSchema,
    return_type: DataType,
}

impl AggregateFunction for AggregateUdfScript {
    fn name(&self) -> &str {
        &self.name
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn init_state(&self, place: StateAddr) {
        let state = self.runtime.create_state(self.agg_name()).unwrap(); // todo
        log::info!("init_state: {:?}", state);
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
        log::info!("accumulate: {:?}", state);
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
        log::info!("accumulate_row: {:?}", state);
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
        log::info!("merge: {:?}", state);
        place.write_state(UdfAggState(state));
        Ok(())
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<UdfAggState>();
        let other = rhs.get::<UdfAggState>();
        let states = arrow_select::concat::concat(&[&state.0, &other.0]).unwrap(); // todo
        let state = self.runtime.merge(self.agg_name(), &states).unwrap();
        log::info!("merge_states: {:?}", state);
        place.write_state(UdfAggState(state));
        Ok(())
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<UdfAggState>();
        let array = self.runtime.finish(self.agg_name(), &state.0).unwrap();
        log::info!("merge_result: {:?}", array);
        let result = Column::from_arrow_rs(array, &self.return_type()?)?;
        builder.append_column(&result);
        Ok(())
    }
}

impl fmt::Display for AggregateUdfScript {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name) // todo
    }
}

impl AggregateUdfScript {
    fn agg_name(&self) -> &str {
        &self.name
    }

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
    _runtime_version: &str,
    state_fields: Vec<DataField>,
    arguments: Vec<DataField>,
    return_type: DataType,
    code: &[u8],
) -> Result<Arc<dyn AggregateFunction>> {
    let mut runtime = arrow_udf_js::Runtime::new().unwrap();

    let state_type = ArrowType::Struct(
        state_fields
            .iter()
            .map(|f| f.into())
            .collect::<Vec<arrow_schema::Field>>()
            .into(),
    );

    log::info!("state_type: {:?}", state_type);

    let code = String::from_utf8(code.to_vec())?;
    let output_type: ArrowType = (&return_type).into();
    runtime
        .add_aggregate(
            name,
            state_type,
            output_type,
            arrow_udf_js::CallMode::CalledOnNullInput,
            &code,
        )
        .unwrap();

    Ok(Arc::new(AggregateUdfScript {
        name: name.to_string(),
        runtime,
        argument_schema: DataSchema::new(arguments),
        return_type,
    }))
}
