// Copyright 2020 Datafuse Labs.
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

use chrono_tz::Tz;
use common_datablocks::DataBlock;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DateConverter;
use common_exception::exception::ABORT_QUERY;
use common_exception::exception::ABORT_SESSION;
use common_exception::ErrorCode;
use common_exception::Result;
use msql_srv::*;

pub struct DFQueryResultWriter<'a, W: std::io::Write> {
    inner: Option<QueryResultWriter<'a, W>>,
}

impl<'a, W: std::io::Write> DFQueryResultWriter<'a, W> {
    pub fn create(inner: QueryResultWriter<'a, W>) -> DFQueryResultWriter<'a, W> {
        DFQueryResultWriter::<'a, W> { inner: Some(inner) }
    }

    pub fn write(&mut self, query_result: Result<Vec<DataBlock>>) -> Result<()> {
        if let Some(writer) = self.inner.take() {
            match query_result {
                Ok(received_data) => Self::ok(received_data, writer)?,
                Err(error) => Self::err(&error, writer)?,
            }
        }
        Ok(())
    }

    fn ok(blocks: Vec<DataBlock>, dataset_writer: QueryResultWriter<'a, W>) -> Result<()> {
        // XXX: num_columns == 0 may is error?
        if blocks.is_empty() || (blocks[0].num_columns() == 0) {
            dataset_writer.completed(0, 0)?;
            return Ok(());
        }

        fn convert_field_type(field: &DataField) -> Result<ColumnType> {
            match field.data_type() {
                DataType::Int8 => Ok(ColumnType::MYSQL_TYPE_LONG),
                DataType::Int16 => Ok(ColumnType::MYSQL_TYPE_LONG),
                DataType::Int32 => Ok(ColumnType::MYSQL_TYPE_LONG),
                DataType::Int64 => Ok(ColumnType::MYSQL_TYPE_LONG),
                DataType::UInt8 => Ok(ColumnType::MYSQL_TYPE_LONG),
                DataType::UInt16 => Ok(ColumnType::MYSQL_TYPE_LONG),
                DataType::UInt32 => Ok(ColumnType::MYSQL_TYPE_LONG),
                DataType::UInt64 => Ok(ColumnType::MYSQL_TYPE_LONG),
                DataType::Float32 => Ok(ColumnType::MYSQL_TYPE_FLOAT),
                DataType::Float64 => Ok(ColumnType::MYSQL_TYPE_FLOAT),
                DataType::String => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                DataType::Boolean => Ok(ColumnType::MYSQL_TYPE_SHORT),
                DataType::Date16 | DataType::Date32 => Ok(ColumnType::MYSQL_TYPE_DATE),
                DataType::DateTime32(_) => Ok(ColumnType::MYSQL_TYPE_DATETIME),
                DataType::Null => Ok(ColumnType::MYSQL_TYPE_NULL),
                _ => Err(ErrorCode::UnImplement(format!(
                    "Unsupported column type:{:?}",
                    field.data_type()
                ))),
            }
        }

        fn make_column_from_field(field: &DataField) -> Result<Column> {
            convert_field_type(field).map(|column_type| Column {
                table: "".to_string(),
                column: field.name().to_string(),
                coltype: column_type,
                colflags: ColumnFlags::empty(),
            })
        }

        fn convert_schema(schema: &DataSchemaRef) -> Result<Vec<Column>> {
            schema.fields().iter().map(make_column_from_field).collect()
        }

        let block = blocks[0].clone();
        let tz: Tz = "UTC".parse().unwrap();
        match convert_schema(block.schema()) {
            Err(error) => Self::err(&error, dataset_writer),
            Ok(columns) => {
                let columns_size = block.num_columns();
                let mut row_writer = dataset_writer.start(&columns)?;

                for block in &blocks {
                    let rows_size = block.column(0).len();
                    for row_index in 0..rows_size {
                        for col_index in 0..columns_size {
                            let val = block.column(col_index).try_get(row_index)?;
                            if val.is_null() {
                                row_writer.write_col(None::<u8>)?;
                                continue;
                            }
                            let data_type = block.schema().fields()[col_index].data_type();
                            match val {
                                DataValue::Float32(Some(v)) => row_writer.write_col(v)?,
                                DataValue::Float64(Some(v)) => row_writer.write_col(v)?,
                                DataValue::Boolean(Some(v)) => row_writer.write_col(v as u8)?,
                                DataValue::Int8(Some(v)) => row_writer.write_col(v)?,
                                DataValue::Int16(Some(v)) => row_writer.write_col(v)?,
                                DataValue::Int32(Some(v)) => row_writer.write_col(v)?,
                                DataValue::Int64(Some(v)) => row_writer.write_col(v)?,
                                DataValue::UInt8(Some(v)) => row_writer.write_col(v)?,
                                DataValue::UInt16(Some(v)) => match data_type {
                                    DataType::Date16 => {
                                        row_writer.write_col(v.to_date(&tz).naive_local())?
                                    }
                                    _ => row_writer.write_col(v)?,
                                },
                                DataValue::UInt32(Some(v)) => match data_type {
                                    DataType::Date32 => {
                                        row_writer.write_col(v.to_date(&tz).naive_local())?
                                    }
                                    DataType::DateTime32 => {
                                        row_writer.write_col(v.to_date_time(&tz).naive_local())?
                                    }
                                    _ => row_writer.write_col(v)?,
                                },
                                DataValue::UInt64(Some(v)) => row_writer.write_col(v)?,
                                DataValue::String(Some(v)) => row_writer.write_col(v)?,
                                v => {
                                    return Err(ErrorCode::BadDataValueType(format!(
                                        "Unsupported column type:{:?}",
                                        v.data_type()
                                    )));
                                }
                            }
                        }
                        row_writer.end_row()?;
                    }
                }

                row_writer.finish()?;

                Ok(())
            }
        }
    }

    fn err(error: &ErrorCode, writer: QueryResultWriter<'a, W>) -> Result<()> {
        if error.code() != ABORT_QUERY && error.code() != ABORT_SESSION {
            log::error!("OnQuery Error: {:?}", error);
            writer.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{}", error).as_bytes())?;
        } else {
            writer.error(
                ErrorKind::ER_ABORTING_CONNECTION,
                format!("{}", error).as_bytes(),
            )?;
        }

        Ok(())
    }
}
