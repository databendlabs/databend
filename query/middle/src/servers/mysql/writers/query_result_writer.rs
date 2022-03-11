// Copyright 2021 Datafuse Labs.
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
use common_datavalues::prelude::TypeID;
use common_datavalues::remove_nullable;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_datavalues::DateConverter;
use common_datavalues::DateTime32Type;
use common_datavalues::DateTime64Type;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ABORT_QUERY;
use common_exception::ABORT_SESSION;
use common_tracing::tracing;
use msql_srv::*;

pub struct DFQueryResultWriter<'a, W: std::io::Write> {
    inner: Option<QueryResultWriter<'a, W>>,
}

impl<'a, W: std::io::Write> DFQueryResultWriter<'a, W> {
    pub fn create(inner: QueryResultWriter<'a, W>) -> DFQueryResultWriter<'a, W> {
        DFQueryResultWriter::<'a, W> { inner: Some(inner) }
    }

    pub fn write(&mut self, query_result: Result<(Vec<DataBlock>, String)>) -> Result<()> {
        if let Some(writer) = self.inner.take() {
            match query_result {
                Ok((blocks, extra_info)) => Self::ok(blocks, extra_info, writer)?,
                Err(error) => Self::err(&error, writer)?,
            }
        }
        Ok(())
    }

    fn ok(
        blocks: Vec<DataBlock>,
        extra_info: String,
        dataset_writer: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        // XXX: num_columns == 0 may is error?
        let default_response = OkResponse {
            info: extra_info,
            ..Default::default()
        };

        if blocks.is_empty() || (blocks[0].num_columns() == 0) {
            dataset_writer.completed(default_response)?;
            return Ok(());
        }

        fn convert_field_type(field: &DataField) -> Result<ColumnType> {
            match remove_nullable(field.data_type()).data_type_id() {
                TypeID::Int8 => Ok(ColumnType::MYSQL_TYPE_LONG),
                TypeID::Int16 => Ok(ColumnType::MYSQL_TYPE_LONG),
                TypeID::Int32 => Ok(ColumnType::MYSQL_TYPE_LONG),
                TypeID::Int64 => Ok(ColumnType::MYSQL_TYPE_LONG),
                TypeID::UInt8 => Ok(ColumnType::MYSQL_TYPE_LONG),
                TypeID::UInt16 => Ok(ColumnType::MYSQL_TYPE_LONG),
                TypeID::UInt32 => Ok(ColumnType::MYSQL_TYPE_LONG),
                TypeID::UInt64 => Ok(ColumnType::MYSQL_TYPE_LONG),
                TypeID::Float32 => Ok(ColumnType::MYSQL_TYPE_FLOAT),
                TypeID::Float64 => Ok(ColumnType::MYSQL_TYPE_FLOAT),
                TypeID::String => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                TypeID::Boolean => Ok(ColumnType::MYSQL_TYPE_SHORT),
                TypeID::Date16 | TypeID::Date32 => Ok(ColumnType::MYSQL_TYPE_DATE),
                TypeID::DateTime32 => Ok(ColumnType::MYSQL_TYPE_DATETIME),
                TypeID::DateTime64 => Ok(ColumnType::MYSQL_TYPE_DATETIME),
                TypeID::Null => Ok(ColumnType::MYSQL_TYPE_NULL),
                TypeID::Interval => Ok(ColumnType::MYSQL_TYPE_LONG),
                TypeID::Struct => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
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
        let utc: Tz = "UTC".parse().unwrap();
        match convert_schema(block.schema()) {
            Err(error) => Self::err(&error, dataset_writer),
            Ok(columns) => {
                let columns_size = block.num_columns();
                let mut row_writer = dataset_writer.start(&columns)?;

                for block in &blocks {
                    let rows_size = block.column(0).len();
                    for row_index in 0..rows_size {
                        for col_index in 0..columns_size {
                            let val = block.column(col_index).get_checked(row_index)?;
                            if val.is_null() {
                                row_writer.write_col(None::<u8>)?;
                                continue;
                            }
                            let data_type =
                                remove_nullable(block.schema().fields()[col_index].data_type());

                            match (data_type.data_type_id(), val.clone()) {
                                (TypeID::Boolean, DataValue::Boolean(v)) => {
                                    row_writer.write_col(v as i8)?
                                }
                                (TypeID::Date16, DataValue::UInt64(v)) => {
                                    row_writer.write_col(v.to_date(&utc).naive_local())?
                                }
                                (TypeID::Date32, DataValue::Int64(v)) => {
                                    row_writer.write_col(v.to_date(&utc).naive_local())?
                                }
                                (TypeID::DateTime32, DataValue::UInt64(v)) => {
                                    let data_type: &DateTime32Type =
                                        data_type.as_any().downcast_ref().unwrap();
                                    let tz = data_type.tz();
                                    let tz = tz.cloned().unwrap_or_else(|| "UTC".to_string());
                                    let tz: Tz = tz.parse().unwrap();
                                    row_writer.write_col(v.to_date_time(&tz).naive_local())?
                                }
                                (TypeID::DateTime64, DataValue::Int64(v)) => {
                                    let data_type: &DateTime64Type =
                                        data_type.as_any().downcast_ref().unwrap();
                                    let tz = data_type.tz();
                                    let tz = tz.cloned().unwrap_or_else(|| "UTC".to_string());
                                    let tz: Tz = tz.parse().unwrap();

                                    row_writer.write_col(
                                        v.to_date_time64(data_type.precision(), &tz)
                                            .naive_local()
                                            .format(data_type.format_string().as_str())
                                            .to_string(),
                                    )?
                                }
                                (TypeID::String, DataValue::String(v)) => {
                                    row_writer.write_col(v)?
                                }
                                (TypeID::Struct, DataValue::Struct(_)) => {
                                    let serializer = data_type.create_serializer();
                                    row_writer.write_col(serializer.serialize_value(&val)?)?
                                }
                                (_, DataValue::Int64(v)) => row_writer.write_col(v)?,

                                (_, DataValue::UInt64(v)) => row_writer.write_col(v)?,

                                (_, DataValue::Float64(v)) => row_writer.write_col(v)?,
                                (_, v) => {
                                    return Err(ErrorCode::BadDataValueType(format!(
                                        "Unsupported column type:{:?}, expected type in schema: {:?}",
                                        v.data_type(), data_type
                                    )));
                                }
                            }
                        }
                        row_writer.end_row()?;
                    }
                }
                row_writer.finish_with_info(&default_response.info)?;

                Ok(())
            }
        }
    }

    fn err(error: &ErrorCode, writer: QueryResultWriter<'a, W>) -> Result<()> {
        if error.code() != ABORT_QUERY && error.code() != ABORT_SESSION {
            tracing::error!("OnQuery Error: {:?}", error);
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
