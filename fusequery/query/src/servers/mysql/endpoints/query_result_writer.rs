use msql_srv::*;
use common_datablocks::DataBlock;
use common_exception::{Result, ErrorCode};
use common_datavalues::{DataField, DataSchemaRef};
use common_arrow::arrow::datatypes::DataType;
use common_arrow::arrow::util::display::array_value_to_string;

pub struct DFQueryResultWriter<'a, W: std::io::Write> {
    inner: Option<QueryResultWriter<'a, W>>,
}

impl<'a, W: std::io::Write> DFQueryResultWriter<'a, W> {
    pub fn create(inner: QueryResultWriter<'a, W>) -> DFQueryResultWriter<'a, W> {
        DFQueryResultWriter::<'a, W> {
            inner: Some(inner)
        }
    }

    pub fn write(&mut self, query_result: Result<Vec<DataBlock>>) -> Result<()> {
        if let Some(mut writer) = self.inner.take() {
            match query_result {
                Ok(received_data) => Self::ok(received_data, writer)?,
                Err(error) => Self::err(&error, writer)?
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
                DataType::Utf8 => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                DataType::Boolean => Ok(ColumnType::MYSQL_TYPE_SHORT),
                DataType::Date32 => Ok(ColumnType::MYSQL_TYPE_TIMESTAMP),
                DataType::Date64 => Ok(ColumnType::MYSQL_TYPE_TIMESTAMP),
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
        match convert_schema(block.schema()) {
            Err(error) => Self::err(&error, dataset_writer),
            Ok(columns) => {
                let columns_size = block.num_columns();
                let mut row_writer = dataset_writer.start(&columns)?;

                for block in &blocks {
                    let rows_size = block.column(0).len();
                    for row_index in 0..rows_size {
                        let mut row = Vec::with_capacity(columns_size);
                        for column_index in 0..columns_size {
                            let column = block.column(column_index).to_array()?;
                            row.push(array_value_to_string(&column, row_index)?);
                        }
                        row_writer.write_row(row)?;
                    }
                }

                row_writer.finish()?;

                Ok(())
            }
        }
    }

    fn err(error: &ErrorCode, writer: QueryResultWriter<'a, W>) -> Result<()> {
        log::error!("OnQuery Error: {:?}", error);
        let aborted_code = ErrorCode::AbortedSession("").code();

        if error.code() == aborted_code {
            writer.error(ErrorKind::ER_ABORTING_CONNECTION, format!("{}", error).as_bytes())?;
            Err(ErrorCode::AbortedSession("Aborting this connection. because we are try aborting server."))
        } else {
            writer.error(ErrorKind::ER_ABORTING_CONNECTION, format!("{}", error).as_bytes())?;
            Ok(())
        }
    }
}

