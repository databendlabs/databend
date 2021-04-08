// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::bail;
use anyhow::Result;
use common_arrow::arrow::datatypes::DataType;
use common_arrow::arrow::util::display::array_value_to_string;
use common_datablocks::DataBlock;
use msql_srv::*;

pub struct MysqlStream {
    blocks: Vec<DataBlock>,
}

impl MysqlStream {
    pub fn create(blocks: Vec<DataBlock>) -> Self {
        MysqlStream { blocks }
    }

    pub fn execute<W: std::io::Write>(&self, writer: QueryResultWriter<W>) -> Result<()> {
        if self.blocks.is_empty() {
            writer.completed(0, 0)?;
            return Ok(());
        }

        let block = self.blocks[0].clone();
        let fields = block.schema().fields();
        let mut cols = Vec::with_capacity(fields.len());
        for field in fields {
            cols.push(match field.data_type() {
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64 => Column {
                    table: "".to_string(),
                    column: field.name().to_string(),
                    coltype: ColumnType::MYSQL_TYPE_LONG,
                    colflags: ColumnFlags::empty(),
                },
                DataType::Float32 | DataType::Float64 => Column {
                    table: "".to_string(),
                    column: field.name().to_string(),
                    coltype: ColumnType::MYSQL_TYPE_FLOAT,
                    colflags: ColumnFlags::empty(),
                },
                DataType::Utf8 => Column {
                    table: "".to_string(),
                    column: field.name().to_string(),
                    coltype: ColumnType::MYSQL_TYPE_VARCHAR,
                    colflags: ColumnFlags::empty(),
                },
                DataType::Boolean => Column {
                    table: "".to_string(),
                    column: field.name().to_string(),
                    coltype: ColumnType::MYSQL_TYPE_SHORT,
                    colflags: ColumnFlags::empty(),
                },
                DataType::Date64 => Column {
                    table: "".to_string(),
                    column: field.name().to_string(),
                    coltype: ColumnType::MYSQL_TYPE_TIMESTAMP,
                    colflags: ColumnFlags::empty(),
                },
                _ => bail!("Unsupported column type:{:?}", field.data_type()),
            });
        }

        let cols_num = block.num_columns();
        if cols_num > 0 {
            let mut row_writer = writer.start(&cols)?;

            for block in &self.blocks {
                let rows_num = block.column(0).len();
                for r in 0..rows_num {
                    let mut row = Vec::with_capacity(cols_num);
                    for c in 0..cols_num {
                        let column = block.column(c);
                        row.push(array_value_to_string(column, r)?);
                    }
                    row_writer.write_row(row)?;
                }
            }
            row_writer.finish()?;
        }
        Ok(())
    }
}
