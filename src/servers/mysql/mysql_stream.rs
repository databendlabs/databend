// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use msql_srv::*;

use crate::datablocks::DataBlock;
use crate::error::{FuseQueryError, FuseQueryResult};
use arrow::datatypes::DataType;
use arrow::util::display::array_value_to_string;

pub struct MySQLStream {
    blocks: Vec<DataBlock>,
}

impl MySQLStream {
    pub fn create(blocks: Vec<DataBlock>) -> Self {
        MySQLStream { blocks }
    }

    pub fn execute<W: std::io::Write>(&self, writer: QueryResultWriter<W>) -> FuseQueryResult<()> {
        let mut cols = vec![];
        let block = self.blocks[0].clone();
        let fields = block.schema().fields();
        for field in fields {
            cols.push(match field.data_type() {
                DataType::Utf8 => Column {
                    table: "".to_string(),
                    column: field.name().to_string(),
                    coltype: ColumnType::MYSQL_TYPE_VARCHAR,
                    colflags: ColumnFlags::empty(),
                },
                _ => {
                    return Err(FuseQueryError::Unsupported(format!(
                        "Column type:{:?}",
                        field.data_type()
                    )))
                }
            });
        }
        let mut rw = writer.start(&cols).unwrap();
        let column = block.column(0);
        rw.write_col(array_value_to_string(&column, 0)?)?;
        rw.finish()?;
        Ok(())
    }
}
