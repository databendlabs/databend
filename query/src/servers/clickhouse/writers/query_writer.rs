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

use std::borrow::Cow;

use common_base::ProgressValues;
use common_clickhouse_srv::connection::Connection;
use common_clickhouse_srv::errors::Error as CHError;
use common_clickhouse_srv::errors::Result as CHResult;
use common_clickhouse_srv::errors::ServerError;
use common_clickhouse_srv::types::column::{self};
use common_clickhouse_srv::types::Block;
use common_clickhouse_srv::types::DateTimeType;
use common_clickhouse_srv::types::SqlType;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;
use futures::channel::mpsc::Receiver;
use futures::StreamExt;

use crate::servers::clickhouse::interactive_worker_base::BlockItem;

pub struct QueryWriter<'a> {
    client_version: u64,
    conn: &'a mut Connection,
}

impl<'a> QueryWriter<'a> {
    pub fn create(version: u64, conn: &'a mut Connection) -> QueryWriter {
        QueryWriter {
            conn,
            client_version: version,
        }
    }

    pub async fn write(&mut self, receiver: Result<Receiver<BlockItem>>) -> Result<()> {
        match receiver {
            Err(error) => self.write_error(error).await,
            Ok(receiver) => {
                let write_data = self.write_data(receiver);
                write_data.await
            }
        }
    }

    async fn write_progress(&mut self, values: ProgressValues) -> Result<()> {
        let progress = common_clickhouse_srv::types::Progress {
            rows: values.read_rows as u64,
            bytes: values.read_bytes as u64,
            total_rows: 0,
        };

        let version = self.client_version;
        match self.conn.write_progress(progress, version).await {
            Ok(_) => Ok(()),
            Err(error) => Err(ErrorCode::UnknownException(format!(
                "Cannot send progress {:?}",
                error
            ))),
        }
    }

    async fn write_error(&mut self, error: ErrorCode) -> Result<()> {
        tracing::error!("OnQuery Error: {:?}", error);
        let clickhouse_err = to_clickhouse_err(error);
        match self.conn.write_error(&clickhouse_err).await {
            Ok(_) => Ok(()),
            Err(error) => Err(ErrorCode::UnknownException(format!(
                "Cannot send error {:?}",
                error
            ))),
        }
    }

    async fn write_block(&mut self, block: DataBlock) -> Result<()> {
        let block = to_clickhouse_block(block)?;

        match self.conn.write_block(&block).await {
            Ok(_) => Ok(()),
            Err(error) => Err(ErrorCode::UnknownException(format!("{:?}", error))),
        }
    }

    async fn write_data(&mut self, mut receiver: Receiver<BlockItem>) -> Result<()> {
        loop {
            match receiver.next().await {
                None => {
                    return Ok(());
                }
                Some(BlockItem::ProgressTicker(values)) => self.write_progress(values).await?,
                Some(BlockItem::Block(Err(error))) => {
                    self.write_error(error).await?;
                    return Ok(());
                }
                Some(BlockItem::Block(Ok(block))) => {
                    self.write_block(block).await?;
                }
                Some(BlockItem::InsertSample(block)) => {
                    let schema = block.schema();
                    let header = DataBlock::empty_with_schema(schema.clone());

                    self.write_block(header).await?;
                }
            }
        }
    }
}

pub fn to_clickhouse_err(res: ErrorCode) -> common_clickhouse_srv::errors::Error {
    common_clickhouse_srv::errors::Error::Server(ServerError {
        code: res.code() as u32,
        name: "DB:Exception".to_string(),
        message: res.message(),
        stack_trace: res.backtrace_str(),
    })
}

pub fn from_clickhouse_err(res: common_clickhouse_srv::errors::Error) -> ErrorCode {
    ErrorCode::LogicalError(format!("clickhouse-srv expception: {:?}", res))
}

pub fn to_clickhouse_block(block: DataBlock) -> Result<Block> {
    let mut result = Block::new();
    if block.num_columns() == 0 {
        return Ok(result);
    }

    for column_index in 0..block.num_columns() {
        let column = block.column(column_index);
        let field = block.schema().field(column_index);
        let name = field.name();
        let serializer = field.data_type().create_serializer();
        result.append_column(column::new_column(
            name,
            serializer.serialize_clickhouse_format(column)?,
        ));
    }
    Ok(result)
}

pub fn from_clickhouse_block(schema: DataSchemaRef, block: Block) -> Result<DataBlock> {
    let get_series = |block: &Block, index: usize| -> CHResult<ColumnRef> {
        let col = &block.columns()[index];
        match col.sql_type() {
            SqlType::UInt8 => Ok(UInt8Column::from_iterator(col.iter::<u8>()?.copied()).arc()),
            SqlType::UInt16 | SqlType::Date => {
                Ok(UInt16Column::from_iterator(col.iter::<u16>()?.copied()).arc())
            }
            SqlType::UInt32 | SqlType::DateTime(DateTimeType::DateTime32) => {
                Ok(UInt32Column::from_iterator(col.iter::<u32>()?.copied()).arc())
            }
            SqlType::UInt64 => Ok(UInt64Column::from_iterator(col.iter::<u64>()?.copied()).arc()),
            SqlType::Int8 => Ok(Int8Column::from_iterator(col.iter::<i8>()?.copied()).arc()),
            SqlType::Int16 => Ok(Int16Column::from_iterator(col.iter::<i16>()?.copied()).arc()),
            SqlType::Int32 => Ok(Int32Column::from_iterator(col.iter::<i32>()?.copied()).arc()),
            SqlType::Int64 => Ok(Int64Column::from_iterator(col.iter::<i64>()?.copied()).arc()),
            SqlType::Float32 => Ok(Float32Column::from_iterator(col.iter::<f32>()?.copied()).arc()),
            SqlType::Float64 => Ok(Float64Column::from_iterator(col.iter::<f64>()?.copied()).arc()),
            SqlType::String => Ok(StringColumn::from_iterator(col.iter::<&[u8]>()?).arc()),
            SqlType::FixedString(_) => Ok(StringColumn::from_iterator(col.iter::<&[u8]>()?).arc()),

            SqlType::Nullable(SqlType::UInt8) => Ok(Series::from_data(
                col.iter::<Option<u8>>()?.map(|c| c.copied()),
            )),
            SqlType::Nullable(SqlType::UInt16) | SqlType::Nullable(SqlType::Date) => Ok(
                Series::from_data(col.iter::<Option<u16>>()?.map(|c| c.copied())),
            ),
            SqlType::Nullable(SqlType::UInt32)
            | SqlType::Nullable(SqlType::DateTime(DateTimeType::DateTime32)) => Ok(
                Series::from_data(col.iter::<Option<u32>>()?.map(|c| c.copied())),
            ),
            SqlType::Nullable(SqlType::UInt64) => Ok(Series::from_data(
                col.iter::<Option<u64>>()?.map(|c| c.copied()),
            )),
            SqlType::Nullable(SqlType::Int8) => Ok(Series::from_data(
                col.iter::<Option<i8>>()?.map(|c| c.copied()),
            )),
            SqlType::Nullable(SqlType::Int16) => Ok(Series::from_data(
                col.iter::<Option<i16>>()?.map(|c| c.copied()),
            )),
            SqlType::Nullable(SqlType::Int32) => Ok(Series::from_data(
                col.iter::<Option<i32>>()?.map(|c| c.copied()),
            )),
            SqlType::Nullable(SqlType::Int64) => Ok(Series::from_data(
                col.iter::<Option<i64>>()?.map(|c| c.copied()),
            )),
            SqlType::Nullable(SqlType::Float32) => Ok(Series::from_data(
                col.iter::<Option<f32>>()?.map(|c| c.copied()),
            )),
            SqlType::Nullable(SqlType::Float64) => Ok(Series::from_data(
                col.iter::<Option<f64>>()?.map(|c| c.copied()),
            )),
            SqlType::Nullable(SqlType::String) => {
                Ok(Series::from_data(col.iter::<Option<&[u8]>>()?))
            }
            SqlType::Nullable(SqlType::FixedString(_)) => {
                Ok(Series::from_data(col.iter::<Option<&[u8]>>()?))
            }

            other => Err(CHError::Other(Cow::from(format!(
                "Unsupported type: {:?}",
                other
            )))),
        }
    };

    let mut columns = vec![];
    for index in 0..block.column_count() {
        let array = get_series(&block, index);
        let a2 = array.map_err(from_clickhouse_err);
        columns.push(a2?);
    }
    Ok(DataBlock::create(schema, columns))
}
