// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::borrow::Cow;

use clickhouse_srv::connection::Connection;
use clickhouse_srv::errors::Error as CHError;
use clickhouse_srv::errors::Result as CHResult;
use clickhouse_srv::errors::ServerError;
use clickhouse_srv::types::Block;
use clickhouse_srv::types::SqlType;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::DFUInt8Array;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::channel::mpsc::Receiver;
use futures::StreamExt;

use crate::servers::clickhouse::interactive_worker_base::BlockItem;
use crate::sessions::FuseQueryContextRef;

pub struct QueryWriter<'a> {
    client_version: u64,
    conn: &'a mut Connection,
    ctx: FuseQueryContextRef,
}

impl<'a> QueryWriter<'a> {
    pub fn create(version: u64, conn: &'a mut Connection, ctx: FuseQueryContextRef) -> QueryWriter {
        QueryWriter {
            client_version: version,
            conn,
            ctx,
        }
    }

    pub async fn write(&mut self, receiver: Result<Receiver<BlockItem>>) -> CHResult<()> {
        match receiver {
            Err(error) => self.write_error(error).await.map_err(to_clickhouse_err),
            Ok(receiver) => {
                let write_data = self.write_data(receiver);
                write_data.await.map_err(to_clickhouse_err)
            }
        }
    }

    async fn write_progress(&mut self) -> Result<()> {
        let values = self.ctx.get_and_reset_progress_value();
        let progress = clickhouse_srv::types::Progress {
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
            Err(error) => Err(ErrorCode::UnknownException(format!("{}", error))),
        }
    }

    async fn write_data(&mut self, mut receiver: Receiver<BlockItem>) -> Result<()> {
        loop {
            match receiver.next().await {
                None => {
                    return Ok(());
                }
                Some(BlockItem::ProgressTicker) => self.write_progress().await?,
                Some(BlockItem::Block(Err(error))) => {
                    self.write_error(error).await?;
                    return Ok(());
                }
                Some(BlockItem::Block(Ok(block))) => {
                    // Send header to client
                    let schema = block.schema();
                    let header = DataBlock::empty_with_schema(schema.clone());

                    self.write_block(header).await?;
                    self.write_block(block).await?;
                    return self.write_tail_data(receiver).await;
                }
                Some(BlockItem::InsertSample(block)) => {
                    let schema = block.schema();
                    let header = DataBlock::empty_with_schema(schema.clone());

                    self.write_block(header).await?;
                }
            }
        }
    }

    async fn write_tail_data(&mut self, mut receiver: Receiver<BlockItem>) -> Result<()> {
        while let Some(item) = receiver.next().await {
            match item {
                BlockItem::ProgressTicker => self.write_progress().await?,
                BlockItem::Block(Ok(block)) => self.write_block(block).await?,
                BlockItem::Block(Err(error)) => self.write_error(error).await?,
                BlockItem::InsertSample(block) => self.write_block(block).await?,
            };
        }

        Ok(())
    }
}

pub fn to_clickhouse_err(res: ErrorCode) -> clickhouse_srv::errors::Error {
    clickhouse_srv::errors::Error::Server(ServerError {
        code: res.code() as u32,
        name: "DB:Exception".to_string(),
        message: res.message(),
        stack_trace: res.backtrace_str(),
    })
}

pub fn from_clickhouse_err(res: clickhouse_srv::errors::Error) -> ErrorCode {
    ErrorCode::LogicalError(format!("clickhouse-srv expception: {:?}", res))
}

pub fn to_clickhouse_block(block: DataBlock) -> Result<Block> {
    let mut result = Block::new();
    if block.num_columns() == 0 {
        return Ok(result);
    }

    for column_index in 0..block.num_columns() {
        let column = block.column(column_index).to_array()?;
        let field = block.schema().field(column_index);
        let name = field.name();
        let is_nullable = field.is_nullable();

        result = match is_nullable {
            true => match column.data_type() {
                DataType::Int8 => result.column(name, column.i8()?.collect_values()),
                DataType::Int16 => result.column(name, column.i16()?.collect_values()),
                DataType::Int32 => result.column(name, column.i32()?.collect_values()),
                DataType::Int64 => result.column(name, column.i64()?.collect_values()),
                DataType::UInt8 => result.column(name, column.u8()?.collect_values()),
                DataType::UInt16 => result.column(name, column.u16()?.collect_values()),
                DataType::UInt32 => result.column(name, column.u32()?.collect_values()),
                DataType::UInt64 => result.column(name, column.u64()?.collect_values()),
                DataType::Float32 => result.column(name, column.f32()?.collect_values()),
                DataType::Float64 => result.column(name, column.f64()?.collect_values()),
                DataType::Date32 => result.column(name, column.date32()?.collect_values()),
                DataType::Date64 => result.column(name, column.date64()?.collect_values()),
                DataType::Utf8 => result.column(name, column.utf8()?.collect_values()),
                DataType::Boolean => {
                    let v: Vec<Option<u8>> = column
                        .bool()?
                        .downcast_iter()
                        .map(|f| f.map(|v| v as u8))
                        .collect();

                    result.column(name, v)
                }
                _ => {
                    return Err(ErrorCode::BadDataValueType(format!(
                        "Unsupported column type:{:?}",
                        column.data_type()
                    )));
                }
            },
            false => match column.data_type() {
                DataType::Int8 => {
                    result.column(name, column.i8()?.downcast_ref().values().to_owned())
                }
                DataType::Int16 => {
                    result.column(name, column.i16()?.downcast_ref().values().to_owned())
                }
                DataType::Int32 => {
                    result.column(name, column.i32()?.downcast_ref().values().to_owned())
                }
                DataType::Int64 => {
                    result.column(name, column.i64()?.downcast_ref().values().to_owned())
                }
                DataType::UInt8 => {
                    result.column(name, column.u8()?.downcast_ref().values().to_owned())
                }
                DataType::UInt16 => {
                    result.column(name, column.u16()?.downcast_ref().values().to_owned())
                }
                DataType::UInt32 => {
                    result.column(name, column.u32()?.downcast_ref().values().to_owned())
                }
                DataType::UInt64 => {
                    result.column(name, column.u64()?.downcast_ref().values().to_owned())
                }
                DataType::Float32 => {
                    result.column(name, column.f32()?.downcast_ref().values().to_owned())
                }
                DataType::Float64 => {
                    result.column(name, column.f64()?.downcast_ref().values().to_owned())
                }
                DataType::Date32 => {
                    result.column(name, column.date32()?.downcast_ref().values().to_owned())
                }
                DataType::Date64 => {
                    result.column(name, column.date64()?.downcast_ref().values().to_owned())
                }
                DataType::Utf8 => {
                    let vs: Vec<&str> =
                        column.utf8()?.downcast_iter().map(|c| c.unwrap()).collect();
                    result.column(name, vs)
                }
                DataType::Boolean => {
                    let vs: Vec<u8> = column
                        .bool()?
                        .downcast_iter()
                        .map(|c| c.unwrap() as u8)
                        .collect();
                    result.column(name, vs)
                }
                _ => {
                    return Err(ErrorCode::BadDataValueType(format!(
                        "Unsupported column type:{:?}",
                        column.data_type()
                    )));
                }
            },
        }
    }
    Ok(result)
}

pub fn from_clickhouse_block(schema: DataSchemaRef, block: Block) -> Result<DataBlock> {
    let get_series = |block: &Block, index: usize| -> CHResult<Series> {
        let col = &block.columns()[index];
        match col.sql_type() {
            SqlType::UInt8 => {
                Ok(DFUInt8Array::new_from_iter(col.iter::<u8>()?.copied()).into_series())
            }
            SqlType::UInt16 => {
                Ok(DFUInt16Array::new_from_iter(col.iter::<u16>()?.copied()).into_series())
            }
            SqlType::UInt32 => {
                Ok(DFUInt32Array::new_from_iter(col.iter::<u32>()?.copied()).into_series())
            }
            SqlType::UInt64 => {
                Ok(DFUInt64Array::new_from_iter(col.iter::<u64>()?.copied()).into_series())
            }
            SqlType::Int8 => {
                Ok(DFInt8Array::new_from_iter(col.iter::<i8>()?.copied()).into_series())
            }
            SqlType::Int16 => {
                Ok(DFInt16Array::new_from_iter(col.iter::<i16>()?.copied()).into_series())
            }
            SqlType::Int32 => {
                Ok(DFInt32Array::new_from_iter(col.iter::<i32>()?.copied()).into_series())
            }
            SqlType::Int64 => {
                Ok(DFInt64Array::new_from_iter(col.iter::<i64>()?.copied()).into_series())
            }
            SqlType::Float32 => {
                Ok(DFFloat32Array::new_from_iter(col.iter::<f32>()?.copied()).into_series())
            }
            SqlType::Float64 => {
                Ok(DFFloat64Array::new_from_iter(col.iter::<f64>()?.copied()).into_series())
            }
            SqlType::String => Ok(DFUtf8Array::new_from_iter(
                col.iter::<&[u8]>()?.map(|c| String::from_utf8_lossy(c)),
            )
            .into_series()),
            SqlType::FixedString(_) => Ok(DFUtf8Array::new_from_iter(
                col.iter::<&[u8]>()?.map(|c| String::from_utf8_lossy(c)),
            )
            .into_series()),

            SqlType::Nullable(SqlType::UInt8) => Ok(DFUInt8Array::new_from_opt_iter(
                col.iter::<Option<u8>>()?.map(|c| c.copied()),
            )
            .into_series()),
            SqlType::Nullable(SqlType::UInt16) => Ok(DFUInt16Array::new_from_opt_iter(
                col.iter::<Option<u16>>()?.map(|c| c.copied()),
            )
            .into_series()),
            SqlType::Nullable(SqlType::UInt32) => Ok(DFUInt32Array::new_from_opt_iter(
                col.iter::<Option<u32>>()?.map(|c| c.copied()),
            )
            .into_series()),
            SqlType::Nullable(SqlType::UInt64) => Ok(DFUInt64Array::new_from_opt_iter(
                col.iter::<Option<u64>>()?.map(|c| c.copied()),
            )
            .into_series()),
            SqlType::Nullable(SqlType::Int8) => Ok(DFInt8Array::new_from_opt_iter(
                col.iter::<Option<i8>>()?.map(|c| c.copied()),
            )
            .into_series()),
            SqlType::Nullable(SqlType::Int16) => Ok(DFInt16Array::new_from_opt_iter(
                col.iter::<Option<i16>>()?.map(|c| c.copied()),
            )
            .into_series()),
            SqlType::Nullable(SqlType::Int32) => Ok(DFInt32Array::new_from_opt_iter(
                col.iter::<Option<i32>>()?.map(|c| c.copied()),
            )
            .into_series()),
            SqlType::Nullable(SqlType::Int64) => Ok(DFInt64Array::new_from_opt_iter(
                col.iter::<Option<i64>>()?.map(|c| c.copied()),
            )
            .into_series()),
            SqlType::Nullable(SqlType::Float32) => Ok(DFFloat32Array::new_from_opt_iter(
                col.iter::<Option<f32>>()?.map(|c| c.copied()),
            )
            .into_series()),
            SqlType::Nullable(SqlType::Float64) => Ok(DFFloat64Array::new_from_opt_iter(
                col.iter::<Option<f64>>()?.map(|c| c.copied()),
            )
            .into_series()),
            SqlType::Nullable(SqlType::String) => Ok(DFUtf8Array::new_from_opt_iter(
                col.iter::<Option<&[u8]>>()?
                    .map(|c| c.map(|d| String::from_utf8_lossy(d))),
            )
            .into_series()),
            SqlType::Nullable(SqlType::FixedString(_)) => Ok(DFUtf8Array::new_from_opt_iter(
                col.iter::<Option<&[u8]>>()?
                    .map(|c| c.map(|d| String::from_utf8_lossy(d))),
            )
            .into_series()),

            other => Err(CHError::Other(Cow::from(format!(
                "Unsupported type: {:?}",
                other
            )))),
        }
    };

    let mut arrays = vec![];
    for index in 0..block.column_count() {
        let array = get_series(&block, index);
        let a2 = array.map_err(from_clickhouse_err);
        arrays.push(a2?);
    }
    Ok(DataBlock::create_by_array(schema, arrays))
}
