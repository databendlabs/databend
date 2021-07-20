// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use clickhouse_srv::connection::Connection;
use clickhouse_srv::errors::Result as CHResult;
use clickhouse_srv::errors::ServerError;
use clickhouse_srv::types::Block;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
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
        let block = convert_block(block)?;

        match self.conn.write_block(&block).await {
            Ok(_) => Ok(()),
            Err(error) => Err(ErrorCode::UnknownException(format!(
                "Cannot send block {:?}",
                error
            ))),
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

pub fn convert_block(block: DataBlock) -> Result<Block> {
    let mut result = Block::new();
    if block.num_columns() == 0 {
        return Ok(result);
    }

    for column_index in 0..block.num_columns() {
        let column = block.column(column_index).to_array()?;
        let name = block.schema().field(column_index).name();

        result = match column.data_type() {
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
        }
    }
    Ok(result)
}
