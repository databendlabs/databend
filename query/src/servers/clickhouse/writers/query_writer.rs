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

use chrono::Date;
use chrono::DateTime;
use chrono_tz::Tz;
use common_base::ProgressValues;
use common_clickhouse_srv::connection::Connection;
use common_clickhouse_srv::errors::Error as CHError;
use common_clickhouse_srv::errors::Result as CHResult;
use common_clickhouse_srv::errors::ServerError;
use common_clickhouse_srv::types::column::ArcColumnData;
use common_clickhouse_srv::types::column::ArcColumnWrapper;
use common_clickhouse_srv::types::column::ColumnFrom;
use common_clickhouse_srv::types::column::{self};
use common_clickhouse_srv::types::Block;
use common_clickhouse_srv::types::DateTimeType;
use common_clickhouse_srv::types::SqlType;
use common_datablocks::DataBlock;
use common_datavalues2::prelude::*;
use common_datavalues2::with_match_primitive_type_id;
use common_datavalues2::with_match_scalar_type;
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
            Err(error) => Err(ErrorCode::UnknownException(format!("{}", error))),
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
    todo!()
}
