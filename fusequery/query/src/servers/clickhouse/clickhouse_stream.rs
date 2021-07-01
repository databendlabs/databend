// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use clickhouse_srv::types::Block as ClickHouseBlock;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use futures::stream::Stream;
use futures::StreamExt;

pub struct ClickHouseStream {
    input: SendableDataBlockStream,
    block_index: usize,
    schema: DataSchemaRef,
}

impl ClickHouseStream {
    pub fn create(input: SendableDataBlockStream, schema: DataSchemaRef) -> Self {
        ClickHouseStream {
            input,
            block_index: 0,
            schema,
        }
    }

    pub fn convert_block(&self, block: DataBlock) -> Result<ClickHouseBlock> {
        let mut result = ClickHouseBlock::new();
        if block.num_columns() == 0 {
            return Ok(result);
        }

        for i in 0..block.num_columns() {
            let column = block.column(i).to_array()?;
            let name = block.schema().field(i).name();

            match column.data_type() {
                DataType::Int8 => {
                    result = result.column(name, column.i8()?.collect_values());
                }
                DataType::Int16 => {
                    result = result.column(name, column.i16()?.collect_values());
                }
                DataType::Int32 => {
                    result = result.column(name, column.i32()?.collect_values());
                }
                DataType::Int64 => {
                    result = result.column(name, column.i64()?.collect_values());
                }
                DataType::UInt8 => {
                    result = result.column(name, column.u8()?.collect_values());
                }
                DataType::UInt16 => {
                    result = result.column(name, column.u16()?.collect_values());
                }
                DataType::UInt32 => {
                    result = result.column(name, column.u32()?.collect_values());
                }
                DataType::UInt64 => {
                    result = result.column(name, column.u64()?.collect_values());
                }

                DataType::Float32 => {
                    result = result.column(name, column.f32()?.collect_values());
                }
                DataType::Float64 => {
                    result = result.column(name, column.f64()?.collect_values());
                }

                DataType::Date32 => {
                    result = result.column(name, column.date32()?.collect_values());
                }
                DataType::Date64 => {
                    result = result.column(name, column.date64()?.collect_values());
                }

                DataType::Boolean => {
                    let v: Vec<Option<u8>> = column
                        .bool()?
                        .downcast_iter()
                        .map(|f| f.map(|v| v as u8))
                        .collect();

                    result = result.column(name, v);
                }

                DataType::Utf8 => {
                    result = result.column(name, column.utf8()?.collect_values());
                }

                _ => {
                    return Err(ErrorCode::from(anyhow!(
                        "Unsupported column type:{:?}",
                        column.data_type()
                    )))
                }
            }
        }
        Ok(result)
    }
}

impl Stream for ClickHouseStream {
    type Item = Result<ClickHouseBlock>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Some drivers will skip the first block for it recognizes the first block as schema
        // So we make the first block to be an empty block
        if self.block_index == 0 {
            self.block_index += 1;
            let block = DataBlock::empty_with_schema(self.schema.clone());
            return Poll::Ready(Some(self.convert_block(block)));
        }

        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(v)) => Some(self.convert_block(v)),
            // Some(Err(e)) => Some(Err(e)),
            _other => None,
        })
    }
}
