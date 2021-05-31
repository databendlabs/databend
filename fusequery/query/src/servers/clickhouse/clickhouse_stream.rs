// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use clickhouse_srv::types::Block as ClickHouseBlock;
use common_arrow::arrow::array::*;
use common_arrow::arrow::datatypes::*;
use common_datablocks::DataBlock;
use common_datavalues::DataArrayRef;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use futures::stream::Stream;
use futures::StreamExt;

pub struct ClickHouseStream {
    input: SendableDataBlockStream,
    block_index: usize,
    schema: DataSchemaRef
}

impl ClickHouseStream {
    pub fn create(input: SendableDataBlockStream, schema: DataSchemaRef) -> Self {
        ClickHouseStream {
            input,
            block_index: 0,
            schema
        }
    }

    pub fn convert_block(&self, block: DataBlock) -> Result<ClickHouseBlock> {
        let mut result = ClickHouseBlock::new();
        if block.is_empty() && block.num_columns() == 0 {
            return Ok(result);
        }

        for i in 0..block.num_columns() {
            let column = block.column(i).to_array()?;
            let name = block.schema().field(i).name();

            match column.data_type() {
                DataType::Int8 => {
                    let data = build_primitive_column::<Int8Type>(&column)?;
                    result = result.column(name, data);
                }
                DataType::Int16 => {
                    let data = build_primitive_column::<Int16Type>(&column)?;
                    result = result.column(name, data);
                }
                DataType::Int32 => {
                    let data = build_primitive_column::<Int32Type>(&column)?;
                    result = result.column(name, data);
                }
                DataType::Int64 => {
                    let data = build_primitive_column::<Int64Type>(&column)?;
                    result = result.column(name, data);
                }
                DataType::UInt8 => {
                    let data = build_primitive_column::<UInt8Type>(&column)?;
                    result = result.column(name, data);
                }
                DataType::UInt16 => {
                    let data = build_primitive_column::<UInt16Type>(&column)?;
                    result = result.column(name, data);
                }
                DataType::UInt32 => {
                    let data = build_primitive_column::<UInt32Type>(&column)?;
                    result = result.column(name, data);
                }
                DataType::UInt64 => {
                    let data = build_primitive_column::<UInt64Type>(&column)?;
                    result = result.column(name, data);
                }

                DataType::Float32 => {
                    let data = build_primitive_column::<Float32Type>(&column)?;
                    result = result.column(name, data);
                }
                DataType::Float64 => {
                    let data = build_primitive_column::<Float64Type>(&column)?;
                    result = result.column(name, data);
                }

                DataType::Date32 => {
                    let data = build_primitive_column::<Date32Type>(&column)?;
                    result = result.column(name, data);
                }
                DataType::Date64 => {
                    let data = build_primitive_column::<Date64Type>(&column)?;
                    result = result.column(name, data);
                }

                DataType::Boolean => {
                    let data = build_boolean_column(&column)?;
                    result = result.column(name, data);
                }

                DataType::Utf8 => {
                    let data = build_string_column(&column)?;
                    result = result.column(name, data);
                }

                _ => {
                    return Err(ErrorCodes::from(anyhow!(
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
        // some driver will skip the first block for it recognize the first block as schema
        // so we make the first block to be empty block
        if self.block_index == 0 {
            self.block_index += 1;
            let block = DataBlock::empty_with_schema(self.schema.clone());
            return Poll::Ready(Some(self.convert_block(block)));
        }

        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(v)) => Some(self.convert_block(v)),
            // Some(Err(e)) => Some(Err(e)),
            _other => None
        })
    }
}

fn build_primitive_column<T>(values: &DataArrayRef) -> Result<Vec<Option<T::Native>>>
where T: ArrowPrimitiveType {
    let values = as_primitive_array::<T>(values);

    Ok(match values.null_count() {
        //faster path
        0 => (0..values.len())
            .map(|i| Some(values.value(i)))
            .collect::<Vec<Option<T::Native>>>(),
        _ => (0..values.len())
            .map(|i| {
                if values.is_null(i) {
                    None
                } else {
                    Some(values.value(i))
                }
            })
            .collect::<Vec<Option<T::Native>>>()
    })
}

fn build_boolean_column(values: &DataArrayRef) -> Result<Vec<Option<u8>>> {
    let values = as_boolean_array(values);

    Ok(match values.null_count() {
        //faster path
        0 => (0..values.len())
            .map(|i| Some(values.value(i) as u8))
            .collect::<Vec<Option<u8>>>(),
        _ => (0..values.len())
            .map(|i| {
                if values.is_null(i) {
                    None
                } else {
                    Some(values.value(i) as u8)
                }
            })
            .collect::<Vec<Option<u8>>>()
    })
}

fn build_string_column(values: &DataArrayRef) -> Result<Vec<Option<&str>>> {
    let values = as_string_array(values);
    Ok(match values.null_count() {
        //faster path
        0 => (0..values.len())
            .map(|i| Some(values.value(i)))
            .collect::<Vec<Option<&str>>>(),
        _ => (0..values.len())
            .map(|i| {
                if values.is_null(i) {
                    None
                } else {
                    Some(values.value(i))
                }
            })
            .collect::<Vec<Option<&str>>>()
    })
}
