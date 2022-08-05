// Copyright 2022 Datafuse Labs.
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
use std::sync::Arc;

use chrono::Date;
use chrono::DateTime;
use chrono::Datelike;
use common_base::base::tokio::sync::mpsc::Receiver;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::ColumnRef;
use common_datavalues::DataSchemaRef;
use common_datavalues::ScalarColumn;
use common_datavalues::Series;
use common_datavalues::SeriesFrom;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use opensrv_clickhouse::errors::Error as CHError;
use opensrv_clickhouse::errors::Result as CHResult;
use opensrv_clickhouse::types::Block;
use opensrv_clickhouse::types::SqlType;

use crate::processors::sources::SyncSource;
use crate::processors::sources::SyncSourcer;

pub struct SyncReceiverCkSource {
    schema: DataSchemaRef,
    receiver: Receiver<Block>,
}

impl SyncReceiverCkSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        receiver: Receiver<Block>,
        output_port: Arc<OutputPort>,
        schema: DataSchemaRef,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output_port, SyncReceiverCkSource { schema, receiver })
    }
}

#[async_trait::async_trait]
impl SyncSource for SyncReceiverCkSource {
    const NAME: &'static str = "SyncReceiverCkSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self.receiver.blocking_recv() {
            None => Ok(None),
            Some(block) => Ok(Some(from_clickhouse_block(self.schema.clone(), block)?)),
        }
    }
}

pub fn from_clickhouse_block(schema: DataSchemaRef, block: Block) -> Result<DataBlock> {
    let get_series = |block: &Block, index: usize| -> CHResult<ColumnRef> {
        let col = &block.columns()[index];

        match col.sql_type() {
            SqlType::UInt8 => Ok(UInt8Column::from_iterator(col.iter::<u8>()?.copied()).arc()),
            SqlType::UInt16 => Ok(UInt16Column::from_iterator(col.iter::<u16>()?.copied()).arc()),
            SqlType::Date => Ok(Int32Column::from_iterator(
                col.iter::<Date<_>>()?
                    .map(|v| v.naive_utc().num_days_from_ce()),
            )
            .arc()),
            SqlType::UInt32 => Ok(UInt32Column::from_iterator(col.iter::<u32>()?.copied()).arc()),
            SqlType::DateTime(_) => Ok(Int64Column::from_iterator(
                col.iter::<DateTime<_>>()?.map(|v| v.timestamp_micros()),
            )
            .arc()),
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
            SqlType::Nullable(SqlType::UInt16) => Ok(Series::from_data(
                col.iter::<Option<u16>>()?.map(|c| c.copied()),
            )),
            SqlType::Nullable(SqlType::Date) => {
                Ok(Series::from_data(col.iter::<Option<Date<_>>>()?.map(|c| {
                    c.map(|v| v.naive_utc().num_days_from_ce() as u16)
                })))
            }
            SqlType::Nullable(SqlType::UInt32) => Ok(Series::from_data(
                col.iter::<Option<u32>>()?.map(|c| c.copied()),
            )),
            SqlType::Nullable(SqlType::DateTime(_)) => Ok(Series::from_data(
                col.iter::<Option<DateTime<_>>>()?
                    .map(|c| c.map(|v| v.timestamp_micros())),
            )),
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

pub fn from_clickhouse_err(res: opensrv_clickhouse::errors::Error) -> ErrorCode {
    ErrorCode::LogicalError(format!("clickhouse-srv exception: {:?}", res))
}
