// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow::record_batch::RecordBatch;
use common_arrow::arrow_flight::utils::flight_data_to_arrow_batch;
use common_arrow::arrow_flight::FlightData;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_runtime::tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tonic::Streaming;

#[derive(Debug)]
pub struct FlightDataStream();

impl FlightDataStream {
    #[inline]
    pub fn from_remote(
        schema: DataSchemaRef,
        inner: Streaming<FlightData>,
    ) -> impl Stream<Item = Result<DataBlock, ErrorCode>> {
        inner.map(move |flight_data| -> Result<DataBlock, ErrorCode> {
            match flight_data {
                Err(status) => Err(ErrorCode::UnknownException(status.message())),
                Ok(flight_data) => {
                    fn create_data_block(record_batch: RecordBatch) -> DataBlock {
                        let columns = record_batch
                            .columns()
                            .iter()
                            .map(|column| DataColumn::Array(column.clone().into_series()))
                            .collect::<Vec<_>>();

                        DataBlock::create(
                            Arc::new(DataSchema::from(record_batch.schema())),
                            columns,
                        )
                    }

                    let arrow_schema = Arc::new(schema.to_arrow());
                    Ok(flight_data_to_arrow_batch(&flight_data, arrow_schema, &[])
                        .map(create_data_block)?)
                }
            }
        })
    }

    // It is used in testing, and later it will be used in local stream
    #[inline]
    #[allow(dead_code)]
    pub fn from_receiver(
        schema_ref: DataSchemaRef,
        inner: Receiver<Result<FlightData, ErrorCode>>,
    ) -> impl Stream<Item = Result<DataBlock, ErrorCode>> {
        ReceiverStream::new(inner).map(move |flight_data| match flight_data {
            Err(error_code) => Err(error_code),
            Ok(flight_data) => {
                fn create_data_block(record_batch: RecordBatch) -> DataBlock {
                    let columns = record_batch
                        .columns()
                        .iter()
                        .map(|column| DataColumn::Array(column.clone().into_series()))
                        .collect::<Vec<_>>();

                    let schema = DataSchema::from(record_batch.schema());
                    DataBlock::create(Arc::new(schema), columns)
                }

                Ok(
                    flight_data_to_arrow_batch(&flight_data, Arc::new(schema_ref.to_arrow()), &[])
                        .map(create_data_block)?,
                )
            }
        })
    }
}
