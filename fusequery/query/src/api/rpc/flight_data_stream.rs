// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::datatypes::SchemaRef;
use common_arrow::arrow::record_batch::RecordBatch;
use common_arrow::arrow_flight::utils::flight_data_to_arrow_batch;
use common_arrow::arrow_flight::FlightData;
use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_exception::ErrorCodes;
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tonic::Streaming;

#[derive(Debug)]
pub struct FlightDataStream();

impl FlightDataStream {
    #[inline]
    pub fn from_remote(
        schema: SchemaRef,
        inner: Streaming<FlightData>
    ) -> impl Stream<Item = Result<DataBlock, ErrorCodes>> {
        inner.map(move |flight_data| -> Result<DataBlock, ErrorCodes> {
            match flight_data {
                Err(status) => Err(ErrorCodes::UnknownException(status.message())),
                Ok(flight_data) => {
                    fn create_data_block(record_batch: RecordBatch) -> DataBlock {
                        let columns = record_batch
                            .columns()
                            .iter()
                            .map(|column| DataColumnarValue::Array(column.clone()))
                            .collect::<Vec<_>>();

                        DataBlock::create(record_batch.schema(), columns)
                    }

                    Ok(
                        flight_data_to_arrow_batch(&flight_data, schema.clone(), &[])
                            .map(create_data_block)?
                    )
                }
            }
        })
    }

    #[inline]
    pub fn from_receiver(
        schema: SchemaRef,
        inner: Receiver<Result<FlightData, ErrorCodes>>
    ) -> impl Stream<Item = Result<DataBlock, ErrorCodes>> {
        ReceiverStream::new(inner).map(move |flight_data| match flight_data {
            Err(error_code) => Err(error_code),
            Ok(flight_data) => {
                fn create_data_block(record_batch: RecordBatch) -> DataBlock {
                    let columns = record_batch
                        .columns()
                        .iter()
                        .map(|column| DataColumnarValue::Array(column.clone()))
                        .collect::<Vec<_>>();

                    DataBlock::create(record_batch.schema(), columns)
                }

                Ok(
                    flight_data_to_arrow_batch(&flight_data, schema.clone(), &[])
                        .map(create_data_block)?
                )
            }
        })
    }
}
