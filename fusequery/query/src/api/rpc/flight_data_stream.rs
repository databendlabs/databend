// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::Receiver;
use common_arrow::arrow_flight::FlightData;
use tonic::{Status, Streaming};
use tokio_stream::{Stream, StreamExt};
use common_exception::ErrorCodes;
use common_datablocks::DataBlock;
use common_arrow::arrow_flight::utils::flight_data_to_arrow_batch;
use common_datavalues::{DataSchemaRef, DataSchema};
use common_arrow::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::ops::Deref;
use common_arrow::arrow::datatypes::SchemaRef;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug)]
pub struct FlightDataStream();

impl FlightDataStream {
    #[inline]
    pub fn from_remote(schema: SchemaRef, inner: Streaming<FlightData>) -> impl Stream<Item=Result<DataBlock, ErrorCodes>> {
        let schema = schema.clone();
        inner.map(move |flight_data| -> Result<DataBlock, ErrorCodes> {
            match flight_data {
                Err(status) => Err(ErrorCodes::UnknownException(status.message())),
                Ok(flight_data) => {
                    fn create_data_block(record_batch: RecordBatch) -> DataBlock {
                        DataBlock::create(record_batch.schema(), record_batch.columns().to_vec())
                    }

                    Ok(flight_data_to_arrow_batch(&flight_data, schema.clone(), &[])
                        .map(create_data_block)?)
                }
            }
        })
    }

    #[inline]
    pub fn from_receiver(schema: SchemaRef, inner: Receiver<Result<FlightData, ErrorCodes>>) -> impl Stream<Item=Result<DataBlock, ErrorCodes>> {
        let schema = schema.clone();
        ReceiverStream::new(inner).map(move |flight_data| {
            match flight_data {
                Err(error_code) => Err(error_code),
                Ok(flight_data) => {
                    fn create_data_block(record_batch: RecordBatch) -> DataBlock {
                        DataBlock::create(record_batch.schema(), record_batch.columns().to_vec())
                    }

                    Ok(flight_data_to_arrow_batch(&flight_data, schema.clone(), &[])
                        .map(create_data_block)?)
                }
            }
        })
    }
}
