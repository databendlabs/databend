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

use std::sync::Arc;

use common_arrow::arrow::io::flight::deserialize_batch;
use common_arrow::arrow_format::flight::data::FlightData;
use common_base::tokio::sync::mpsc::Receiver;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
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
                    let arrow_schema = Arc::new(schema.to_arrow());
                    let ipc_fields = common_arrow::arrow::io::ipc::write::default_ipc_fields(
                        &arrow_schema.fields,
                    );
                    let ipc_schema = common_arrow::arrow::io::ipc::IpcSchema {
                        fields: ipc_fields,
                        is_little_endian: true,
                    };
                    let batch = deserialize_batch(
                        &flight_data,
                        arrow_schema,
                        &ipc_schema,
                        &Default::default(),
                    )?;
                    batch.try_into()
                }
            }
        })
    }

    // It is used in testing, and later it will be used in local stream
    #[inline]
    #[allow(dead_code)]
    pub fn from_receiver(
        schema: DataSchemaRef,
        inner: Receiver<Result<FlightData, ErrorCode>>,
    ) -> impl Stream<Item = Result<DataBlock, ErrorCode>> {
        ReceiverStream::new(inner).map(move |flight_data| match flight_data {
            Err(error_code) => Err(error_code),
            Ok(flight_data) => {
                let arrow_schema = Arc::new(schema.to_arrow());
                let ipc_fields =
                    common_arrow::arrow::io::ipc::write::default_ipc_fields(&arrow_schema.fields);
                let ipc_schema = common_arrow::arrow::io::ipc::IpcSchema {
                    fields: ipc_fields,
                    is_little_endian: true,
                };

                let batch = deserialize_batch(
                    &flight_data,
                    arrow_schema,
                    &ipc_schema,
                    &Default::default(),
                )?;
                batch.try_into()
            }
        })
    }
}
