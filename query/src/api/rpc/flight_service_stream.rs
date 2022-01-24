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

use std::convert::TryInto;

use common_arrow::arrow::io::flight::serialize_batch;
use common_arrow::arrow::io::ipc::write::WriteOptions;
use common_arrow::arrow::io::ipc::IpcField;
use common_arrow::arrow_format::flight::data::FlightData;
use common_base::tokio::macros::support::Pin;
use common_base::tokio::macros::support::Poll;
use common_base::tokio::sync::mpsc::Receiver;
use common_datablocks::DataBlock;
use futures::task::Context;
use tokio_stream::Stream;
use tonic::Status;

pub struct FlightDataStream {
    input: Receiver<common_exception::Result<DataBlock>>,
    ipc_fields: Vec<IpcField>,
    options: WriteOptions,
}

impl FlightDataStream {
    pub fn create(
        input: Receiver<common_exception::Result<DataBlock>>,
        ipc_fields: Vec<IpcField>,
    ) -> FlightDataStream {
        FlightDataStream {
            input,
            ipc_fields,
            options: WriteOptions { compression: None },
        }
    }
}

impl Stream for FlightDataStream {
    type Item = Result<FlightData, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.input.poll_recv(cx).map(|x| match x {
            None => None,
            Some(Err(error)) => Some(Err(Status::from(error))),
            Some(Ok(block)) => match block.try_into() {
                Err(error) => Some(Err(Status::from(error))),
                Ok(record_batch) => {
                    let (dicts, values) =
                        serialize_batch(&record_batch, &self.ipc_fields, &self.options);

                    match dicts.is_empty() {
                        true => Some(Ok(values)),
                        false => Some(Err(Status::unimplemented(
                            "DatabendQuery does not implement dicts.",
                        ))),
                    }
                }
            },
        })
    }
}
