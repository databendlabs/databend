// Copyright 2024 RisingWave Labs
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

#![doc = include_str!("README.md")]

mod error;

pub use error::{Error, Result};

/// Re-export `arrow_flight` so downstream crates can use it without declaring it on their own,
/// avoiding version conflicts.
pub use arrow_flight;

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, Criteria, FlightData, FlightDescriptor};
use arrow_schema::Schema;
use futures_util::{stream, Stream, StreamExt, TryStreamExt};
use tonic::transport::Channel;

/// Client for a remote Arrow UDF service.
#[derive(Debug)]
pub struct Client {
    client: FlightServiceClient<Channel>,
    protocol_version: u8,
}

impl Client {
    /// Connect to a UDF service.
    pub async fn connect(addr: impl Into<String>) -> Result<Self> {
        let conn = tonic::transport::Endpoint::new(addr.into())?
            .connect()
            .await?;
        Self::new(FlightServiceClient::new(conn)).await
    }

    /// Create a new client.
    pub async fn new(mut client: FlightServiceClient<Channel>) -> Result<Self> {
        // get protocol version in server
        let protocol_version = match client.do_action(Action::new("protocol_version", "")).await {
            // if `do_action` is not implemented, assume protocol version is 1
            Err(_) => 1,
            // >= 2
            Ok(response) => *response
                .into_inner()
                .next()
                .await
                .ok_or_else(|| Error::Decode("no protocol version".into()))??
                .body
                .first()
                .ok_or_else(|| Error::Decode("invalid protocol version".into()))?,
        };

        Ok(Self {
            client,
            protocol_version,
        })
    }

    /// Get protocol version.
    pub fn protocol_version(&self) -> u8 {
        self.protocol_version
    }

    /// Get function schema.
    pub async fn get(&self, name: &str) -> Result<Function> {
        let descriptor = FlightDescriptor::new_path(vec![name.into()]);
        let response = self.client.clone().get_flight_info(descriptor).await?;
        Function::from_flight_info(response.into_inner())
    }

    /// List all available functions.
    pub async fn list(&self) -> Result<Vec<Function>> {
        let response = self
            .client
            .clone()
            .list_flights(Criteria::default())
            .await?;
        let mut functions = vec![];
        let mut response = response.into_inner();
        while let Some(flight_info) = response.next().await {
            let function = Function::from_flight_info(flight_info?)?;
            functions.push(function);
        }
        Ok(functions)
    }

    /// Call a function.
    pub async fn call(&self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
        self.call_internal(name, input).await
    }

    async fn call_internal(&self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
        let input = input.clone();
        let mut output_stream = self.call_stream_internal(name, input).await?;
        let mut batches = vec![];
        while let Some(batch) = output_stream.next().await {
            batches.push(batch?);
        }
        Ok(arrow_select::concat::concat_batches(
            output_stream
                .schema()
                .ok_or_else(|| Error::Decode("no schema".into()))?,
            batches.iter(),
        )?)
    }

    /// Call a table function.
    pub async fn call_table_function(
        &self,
        name: &str,
        input: &RecordBatch,
    ) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + 'static> {
        let input = input.clone();
        Ok(self
            .call_stream_internal(name, input)
            .await?
            .map_err(|e| e.into()))
    }

    async fn call_stream_internal(
        &self,
        name: &str,
        input: RecordBatch,
    ) -> Result<FlightRecordBatchStream> {
        let descriptor = FlightDescriptor::new_path(vec![name.into()]);
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .build(stream::once(async { Ok(input) }))
            .map(move |res| FlightData {
                flight_descriptor: Some(descriptor.clone()),
                ..res.unwrap()
            });

        // call `do_exchange` on Flight server
        let response = self.client.clone().do_exchange(flight_data_stream).await?;

        // decode response
        let stream = response.into_inner();
        Ok(FlightRecordBatchStream::new_from_flight_data(
            // convert tonic::Status to FlightError
            stream.map_err(|e| e.into()),
        ))
    }
}

/// Function signature.
#[derive(Debug)]
pub struct Function {
    /// Function name.
    pub name: String,
    /// The schema of function arguments.
    pub args: Schema,
    /// The schema of function return values.
    pub returns: Schema,
}

impl Function {
    /// Create a function from a `FlightInfo`.
    fn from_flight_info(info: arrow_flight::FlightInfo) -> Result<Self> {
        let descriptor = info
            .flight_descriptor
            .as_ref()
            .ok_or_else(|| Error::Decode("no descriptor in flight info".into()))?;
        let name = descriptor
            .path
            .first()
            .ok_or_else(|| Error::Decode("empty path in flight descriptor".into()))?
            .clone();
        let input_num = info.total_records as usize;
        let schema = Schema::try_from(info)
            .map_err(|e| Error::Decode(format!("failed to decode schema: {e}")))?;
        if input_num > schema.fields.len() {
            return Err(Error::Decode(format!("invalid input_number: {input_num}")));
        }
        let (input_fields, return_fields) = schema.fields.split_at(input_num);
        Ok(Self {
            name,
            args: Schema::new(input_fields),
            returns: Schema::new(return_fields),
        })
    }
}
