// Copyright 2021 Datafuse Labs
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

use arrow_array::builder::Int32Builder;
use arrow_array::builder::StringBuilder;
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_flight::sql::SqlInfo;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::FlightData;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use futures_util::stream;
use tonic::Status;

use crate::servers::flight_sql::flight_sql_service::DoGetStream;

pub(super) struct SqlInfoProvider {}

impl SqlInfoProvider {
    fn key_field() -> Field {
        Field::new("info_name", DataType::Int32, false)
    }

    fn string_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Self::key_field(),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    fn key_array(info: SqlInfo) -> ArrayRef {
        let mut builder = Int32Builder::new();
        builder.append_value(info as i32);
        Arc::new(builder.finish())
    }

    fn string_array(value: &str) -> ArrayRef {
        let mut builder = StringBuilder::new();
        builder.append_value(value);
        Arc::new(builder.finish())
    }

    fn server_name() -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(Self::string_schema(), vec![
            Self::key_array(SqlInfo::FlightSqlServerName),
            Self::string_array("Databend"),
        ])
    }

    fn all_info_flight_data() -> Result<Vec<FlightData>, ArrowError> {
        let batch = Self::server_name()?;
        let schema = (*batch.schema()).clone();
        let batches = vec![batch];
        batches_to_flight_data(&schema, batches)
    }

    pub fn all_info() -> Result<DoGetStream, Status> {
        let flight_data = Self::all_info_flight_data()
            .map_err(|e| Status::internal(format!("{e:?}")))?
            .into_iter()
            .map(Ok);
        let stream = stream::iter(flight_data);
        Ok(Box::pin(stream))
    }
}
