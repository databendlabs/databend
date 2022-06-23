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

use std::sync::Arc;

use common_arrow::arrow::io::ipc::write::default_ipc_fields;
use common_arrow::arrow::io::ipc::write::WriteOptions;
use common_arrow::arrow::io::ipc::IpcField;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::api::rpc::flight_scatter::FlightScatter;

#[derive(Clone)]
pub struct SerializeParams {
    pub options: WriteOptions,
    pub ipc_fields: Vec<IpcField>,
    pub local_executor_pos: usize,
}

#[derive(Clone)]
pub struct ShuffleExchangeParams {
    pub query_id: String,
    pub executor_id: String,
    pub fragment_id: usize,
    pub schema: DataSchemaRef,
    pub destination_ids: Vec<String>,
    pub shuffle_scatter: Arc<Box<dyn FlightScatter>>,
}

#[derive(Clone)]
pub struct MergeExchangeParams {
    pub query_id: String,
    pub fragment_id: usize,
    pub destination_id: String,
    pub schema: DataSchemaRef,
}

pub enum ExchangeParams {
    MergeExchange(MergeExchangeParams),
    ShuffleExchange(ShuffleExchangeParams),
}

impl MergeExchangeParams {
    pub fn create_serialize_params(&self) -> Result<SerializeParams> {
        let arrow_schema = self.schema.to_arrow();
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        Ok(SerializeParams {
            ipc_fields,
            local_executor_pos: 0,
            options: WriteOptions { compression: None },
        })
    }
}

impl ShuffleExchangeParams {
    pub fn create_serialize_params(&self) -> Result<SerializeParams> {
        let arrow_schema = self.schema.to_arrow();
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);

        for (index, executor) in self.destination_ids.iter().enumerate() {
            if executor == &self.executor_id {
                return Ok(SerializeParams {
                    ipc_fields,
                    local_executor_pos: index,
                    options: WriteOptions { compression: None },
                });
            }
        }

        Err(ErrorCode::LogicalError("Not found local executor."))
    }
}

impl ExchangeParams {
    pub fn get_schema(&self) -> DataSchemaRef {
        match self {
            ExchangeParams::ShuffleExchange(exchange) => exchange.schema.clone(),
            ExchangeParams::MergeExchange(exchange) => exchange.schema.clone(),
        }
    }
}
