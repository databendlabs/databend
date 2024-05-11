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

use databend_common_expression::DataSchemaRef;

use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::scatter::FlightScatter;

#[derive(Clone)]
pub struct ShuffleExchangeParams {
    pub query_id: String,
    pub executor_id: String,
    pub fragment_id: usize,
    pub schema: DataSchemaRef,
    pub destination_ids: Vec<String>,
    pub shuffle_scatter: Arc<Box<dyn FlightScatter>>,
    pub exchange_injector: Arc<dyn ExchangeInjector>,
}

#[derive(Clone)]
pub struct MergeExchangeParams {
    pub query_id: String,
    pub fragment_id: usize,
    pub destination_id: String,
    pub schema: DataSchemaRef,
    pub ignore_exchange: bool,
    pub allow_adjust_parallelism: bool,
    pub exchange_injector: Arc<dyn ExchangeInjector>,
}

pub enum ExchangeParams {
    MergeExchange(MergeExchangeParams),
    ShuffleExchange(ShuffleExchangeParams),
}

impl ExchangeParams {
    pub fn get_schema(&self) -> DataSchemaRef {
        match self {
            ExchangeParams::ShuffleExchange(exchange) => exchange.schema.clone(),
            ExchangeParams::MergeExchange(exchange) => exchange.schema.clone(),
        }
    }

    pub fn get_query_id(&self) -> String {
        match self {
            ExchangeParams::ShuffleExchange(exchange) => exchange.query_id.clone(),
            ExchangeParams::MergeExchange(exchange) => exchange.query_id.clone(),
        }
    }
}
