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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_pipeline_core::Pipeline;
use databend_common_settings::FlightCompression;

use super::exchange_params::MergeExchangeParams;
use crate::servers::flight::v1::exchange::serde::TransformExchangeDeserializer;
use crate::servers::flight::v1::exchange::serde::TransformExchangeSerializer;
use crate::servers::flight::v1::exchange::serde::TransformScatterExchangeSerializer;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::ExchangeSorting;
use crate::servers::flight::v1::exchange::ShuffleExchangeParams;
use crate::servers::flight::v1::scatter::BroadcastFlightScatter;
use crate::servers::flight::v1::scatter::FlightScatter;
use crate::servers::flight::v1::scatter::HashFlightScatter;
use crate::sessions::QueryContext;

pub trait ExchangeInjector: Send + Sync + 'static {
    fn flight_scatter(
        &self,
        ctx: &Arc<QueryContext>,
        exchange: &DataExchange,
    ) -> Result<Arc<Box<dyn FlightScatter>>>;

    fn exchange_sorting(&self) -> Option<Arc<dyn ExchangeSorting>>;

    fn apply_merge_serializer(
        &self,
        params: &MergeExchangeParams,
        compression: Option<FlightCompression>,
        pipeline: &mut Pipeline,
    ) -> Result<()>;

    fn apply_shuffle_serializer(
        &self,
        params: &ShuffleExchangeParams,
        compression: Option<FlightCompression>,
        pipeline: &mut Pipeline,
    ) -> Result<()>;

    fn apply_merge_deserializer(
        &self,
        params: &MergeExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()>;

    fn apply_shuffle_deserializer(
        &self,
        params: &ShuffleExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()>;
}

pub struct DefaultExchangeInjector;

impl DefaultExchangeInjector {
    pub fn create() -> Arc<dyn ExchangeInjector> {
        Arc::new(DefaultExchangeInjector {})
    }
}

impl ExchangeInjector for DefaultExchangeInjector {
    fn flight_scatter(
        &self,
        ctx: &Arc<QueryContext>,
        exchange: &DataExchange,
    ) -> Result<Arc<Box<dyn FlightScatter>>> {
        Ok(Arc::new(match exchange {
            DataExchange::Merge(_) => unreachable!(),
            DataExchange::Broadcast(exchange) => Box::new(BroadcastFlightScatter::try_create(
                exchange.destination_ids.len(),
            )?),
            DataExchange::ShuffleDataExchange(exchange) => {
                let local_id = &ctx.get_cluster().local_id;
                let local_pos = exchange
                    .destination_ids
                    .iter()
                    .position(|x| x == local_id)
                    .unwrap();
                HashFlightScatter::try_create(
                    ctx.get_function_context()?,
                    exchange.shuffle_keys.clone(),
                    exchange.destination_ids.len(),
                    local_pos,
                )?
            }
        }))
    }

    fn exchange_sorting(&self) -> Option<Arc<dyn ExchangeSorting>> {
        None
    }

    fn apply_merge_serializer(
        &self,
        params: &MergeExchangeParams,
        compression: Option<FlightCompression>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        pipeline.add_transform(|input, output| {
            TransformExchangeSerializer::create(input, output, params, compression)
        })
    }

    fn apply_shuffle_serializer(
        &self,
        params: &ShuffleExchangeParams,
        compression: Option<FlightCompression>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        pipeline.add_transform(|input, output| {
            TransformScatterExchangeSerializer::create(input, output, compression, params)
        })
    }

    fn apply_merge_deserializer(
        &self,
        params: &MergeExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        pipeline.add_transform(|input, output| {
            Ok(TransformExchangeDeserializer::create(
                input,
                output,
                &params.schema,
            ))
        })
    }

    fn apply_shuffle_deserializer(
        &self,
        params: &ShuffleExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        pipeline.add_transform(|input, output| {
            Ok(TransformExchangeDeserializer::create(
                input,
                output,
                &params.schema,
            ))
        })
    }
}
