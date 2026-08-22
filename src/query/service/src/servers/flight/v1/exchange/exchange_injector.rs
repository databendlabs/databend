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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline::core::Pipeline;
use databend_common_settings::FlightCompression;

use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::ExchangeSorting;
use crate::servers::flight::v1::exchange::GlobalShuffleExchange;
use crate::servers::flight::v1::exchange::ShuffleExchangeParams;
use crate::servers::flight::v1::exchange::serde::TransformExchangeDeserializer;
use crate::servers::flight::v1::exchange::serde::TransformScatterExchangeSerializer;
use crate::servers::flight::v1::network::DefaultExchangeDataCodec;
use crate::servers::flight::v1::network::ExchangeDataCodec;
use crate::servers::flight::v1::partition::PartitionStream;
use crate::servers::flight::v1::partition::create_hash_partition_streams;
use crate::servers::flight::v1::scatter::FlightScatter;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextSettings;

pub trait ExchangeInjector: Send + Sync + 'static {
    fn exchange_sorting(&self) -> Option<Arc<dyn ExchangeSorting>> {
        None
    }

    fn flight_scatter(
        &self,
        _ctx: &Arc<QueryContext>,
        _exchange: &DataExchange,
    ) -> Result<Arc<dyn FlightScatter>> {
        Err(ErrorCode::Internal(
            "This exchange injector does not support node shuffle",
        ))
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

    fn exchange_data_codec(&self) -> Arc<dyn ExchangeDataCodec> {
        DefaultExchangeDataCodec::create()
    }

    fn partition_streams(
        &self,
        _ctx: &Arc<QueryContext>,
        _exchange: &GlobalShuffleExchange,
        _streams: usize,
        _rows_threshold: usize,
        _bytes_threshold: usize,
    ) -> Result<Vec<Box<dyn PartitionStream>>> {
        Err(ErrorCode::Internal(
            "This exchange injector does not support global shuffle",
        ))
    }
}

pub struct DefaultExchangeInjector;

impl DefaultExchangeInjector {
    pub fn create() -> Arc<dyn ExchangeInjector> {
        Arc::new(DefaultExchangeInjector {})
    }
}

impl ExchangeInjector for DefaultExchangeInjector {
    fn partition_streams(
        &self,
        ctx: &Arc<QueryContext>,
        exchange: &GlobalShuffleExchange,
        streams: usize,
        rows_threshold: usize,
        bytes_threshold: usize,
    ) -> Result<Vec<Box<dyn PartitionStream>>> {
        let local_id = &ctx.get_cluster().local_id;
        let mut local_pos = 0;
        for (destination, channels) in &exchange.destination_channels {
            if destination == local_id {
                break;
            }
            local_pos += channels.len();
        }
        let partitions = exchange
            .destination_channels
            .iter()
            .map(|(_, channels)| channels.len())
            .sum();
        create_hash_partition_streams(
            ctx.get_function_context()?,
            exchange.shuffle_keys.clone(),
            partitions,
            local_pos,
            streams,
            rows_threshold,
            bytes_threshold,
        )
    }
}
