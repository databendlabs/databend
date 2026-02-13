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
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::processors::create_dummy_item;

use super::exchange_params::BroadcastExchangeParams;
use super::exchange_params::ExchangeParams;
use super::exchange_sink_writer::create_writer_item;
use super::exchange_sorting::ExchangeSorting;
use super::exchange_sorting::TransformExchangeSorting;
use super::exchange_transform_shuffle::exchange_shuffle;
use super::serde::ExchangeSerializeMeta;
use super::thread_channel_writer::ThreadChannelWriter;
use crate::clusters::ClusterHelper;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::network::BroadcastChannel;
use crate::servers::flight::v1::network::OutboundChannel;
use crate::servers::flight::v1::network::RemoteChannel;
use crate::servers::flight::v1::network::outbound_buffer::ExchangeBufferConfig;
use crate::servers::flight::v1::network::outbound_buffer::ExchangeSinkBuffer;
use crate::servers::flight::v1::scatter::BroadcastFlightScatter;
use crate::servers::flight::v1::scatter::FlightScatter;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ExchangeSink;

impl ExchangeSink {
    pub fn via(
        ctx: &Arc<QueryContext>,
        params: &ExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        match params {
            ExchangeParams::MergeExchange(params) => {
                if params.destination_id == ctx.get_cluster().local_id() {
                    return Err(ErrorCode::Internal(format!(
                        "Locally depends on merge exchange, but the localhost is not a coordination node. executor: {}, destination_id: {}, fragment id: {}",
                        ctx.get_cluster().local_id(),
                        params.destination_id,
                        params.fragment_id
                    )));
                }

                let exchange_injector = &params.exchange_injector;

                if !params.ignore_exchange {
                    let settings = ctx.get_settings();
                    let compression = settings.get_query_flight_compression()?;
                    exchange_injector.apply_merge_serializer(params, compression, pipeline)?;
                }

                if !params.ignore_exchange && exchange_injector.exchange_sorting().is_some() {
                    let output_len = pipeline.output_len();
                    let sorting = SinkExchangeSorting::create();
                    let transform = TransformExchangeSorting::create(output_len, sorting);

                    let output = transform.get_output();
                    let inputs = transform.get_inputs();
                    pipeline.add_pipe(Pipe::create(output_len, 1, vec![PipeItem::create(
                        ProcessorPtr::create(Box::new(transform)),
                        inputs,
                        vec![output],
                    )]));
                }

                let exchange_manager = ctx.get_exchange_manager();
                let senders = exchange_manager
                    .get_flight_sender(&ExchangeParams::MergeExchange(params.clone()))?;

                let output = senders.len();
                pipeline.try_resize(output)?;

                let items = senders
                    .into_iter()
                    .map(|(_, sender)| create_writer_item(sender, params.ignore_exchange))
                    .collect::<Vec<_>>();

                pipeline.add_pipe(Pipe::create(output, 0, items));
                Ok(())
            }
            ExchangeParams::BroadcastExchange(params) => {
                Self::broadcast_sink(ctx, pipeline, params)
            }
            ExchangeParams::NodeShuffleExchange(params) => {
                exchange_shuffle(ctx, params, pipeline)?;

                let exchange_manager = ctx.get_exchange_manager();
                let senders = exchange_manager
                    .get_flight_sender(&ExchangeParams::NodeShuffleExchange(params.clone()))?;

                // exchange writer sink
                let len = pipeline.output_len();

                let items = senders
                    .into_iter()
                    .map(|(_, sender)| create_writer_item(sender, false))
                    .collect::<Vec<_>>();

                pipeline.add_pipe(Pipe::create(len, 0, items));
                Ok(())
            }
            ExchangeParams::GlobalShuffleExchange(_) => Ok(()),
        }
    }

    /// BroadcastExchange sink: use ThreadChannelWriter to broadcast data
    /// to all destinations. Local output is discarded via dummy sinks.
    ///
    /// ```text
    /// Pipe 1: P ThreadChannelWriters  (P inputs → P outputs)
    ///         remote partitions → OutboundChannels (RemoteChannel → PingPong)
    ///         local partition → output port (discarded)
    /// Pipe 2: P dummy sinks           (P inputs → 0 outputs)
    /// ```
    fn broadcast_sink(
        ctx: &Arc<QueryContext>,
        pipeline: &mut Pipeline,
        params: &BroadcastExchangeParams,
    ) -> Result<()> {
        let num_destinations = params.destination_channels.len();
        let local_pos = params
            .destination_channels
            .iter()
            .position(|(dest, _)| dest == &params.executor_id)
            .expect("local destination not found in broadcast exchange");

        let scatter: Arc<Box<dyn FlightScatter>> = Arc::new(Box::new(
            BroadcastFlightScatter::try_create(num_destinations)?,
        ));

        let compression = ctx.get_settings().get_query_flight_compression()?;

        let local_channels = vec![];
        let channels = build_broadcast_outbound_channels(params, local_channels, compression)?;

        let output_len = pipeline.output_len();
        let mut output_items = Vec::with_capacity(output_len);

        for _idx in 0..output_len {
            output_items.push(ThreadChannelWriter::create_item(
                channels.clone(),
                local_pos,
                scatter.clone(),
                1024,
                256 * 1024,
            ));
        }

        pipeline.add_pipe(Pipe::create(output_len, output_len, output_items));

        // Pipe 2: dummy sinks to consume local output (P inputs → 0 outputs)
        let sink_items: Vec<_> = (0..output_len).map(|_| create_dummy_item()).collect();
        pipeline.add_pipe(Pipe::create(output_len, 0, sink_items));

        Ok(())
    }
}

struct SinkExchangeSorting;

impl SinkExchangeSorting {
    pub fn create() -> Arc<dyn ExchangeSorting> {
        Arc::new(SinkExchangeSorting {})
    }
}

impl ExchangeSorting for SinkExchangeSorting {
    fn block_number(&self, data_block: &DataBlock) -> Result<isize> {
        let block_meta = data_block.get_meta();
        let shuffle_meta = block_meta
            .and_then(ExchangeSerializeMeta::downcast_ref_from)
            .ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Failed to downcast ExchangeSerializeMeta from BlockMeta: {:?}",
                    block_meta
                ))
            })?;

        Ok(shuffle_meta.block_number)
    }
}

/// Build OutboundChannels for broadcast exchange using PingPongExchange.
/// Local destination uses a LocalOutboundChannel; remote destinations
/// use BroadcastChannel wrapping multiple RemoteChannels (one per thread).
pub(super) fn build_broadcast_outbound_channels(
    params: &BroadcastExchangeParams,
    local_outbound_channels: Vec<Arc<dyn OutboundChannel>>,
    compression: Option<databend_common_settings::FlightCompression>,
) -> Result<Vec<Arc<dyn OutboundChannel>>> {
    let query_id = &params.query_id;
    let exchange_id = &params.exchange_id;
    let exchange_manager = DataExchangeManager::instance();
    let mut exchanges = exchange_manager.take_ping_pong_exchanges(query_id, exchange_id)?;

    let mut exchanges_seq = Vec::with_capacity(exchanges.len());

    for (target_id, threads) in &params.destination_channels {
        if target_id != &params.executor_id {
            let exchange = exchanges.remove(target_id.as_str()).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "PingPongExchange not found for target {}",
                    target_id
                ))
            })?;
            assert_eq!(threads.len(), exchange.num_threads);
            exchanges_seq.push(exchange);
        }
    }

    // Create shared ExchangeSinkBuffer: one RemoteInstance per PingPong, N channels each
    let config = ExchangeBufferConfig::default();
    let shared_buffer = Arc::new(ExchangeSinkBuffer::create(exchanges_seq, config)?);

    let local_channel = BroadcastChannel::create(local_outbound_channels);
    let mut remote_idx = 0;
    let mut channels = vec![];
    for (target_id, threads) in &params.destination_channels {
        if target_id == &params.executor_id {
            channels.push(local_channel.clone());
            continue;
        }

        let mut remote_channels = Vec::with_capacity(threads.len());
        for thread_idx in 0..threads.len() {
            remote_channels.push(RemoteChannel::create(
                remote_idx,
                thread_idx,
                shared_buffer.clone(),
                compression,
            )?);
        }

        channels.push(BroadcastChannel::create(remote_channels));
        remote_idx += 1;
    }

    Ok(channels)
}
