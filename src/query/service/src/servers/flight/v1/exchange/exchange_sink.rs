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

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::Pipeline;

use super::exchange_params::BroadcastExchangeParams;
use super::exchange_params::ExchangeParams;
use super::exchange_params::GlobalExchangeParams;
use super::exchange_sink_writer::create_writer_item;
use super::exchange_transform_shuffle::exchange_shuffle;
use super::partition_send_sink::PartitionSendSink;
use super::serde::TransformExchangeSerializer;
use crate::clusters::ClusterHelper;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::network::OutboundChannel;
use crate::servers::flight::v1::network::RemoteChannel;
use crate::servers::flight::v1::network::RoundRobinChannel;
use crate::servers::flight::v1::network::create_local_channels;
use crate::servers::flight::v1::network::outbound_buffer::ExchangeBufferConfig;
use crate::servers::flight::v1::network::outbound_buffer::ExchangeSinkBuffer;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextSettings;

pub struct ExchangeSink;

impl ExchangeSink {
    pub fn via(
        ctx: &Arc<QueryContext>,
        params: &mut ExchangeParams,
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

                if !params.ignore_exchange {
                    let settings = ctx.get_settings();
                    let compression = settings.get_query_flight_compression()?;
                    pipeline.add_transform(|input, output| {
                        TransformExchangeSerializer::create(input, output, params, compression)
                    })?;
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
            ExchangeParams::BroadcastExchange(_) => Err(ErrorCode::Internal(
                "BroadcastExchange should not appear on the sink side",
            )),
            ExchangeParams::NodeShuffleExchange(params) => {
                exchange_shuffle(ctx, params, pipeline)?;

                let exchange_manager = ctx.get_exchange_manager();
                let senders = exchange_manager
                    .get_flight_sender(&ExchangeParams::NodeShuffleExchange(params.clone()))?;
                let len = pipeline.output_len();
                let items = senders
                    .into_iter()
                    .map(|(_, sender)| create_writer_item(sender, false))
                    .collect::<Vec<_>>();
                pipeline.add_pipe(Pipe::create(len, 0, items));
                Ok(())
            }
            ExchangeParams::GlobalShuffleExchange(params) => {
                Self::global_shuffle_sink(ctx, pipeline, params)
            }
        }
    }

    fn global_shuffle_sink(
        ctx: &Arc<QueryContext>,
        pipeline: &mut Pipeline,
        params: &mut GlobalExchangeParams,
    ) -> Result<()> {
        let local_threads = params
            .destination_channels
            .iter()
            .find_map(|(destination, channels)| {
                (destination == &params.executor_id).then_some(channels.len())
            })
            .ok_or_else(|| ErrorCode::Internal("Global shuffle has no local destination"))?;

        pipeline.resize(local_threads, false)?;

        let exchange_manager = DataExchangeManager::instance();
        let channel_set = exchange_manager.get_or_create_exchange_channel_set(
            &params.query_id,
            &params.exchange_id,
            local_threads,
        )?;
        if channel_set.channels.len() != local_threads {
            return Err(ErrorCode::Internal(format!(
                "Global shuffle channel count mismatch: expected {local_threads}, got {}",
                channel_set.channels.len()
            )));
        }

        let compression = ctx.get_settings().get_query_flight_compression()?;
        let local_outbound = create_local_channels(&channel_set);
        let channels = build_global_shuffle_outbound_channels(params, local_outbound, compression)?;
        let waker = pipeline.get_waker();
        let partition_streams = params.take_partition_streams(local_threads)?;
        let mut items = Vec::with_capacity(local_threads);
        for (worker_id, partition_stream) in partition_streams.into_iter().enumerate() {
            items.push(PartitionSendSink::create_item(
                worker_id,
                partition_stream,
                channels.clone(),
                waker.clone(),
            ));
        }
        pipeline.add_pipe(Pipe::create(local_threads, 0, items));
        Ok(())
    }
}

/// Build OutboundChannels for broadcast exchange using PingPongExchange.
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
    let shared_buffer = Arc::new(ExchangeSinkBuffer::create(
        exchanges_seq,
        config,
        &GlobalIORuntime::instance(),
    )?);

    let local_channel = RoundRobinChannel::create(local_outbound_channels);
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

        channels.push(RoundRobinChannel::create(remote_channels));
        remote_idx += 1;
    }

    Ok(channels)
}

/// Build one outbound channel per global destination worker. Nodes and their
/// worker channels stay in scheduler order, which is also the partition order
/// used by the operator-provided partition streams. Remote channels install the operator
/// codec while local channels preserve the in-memory block representation.
pub(super) fn build_global_shuffle_outbound_channels(
    params: &GlobalExchangeParams,
    mut local_outbound_channels: Vec<Arc<dyn OutboundChannel>>,
    compression: Option<databend_common_settings::FlightCompression>,
) -> Result<Vec<Arc<dyn OutboundChannel>>> {
    let exchange_manager = DataExchangeManager::instance();
    let mut exchanges =
        exchange_manager.take_ping_pong_exchanges(&params.query_id, &params.exchange_id)?;
    let mut exchanges_seq = Vec::with_capacity(exchanges.len());

    for (target_id, threads) in &params.destination_channels {
        if target_id == &params.executor_id {
            continue;
        }
        let exchange = exchanges.remove(target_id.as_str()).ok_or_else(|| {
            ErrorCode::Internal(format!(
                "PingPongExchange not found for global shuffle target {target_id}"
            ))
        })?;
        if threads.len() != exchange.num_threads {
            return Err(ErrorCode::Internal(format!(
                "Global shuffle channel count mismatch for {target_id}: expected {}, got {}",
                threads.len(),
                exchange.num_threads
            )));
        }
        exchanges_seq.push(exchange);
    }

    let shared_buffer = Arc::new(ExchangeSinkBuffer::create(
        exchanges_seq,
        ExchangeBufferConfig::default(),
        &GlobalIORuntime::instance(),
    )?);

    let mut remote_idx = 0;
    let total_threads = params
        .destination_channels
        .iter()
        .map(|(_, threads)| threads.len())
        .sum();
    let mut channels = Vec::with_capacity(total_threads);
    for (target_id, threads) in &params.destination_channels {
        if target_id == &params.executor_id {
            channels.extend(std::mem::take(&mut local_outbound_channels));
            continue;
        }

        for thread_idx in 0..threads.len() {
            channels.push(RemoteChannel::create_with_codec(
                remote_idx,
                thread_idx,
                shared_buffer.clone(),
                compression,
                params.codec.clone(),
            )?);
        }
        remote_idx += 1;
    }

    Ok(channels)
}
