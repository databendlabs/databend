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
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;

use super::exchange_packet_sink::create_packet_writer_item;
use super::exchange_packet_sink::install_packet_outbound_failure_handler;
use super::exchange_params::BroadcastExchangeParams;
use super::exchange_params::ExchangeParams;
use super::exchange_params::GlobalExchangeParams;
use super::exchange_sink_writer::create_writer_item;
use super::exchange_sorting::ExchangeSorting;
use super::exchange_sorting::TransformExchangeSorting;
use super::exchange_transform_shuffle::exchange_shuffle;
use super::hash_send_sink::HashSendSink;
use super::outbound_send_channels::SharedOutboundChannels;
use super::serde::ExchangeSerializeMeta;
use crate::clusters::ClusterHelper;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::network::BlockOutboundConfig;
use crate::servers::flight::v1::network::BlockOutboundSet;
use crate::servers::flight::v1::network::ExchangeBufferConfig;
use crate::servers::flight::v1::network::ExchangeSinkBuffer;
use crate::servers::flight::v1::network::LegacyRemoteChannel;
use crate::servers::flight::v1::network::OutboundChannel;
use crate::servers::flight::v1::network::RemoteOutboundChannel;
use crate::servers::flight::v1::network::RoundRobinChannel;
use crate::servers::flight::v1::network::RoundRobinOutboundChannel;
use crate::servers::flight::v1::network::create_local_channels;
use crate::servers::flight::v1::scatter::HashFlightScatter;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextSettings;

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
                let enable_new_flight = ctx.get_settings().get_enable_experiment_new_flight()?;

                let items = if enable_new_flight {
                    let mut pending = exchange_manager
                        .take_block_outbounds(&params.query_id, &params.channel_id)?;
                    let outbound = pending.remove(&params.destination_id).ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "block outbound not found for target {}",
                            params.destination_id
                        ))
                    })?;
                    let outbound = Arc::new(BlockOutboundSet::create_with_producers(
                        vec![outbound],
                        1,
                        BlockOutboundConfig::default(),
                        &GlobalIORuntime::instance(),
                    ));
                    install_packet_outbound_failure_handler(outbound.clone(), pipeline);
                    vec![create_packet_writer_item(
                        outbound,
                        0,
                        params.ignore_exchange,
                    )]
                } else {
                    exchange_manager
                        .get_flight_sender(&ExchangeParams::MergeExchange(params.clone()))?
                        .into_iter()
                        .map(|(_, sender)| create_writer_item(sender, params.ignore_exchange))
                        .collect::<Vec<_>>()
                };

                let output = items.len();
                pipeline.try_resize(output)?;

                pipeline.add_pipe(Pipe::create(output, 0, items));
                Ok(())
            }
            ExchangeParams::BroadcastExchange(_) => Err(ErrorCode::Internal(
                "BroadcastExchange should not appear on the sink side",
            )),
            ExchangeParams::NodeShuffleExchange(params) => {
                exchange_shuffle(ctx, params, pipeline)?;

                let exchange_manager = ctx.get_exchange_manager();
                let enable_new_flight = ctx.get_settings().get_enable_experiment_new_flight()?;

                // exchange writer sink
                let len = pipeline.output_len();
                let items = if enable_new_flight {
                    build_node_shuffle_packet_sinks(ctx, params, pipeline, 1)?
                } else {
                    exchange_manager
                        .get_flight_sender(&ExchangeParams::NodeShuffleExchange(params.clone()))?
                        .into_iter()
                        .map(|(_, sender)| create_writer_item(sender, false))
                        .collect::<Vec<_>>()
                };

                pipeline.add_pipe(Pipe::create(len, 0, items));
                Ok(())
            }
            ExchangeParams::GlobalShuffleExchange(params) => {
                Self::hash_exchange_sink(ctx, pipeline, params)
            }
        }
    }

    fn hash_exchange_sink(
        ctx: &Arc<QueryContext>,
        pipeline: &mut Pipeline,
        params: &GlobalExchangeParams,
    ) -> Result<()> {
        let mut local_pos = 0;
        let mut local_threads = 0;

        for (dest, threads) in &params.destination_channels {
            if dest == &params.executor_id {
                local_threads = threads.len();
                break;
            }

            local_pos += threads.len();
        }

        let compression = ctx.get_settings().get_query_flight_compression()?;
        let enable_new_flight = ctx.get_settings().get_enable_experiment_new_flight()?;
        let rows_threshold = ctx.get_settings().get_hash_shuffle_rows_threshold()?;
        let bytes_threshold = ctx.get_settings().get_hash_shuffle_bytes_threshold()?;
        let waker = pipeline.get_waker();

        pipeline.resize(local_threads, false)?;

        let query_id = &params.query_id;
        let exchange_id = &params.exchange_id;
        let exchange_manager = DataExchangeManager::instance();

        let channel_set = exchange_manager.get_exchange_channel_set(query_id, exchange_id)?;
        assert_eq!(channel_set.channels.len(), local_threads);

        let local_outbound = create_local_channels(&channel_set);
        let remote_outbound =
            build_hash_outbound_channels(params, local_outbound, compression, enable_new_flight)?;
        remote_outbound.install_failure_handler(pipeline);

        let scatter = Arc::new(HashFlightScatter::try_create(
            ctx.get_function_context()?,
            params.shuffle_keys.clone(),
            remote_outbound.len(),
            local_pos,
        )?);

        let mut items = Vec::with_capacity(local_threads);
        for idx in 0..local_threads {
            items.push(HashSendSink::create_item(
                idx,
                scatter.clone(),
                remote_outbound.clone(),
                waker.clone(),
                rows_threshold,
                bytes_threshold,
            ));
        }

        pipeline.add_pipe(Pipe::create(local_threads, 0, items));
        Ok(())
    }
}

pub(super) fn build_node_shuffle_packet_sinks(
    ctx: &Arc<QueryContext>,
    params: &crate::servers::flight::v1::exchange::ShuffleExchangeParams,
    pipeline: &mut Pipeline,
    local_output_parallelism: usize,
) -> Result<Vec<PipeItem>> {
    let exchange_manager = ctx.get_exchange_manager();
    let mut pending_outbounds = Vec::new();

    for (destination, channels) in &params.destination_channels {
        if destination == &params.executor_id {
            continue;
        }
        let [channel] = channels.as_slice() else {
            return Err(ErrorCode::Internal(format!(
                "node shuffle target {} has {} channels, expected one",
                destination,
                channels.len()
            )));
        };
        let mut pending =
            exchange_manager.take_block_outbounds(&params.query_id, channel.as_str())?;
        pending_outbounds.push(pending.remove(destination).ok_or_else(|| {
            ErrorCode::Internal(format!(
                "block outbound not found for target {}",
                destination
            ))
        })?);
    }

    let remote_count = pending_outbounds.len();
    let outbound = Arc::new(BlockOutboundSet::create_with_producers(
        pending_outbounds,
        remote_count,
        BlockOutboundConfig::default(),
        &GlobalIORuntime::instance(),
    ));
    install_packet_outbound_failure_handler(outbound.clone(), pipeline);

    let mut remote_index = 0;
    let mut items = Vec::with_capacity(params.destination_channels.len());
    for (destination, _) in &params.destination_channels {
        if destination == &params.executor_id {
            items.push(if local_output_parallelism == 1 {
                databend_common_pipeline_transforms::processors::create_dummy_item()
            } else {
                databend_common_pipeline::basic::create_resize_item(1, local_output_parallelism)
            });
        } else {
            items.push(create_packet_writer_item(
                outbound.clone(),
                remote_index,
                false,
            ));
            remote_index += 1;
        }
    }
    Ok(items)
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

/// Build outbound channels for a broadcast exchange.
pub(super) fn build_broadcast_outbound_channels(
    params: &BroadcastExchangeParams,
    local_outbound_channels: Vec<Arc<dyn OutboundChannel>>,
    compression: Option<databend_common_settings::FlightCompression>,
    enable_new_flight: bool,
) -> Result<SharedOutboundChannels> {
    let query_id = &params.query_id;
    let exchange_id = &params.exchange_id;
    let exchange_manager = DataExchangeManager::instance();

    if !enable_new_flight {
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

        let shared_buffer = Arc::new(ExchangeSinkBuffer::create(
            exchanges_seq,
            ExchangeBufferConfig::default(),
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
                remote_channels.push(LegacyRemoteChannel::create(
                    remote_idx,
                    thread_idx,
                    shared_buffer.clone(),
                    compression,
                )?);
            }
            channels.push(RoundRobinChannel::create(remote_channels));
            remote_idx += 1;
        }

        return Ok(SharedOutboundChannels::legacy(channels));
    }

    let mut block_outbounds = exchange_manager.take_block_outbounds(query_id, exchange_id)?;

    let mut remote_outbounds = Vec::with_capacity(block_outbounds.len());

    for (target_id, _threads) in &params.destination_channels {
        if target_id != &params.executor_id {
            let outbound = block_outbounds.remove(target_id.as_str()).ok_or_else(|| {
                ErrorCode::Internal(format!("block outbound not found for target {}", target_id))
            })?;
            remote_outbounds.push(outbound);
        }
    }

    let config = BlockOutboundConfig::default();
    let shared_outbounds = Arc::new(BlockOutboundSet::create_with_producers(
        remote_outbounds,
        local_outbound_channels.len(),
        config,
        &GlobalIORuntime::instance(),
    ));

    let local_channel = RoundRobinOutboundChannel::create(local_outbound_channels);
    let mut remote_idx = 0;
    let mut channels = vec![];
    for (target_id, threads) in &params.destination_channels {
        if target_id == &params.executor_id {
            channels.push(local_channel.clone());
            continue;
        }

        let mut remote_channels = Vec::with_capacity(threads.len());
        for thread_idx in 0..threads.len() {
            remote_channels.push(RemoteOutboundChannel::create(
                remote_idx,
                thread_idx,
                shared_outbounds.clone(),
                compression,
            )?);
        }

        channels.push(RoundRobinOutboundChannel::create(remote_channels));
        remote_idx += 1;
    }

    Ok(SharedOutboundChannels::reconnectable(
        channels,
        shared_outbounds,
    ))
}

/// Build per-thread OutboundChannels for hash exchange.
pub(super) fn build_hash_outbound_channels(
    params: &GlobalExchangeParams,
    mut local_outbound_channels: Vec<Arc<dyn OutboundChannel>>,
    compression: Option<databend_common_settings::FlightCompression>,
    enable_new_flight: bool,
) -> Result<SharedOutboundChannels> {
    let num_threads = local_outbound_channels.len();
    let query_id = &params.query_id;
    let exchange_id = &params.exchange_id;
    let exchange_manager = DataExchangeManager::instance();
    if !enable_new_flight {
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

        let shared_buffer = Arc::new(ExchangeSinkBuffer::create(
            exchanges_seq,
            ExchangeBufferConfig::default(),
            &GlobalIORuntime::instance(),
        )?);
        let mut remote_idx = 0;
        let mut channels = Vec::with_capacity(params.destination_channels.len() * num_threads);

        for (target_id, threads) in &params.destination_channels {
            if target_id == &params.executor_id {
                channels.extend(std::mem::take(&mut local_outbound_channels));
                continue;
            }

            for t_idx in 0..threads.len() {
                channels.push(LegacyRemoteChannel::create(
                    remote_idx,
                    t_idx,
                    shared_buffer.clone(),
                    compression,
                )?);
            }
            remote_idx += 1;
        }

        return Ok(SharedOutboundChannels::legacy(channels));
    }

    let mut block_outbounds = exchange_manager.take_block_outbounds(query_id, exchange_id)?;

    let mut remote_outbounds = Vec::with_capacity(block_outbounds.len());

    for (target_id, _threads) in &params.destination_channels {
        if target_id != &params.executor_id {
            let outbound = block_outbounds.remove(target_id.as_str()).ok_or_else(|| {
                ErrorCode::Internal(format!("block outbound not found for target {}", target_id))
            })?;
            remote_outbounds.push(outbound);
        }
    }

    let config = BlockOutboundConfig::default();
    let shared_outbounds = Arc::new(BlockOutboundSet::create_with_producers(
        remote_outbounds,
        num_threads,
        config,
        &GlobalIORuntime::instance(),
    ));

    let mut remote_idx = 0;
    let mut channels = Vec::with_capacity(params.destination_channels.len() * num_threads);

    for (target_id, threads) in &params.destination_channels {
        if target_id == &params.executor_id {
            channels.extend(std::mem::take(&mut local_outbound_channels));
            continue;
        }

        for t_idx in 0..threads.len() {
            channels.push(RemoteOutboundChannel::create(
                remote_idx,
                t_idx,
                shared_outbounds.clone(),
                compression,
            )?);
        }

        remote_idx += 1;
    }

    Ok(SharedOutboundChannels::reconnectable(
        channels,
        shared_outbounds,
    ))
}
