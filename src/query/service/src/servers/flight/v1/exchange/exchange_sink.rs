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
use databend_common_pipeline_transforms::processors::create_dummy_item;
use tokio::sync::Semaphore;

use super::exchange_packet_sink::create_packet_writer_item;
use super::exchange_params::BroadcastExchangeParams;
use super::exchange_params::ExchangeParams;
use super::exchange_params::GlobalExchangeParams;
use super::exchange_sorting::ExchangeSorting;
use super::exchange_sorting::TransformExchangeSorting;
use super::exchange_transform_shuffle::exchange_shuffle;
use super::hash_send_sink::HashSendSink;
use super::outbound_send_channels::SharedOutboundChannels;
use super::outbound_send_channels::fail_streams_on_pipeline_error;
use super::serde::ExchangeSerializeMeta;
use crate::clusters::ClusterHelper;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::exchange::local_channel::create_local_channels;
use crate::servers::flight::v1::exchange::outbound_channel::OutboundChannel;
use crate::servers::flight::v1::exchange::outbound_channel::RemoteOutboundChannel;
use crate::servers::flight::v1::exchange::outbound_channel::RoundRobinChannel;
use crate::servers::flight::v1::scatter::HashFlightScatter;
use crate::servers::flight::v1::transport::OutboundStreamRef;
use crate::servers::flight::v1::transport::legacy::ExchangeBufferConfig;
use crate::servers::flight::v1::transport::legacy::ExchangeSinkBuffer;
use crate::servers::flight::v1::transport::reliable::PendingReliableOutbound;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextSettings;

const QUEUE_CAPACITY: usize = 64;
const MAX_BATCH_BYTES: usize = 256 * 1024;

fn start_reliable_outbound(pending: PendingReliableOutbound) -> OutboundStreamRef {
    Arc::new(pending.start(
        Arc::new(Semaphore::new(QUEUE_CAPACITY)),
        Some(MAX_BATCH_BYTES),
        &GlobalIORuntime::instance(),
    ))
}

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
                let items = if ctx.get_settings().get_enable_experiment_new_flight()? {
                    let mut pending = exchange_manager
                        .take_new_flight_fragment_outbounds(&params.query_id, &params.channel_id)?;
                    let outbound = pending.remove(&params.destination_id).ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "New Flight outbound not found for target {}",
                            params.destination_id
                        ))
                    })?;
                    let stream = start_reliable_outbound(outbound);
                    fail_streams_on_pipeline_error(std::slice::from_ref(&stream), pipeline);
                    vec![create_packet_writer_item(stream, params.ignore_exchange)]
                } else {
                    let streams = exchange_manager
                        .take_fragment_outbound_streams(&ExchangeParams::MergeExchange(
                            params.clone(),
                        ))?
                        .into_iter()
                        .map(|(_, stream)| {
                            stream.ok_or_else(|| {
                                ErrorCode::Internal("Merge exchange cannot target the local node")
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    build_legacy_packet_sinks(
                        streams.into_iter().map(Some).collect(),
                        params.ignore_exchange,
                        || unreachable!("merge exchange cannot target the local node"),
                    )
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

                // exchange writer sink
                let len = pipeline.output_len();
                let items = if ctx.get_settings().get_enable_experiment_new_flight()? {
                    build_node_shuffle_packet_sinks(ctx, params, pipeline, 1)?
                } else {
                    let streams = exchange_manager.take_fragment_outbound_streams(
                        &ExchangeParams::NodeShuffleExchange(params.clone()),
                    )?;
                    build_legacy_packet_sinks(
                        streams.into_iter().map(|(_, stream)| stream).collect(),
                        false,
                        create_dummy_item,
                    )
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
        let new_flight = ctx.get_settings().get_enable_experiment_new_flight()?;
        let rows_threshold = ctx.get_settings().get_hash_shuffle_rows_threshold()?;
        let bytes_threshold = ctx.get_settings().get_hash_shuffle_bytes_threshold()?;
        let waker = pipeline.get_waker();

        pipeline.resize(local_threads, false)?;

        let query_id = &params.query_id;
        let exchange_id = &params.exchange_id;
        let exchange_manager = DataExchangeManager::instance();

        let channel_set = exchange_manager.get_exchange_channel_set(query_id, exchange_id)?;
        assert_eq!(channel_set.receivers.len(), local_threads);

        let local_outbound = create_local_channels(&channel_set);
        let remote_outbound =
            build_hash_outbound_channels(params, local_outbound, compression, new_flight)?;
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

/// Builds legacy packet sinks without sharing completion across independent do_get streams.
pub(super) fn build_legacy_packet_sinks(
    streams: Vec<Option<OutboundStreamRef>>,
    ignore_exchange: bool,
    mut local_item: impl FnMut() -> PipeItem,
) -> Vec<PipeItem> {
    streams
        .into_iter()
        .map(|stream| match stream {
            None => local_item(),
            Some(stream) => create_packet_writer_item(stream, ignore_exchange),
        })
        .collect()
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
        let mut pending = exchange_manager
            .take_new_flight_fragment_outbounds(&params.query_id, channel.as_str())?;
        pending_outbounds.push(pending.remove(destination).ok_or_else(|| {
            ErrorCode::Internal(format!(
                "New Flight outbound not found for target {}",
                destination
            ))
        })?);
    }

    let streams = pending_outbounds
        .into_iter()
        .map(start_reliable_outbound)
        .collect::<Vec<_>>();
    fail_streams_on_pipeline_error(&streams, pipeline);
    let mut streams = streams.into_iter();

    let mut items = Vec::with_capacity(params.destination_channels.len());
    for (destination, _) in &params.destination_channels {
        if destination == &params.executor_id {
            items.push(if local_output_parallelism == 1 {
                databend_common_pipeline_transforms::processors::create_dummy_item()
            } else {
                databend_common_pipeline::basic::create_resize_item(1, local_output_parallelism)
            });
        } else {
            items.push(create_packet_writer_item(streams.next().unwrap(), false));
        }
    }
    debug_assert!(streams.next().is_none());
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

/// Build OutboundChannels for broadcast exchange using PingPongExchange.
pub(super) fn build_broadcast_outbound_channels(
    params: &BroadcastExchangeParams,
    local_outbound_channels: Vec<Arc<dyn OutboundChannel>>,
    compression: Option<databend_common_settings::FlightCompression>,
    new_flight: bool,
) -> Result<SharedOutboundChannels> {
    let query_id = &params.query_id;
    let exchange_id = &params.exchange_id;
    let exchange_manager = DataExchangeManager::instance();

    if new_flight {
        let mut pending =
            exchange_manager.take_new_flight_fragment_outbounds(query_id, exchange_id)?;
        let mut remote_outbounds = Vec::with_capacity(pending.len());
        for (target_id, _) in &params.destination_channels {
            if target_id != &params.executor_id {
                remote_outbounds.push(pending.remove(target_id).ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "New Flight outbound not found for target {}",
                        target_id
                    ))
                })?);
            }
        }

        let streams = remote_outbounds
            .into_iter()
            .map(start_reliable_outbound)
            .collect::<Vec<_>>();
        let num_producers = local_outbound_channels.len();
        let local_channel = RoundRobinChannel::create(local_outbound_channels);
        let mut remote_idx = 0;
        let mut channels = Vec::with_capacity(params.destination_channels.len());
        for (target_id, threads) in &params.destination_channels {
            if target_id == &params.executor_id {
                channels.push(local_channel.clone());
                continue;
            }

            let mut remote_channels = Vec::with_capacity(threads.len());
            for thread_idx in 0..threads.len() {
                remote_channels.push(RemoteOutboundChannel::create(
                    thread_idx,
                    streams[remote_idx].clone(),
                    compression,
                )?);
            }
            channels.push(RoundRobinChannel::create(remote_channels));
            remote_idx += 1;
        }
        return Ok(SharedOutboundChannels::reliable(
            channels,
            streams,
            num_producers,
        ));
    }

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
            remote_channels.push(RemoteOutboundChannel::create(
                thread_idx,
                shared_buffer.destination(remote_idx),
                compression,
            )?);
        }

        channels.push(RoundRobinChannel::create(remote_channels));
        remote_idx += 1;
    }

    Ok(SharedOutboundChannels::immediate(channels))
}

/// Build per-thread OutboundChannels for hash exchange.
pub(super) fn build_hash_outbound_channels(
    params: &GlobalExchangeParams,
    mut local_outbound_channels: Vec<Arc<dyn OutboundChannel>>,
    compression: Option<databend_common_settings::FlightCompression>,
    new_flight: bool,
) -> Result<SharedOutboundChannels> {
    let num_threads = local_outbound_channels.len();
    let query_id = &params.query_id;
    let exchange_id = &params.exchange_id;
    let exchange_manager = DataExchangeManager::instance();

    if new_flight {
        let mut pending =
            exchange_manager.take_new_flight_fragment_outbounds(query_id, exchange_id)?;
        let mut remote_outbounds = Vec::with_capacity(pending.len());
        for (target_id, _) in &params.destination_channels {
            if target_id != &params.executor_id {
                remote_outbounds.push(pending.remove(target_id).ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "New Flight outbound not found for target {}",
                        target_id
                    ))
                })?);
            }
        }

        let streams = remote_outbounds
            .into_iter()
            .map(start_reliable_outbound)
            .collect::<Vec<_>>();
        let mut remote_idx = 0;
        let mut channels = Vec::with_capacity(params.destination_channels.len() * num_threads);
        for (target_id, threads) in &params.destination_channels {
            if target_id == &params.executor_id {
                channels.extend(std::mem::take(&mut local_outbound_channels));
                continue;
            }
            for thread_idx in 0..threads.len() {
                channels.push(RemoteOutboundChannel::create(
                    thread_idx,
                    streams[remote_idx].clone(),
                    compression,
                )?);
            }
            remote_idx += 1;
        }
        return Ok(SharedOutboundChannels::reliable(
            channels,
            streams,
            num_threads,
        ));
    }

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

    let config = ExchangeBufferConfig::default();
    let shared_buffer = Arc::new(ExchangeSinkBuffer::create(
        exchanges_seq,
        config,
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
            channels.push(RemoteOutboundChannel::create(
                t_idx,
                shared_buffer.destination(remote_idx),
                compression,
            )?);
        }

        remote_idx += 1;
    }

    Ok(SharedOutboundChannels::immediate(channels))
}
