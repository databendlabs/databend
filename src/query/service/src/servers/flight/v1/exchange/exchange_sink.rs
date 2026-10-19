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

use arrow_schema::Schema as ArrowSchema;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_settings::Settings;

use super::exchange_params::BroadcastExchangeParams;
use super::exchange_params::ExchangeParams;
use super::exchange_params::GlobalExchangeParams;
use super::exchange_sink_writer::create_writer_item;
use super::exchange_sorting::ExchangeSorting;
use super::exchange_sorting::TransformExchangeSorting;
use super::exchange_transform_shuffle::exchange_shuffle;
use super::hash_send_sink::HashSendSink;
use super::hash_shuffle_writer::ShuffleNode;
use super::hash_shuffle_writer::ShuffleTarget;
use super::hash_shuffle_writer::hash_shuffle_send_budget;
use super::serde::ExchangeSerializeMeta;
use crate::clusters::ClusterHelper;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::network::OutboundChannel;
use crate::servers::flight::v1::network::RemoteChannel;
use crate::servers::flight::v1::network::RemoteNodeChannel;
use crate::servers::flight::v1::network::RoundRobinChannel;
use crate::servers::flight::v1::network::create_local_channels;
use crate::servers::flight::v1::network::outbound_buffer::ExchangeBufferConfig;
use crate::servers::flight::v1::network::outbound_buffer::ExchangeSinkBuffer;
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

                // exchange writer sink
                let len = pipeline.output_len();

                let items = senders
                    .into_iter()
                    .map(|(_, sender)| create_writer_item(sender, false))
                    .collect::<Vec<_>>();

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

        let settings = ctx.get_settings();
        let compression = settings.get_query_flight_compression()?;
        let buffer_config = exchange_buffer_config(&settings)?;
        let budget = hash_shuffle_send_budget(
            local_threads,
            settings.get_hash_shuffle_send_buffer_bytes()?,
        );
        let waker = pipeline.get_waker();

        pipeline.resize(local_threads, false)?;

        let query_id = &params.query_id;
        let exchange_id = &params.exchange_id;
        let exchange_manager = DataExchangeManager::instance();

        let channel_set = exchange_manager.get_exchange_channel_set(query_id, exchange_id)?;
        assert_eq!(channel_set.channels.len(), local_threads);

        let local_outbound = create_local_channels(&channel_set);
        let nodes = build_hash_shuffle_nodes(params, local_outbound, compression, buffer_config)?;
        let num_partitions = nodes.last().map(|x| x.partitions.end).unwrap_or_default();

        let scatter = Arc::new(HashFlightScatter::try_create(
            ctx.get_function_context()?,
            params.shuffle_keys.clone(),
            num_partitions,
            local_pos,
        )?);

        let mut items = Vec::with_capacity(local_threads);
        for idx in 0..local_threads {
            items.push(HashSendSink::create_item(
                idx,
                scatter.clone(),
                nodes.clone(),
                waker.clone(),
                budget,
            ));
        }

        pipeline.add_pipe(Pipe::create(local_threads, 0, items));
        Ok(())
    }
}

/// Send queue configuration of ping-pong exchanges.
pub(super) fn exchange_buffer_config(settings: &Settings) -> Result<ExchangeBufferConfig> {
    Ok(ExchangeBufferConfig {
        max_queued_bytes: settings.get_exchange_send_queue_bytes()?,
        max_batch_bytes: settings.get_exchange_frame_bytes()?,
    })
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
    config: ExchangeBufferConfig,
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

    // Create shared ExchangeSinkBuffer: one RemoteInstance per PingPong
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

/// Build the shuffle destinations of a hash exchange, one entry per node in
/// `destination_channels` order. Each node owns a contiguous range of partitions,
/// one per thread.
pub(super) fn build_hash_shuffle_nodes(
    params: &GlobalExchangeParams,
    local_outbound_channels: Vec<Arc<dyn OutboundChannel>>,
    compression: Option<databend_common_settings::FlightCompression>,
    config: ExchangeBufferConfig,
) -> Result<Vec<ShuffleNode>> {
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

    let shared_buffer = Arc::new(ExchangeSinkBuffer::create(
        exchanges_seq,
        config,
        &GlobalIORuntime::instance(),
    )?);

    let arrow_schema = Arc::new(ArrowSchema::from(params.schema.as_ref()));
    let mut remote_idx = 0;
    let mut partition_start = 0;
    let mut local_outbound_channels = Some(local_outbound_channels);
    let mut nodes = Vec::with_capacity(params.destination_channels.len());

    for (target_id, threads) in &params.destination_channels {
        let partitions = partition_start..partition_start + threads.len();
        partition_start = partitions.end;

        let target = match target_id == &params.executor_id {
            true => {
                let channels = local_outbound_channels.take().ok_or_else(|| {
                    ErrorCode::Internal("Local node appears twice in hash exchange destinations")
                })?;
                assert_eq!(channels.len(), threads.len());
                ShuffleTarget::Local(channels.into_iter().map(Some).collect())
            }
            false => {
                let channel = RemoteNodeChannel::create(
                    remote_idx,
                    shared_buffer.clone(),
                    compression,
                    arrow_schema.clone(),
                )?;
                remote_idx += 1;
                ShuffleTarget::Remote(Some(channel))
            }
        };

        nodes.push(ShuffleNode { partitions, target });
    }

    Ok(nodes)
}
