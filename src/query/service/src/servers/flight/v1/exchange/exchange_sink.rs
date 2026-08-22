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

use std::collections::HashMap;
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

use super::exchange_params::BroadcastExchangeParams;
use super::exchange_params::ExchangeParams;
use super::exchange_params::GlobalExchangeParams;
use super::exchange_sink_writer::create_writer_item;
use super::exchange_sorting::ExchangeSorting;
use super::exchange_sorting::TransformExchangeSorting;
use super::exchange_transform_shuffle::exchange_shuffle;
use super::hash_send_sink::HashSendSink;
use super::serde::ExchangeSerializeMeta;
use crate::clusters::ClusterHelper;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::network::OutboundChannel;
use crate::servers::flight::v1::network::PingPongExchange;
use crate::servers::flight::v1::network::RemoteChannel;
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

        let compression = ctx.get_settings().get_query_flight_compression()?;
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
        let remote_outbound = build_hash_outbound_channels(params, local_outbound, compression)?;

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
) -> Result<Vec<Arc<dyn OutboundChannel>>> {
    let query_id = &params.query_id;
    let exchange_id = &params.exchange_id;
    let exchange_manager = DataExchangeManager::instance();

    let exchanges = exchange_manager.take_ping_pong_exchanges(query_id, exchange_id)?;
    let exchanges_seq = take_broadcast_remote_exchanges(
        exchange_id,
        &params.executor_id,
        &params.destination_channels,
        exchanges,
    )?;

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

fn take_broadcast_remote_exchanges(
    exchange_id: &str,
    executor_id: &str,
    destination_channels: &[(String, Vec<String>)],
    mut exchanges: HashMap<String, PingPongExchange>,
) -> Result<Vec<PingPongExchange>> {
    let mut exchanges_seq = Vec::with_capacity(exchanges.len());

    for (target_id, threads) in destination_channels {
        if target_id == executor_id {
            continue;
        }

        let exchange = exchanges.remove(target_id.as_str()).ok_or_else(|| {
            ErrorCode::Internal(format!(
                "PingPongExchange not found for target {}",
                target_id
            ))
        })?;
        if threads.len() != exchange.num_threads {
            return Err(ErrorCode::Internal(format!(
                "Broadcast exchange {} target {} has {} channels, but the connection has {}",
                exchange_id,
                target_id,
                threads.len(),
                exchange.num_threads
            )));
        }
        exchanges_seq.push(exchange);
    }

    if !exchanges.is_empty() {
        let mut unexpected_targets = exchanges.into_keys().collect::<Vec<_>>();
        unexpected_targets.sort();
        return Err(ErrorCode::Internal(format!(
            "Broadcast exchange {} has unexpected connection targets: {}",
            exchange_id,
            unexpected_targets.join(", ")
        )));
    }

    Ok(exchanges_seq)
}

/// Build per-thread OutboundChannels for hash exchange.
pub(super) fn build_hash_outbound_channels(
    params: &GlobalExchangeParams,
    mut local_outbound_channels: Vec<Arc<dyn OutboundChannel>>,
    compression: Option<databend_common_settings::FlightCompression>,
) -> Result<Vec<Arc<dyn OutboundChannel>>> {
    let num_threads = local_outbound_channels.len();
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
            channels.push(RemoteChannel::create(
                remote_idx,
                t_idx,
                shared_buffer.clone(),
                compression,
            )?);
        }

        remote_idx += 1;
    }

    Ok(channels)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_flight::FlightData;
    use tonic::Status;

    use super::take_broadcast_remote_exchanges;
    use crate::servers::flight::v1::network::PingPongExchange;

    fn ping_pong_exchange(num_threads: usize, remote: &str) -> PingPongExchange {
        let (send_tx, _send_rx) = async_channel::bounded(1);
        PingPongExchange::from_stream(
            num_threads,
            send_tx,
            futures::stream::empty::<std::result::Result<FlightData, Status>>(),
            "node-a",
            remote,
        )
    }

    #[test]
    fn test_broadcast_sender_rejects_unexpected_connection_target() {
        let destinations = vec![
            ("node-a".to_string(), vec!["a-0".to_string()]),
            ("node-b".to_string(), vec!["b-0".to_string()]),
        ];
        let exchanges = HashMap::from([
            ("node-b".to_string(), ping_pong_exchange(1, "node-b")),
            ("node-c".to_string(), ping_pong_exchange(1, "node-c")),
        ]);

        let error =
            match take_broadcast_remote_exchanges("exchange", "node-a", &destinations, exchanges) {
                Ok(_) => panic!("an unexpected connection target must fail"),
                Err(error) => error,
            };
        assert!(
            error
                .to_string()
                .contains("unexpected connection targets: node-c")
        );
    }

    #[test]
    fn test_broadcast_sender_rejects_connection_parallelism_mismatch() {
        let destinations = vec![
            ("node-a".to_string(), vec!["a-0".to_string()]),
            ("node-b".to_string(), vec!["b-0".to_string()]),
        ];
        let exchanges = HashMap::from([("node-b".to_string(), ping_pong_exchange(2, "node-b"))]);

        let error =
            match take_broadcast_remote_exchanges("exchange", "node-a", &destinations, exchanges) {
                Ok(_) => panic!("connection and plan parallelism must match"),
                Err(error) => error,
            };
        assert!(
            error
                .to_string()
                .contains("has 1 channels, but the connection has 2")
        );
    }
}
