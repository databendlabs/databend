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

use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline::basic::create_resize_item;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline_transforms::processors::create_dummy_item;

use super::broadcast_recv_transform::ExchangeRecvTransform;
use super::broadcast_send_transform::BroadcastSendTransform;
use super::exchange_params::BroadcastExchangeParams;
use super::exchange_params::ExchangeParams;
use super::exchange_params::GlobalExchangeParams;
use super::exchange_source::via_exchange_source;
use super::exchange_source_reader::create_reader_item;
use super::exchange_transform_shuffle::exchange_shuffle;
use super::partition_send_transform::PartitionSendTransform;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::exchange::ShuffleExchangeParams;
use crate::servers::flight::v1::exchange::exchange_sink::build_broadcast_outbound_channels;
use crate::servers::flight::v1::exchange::exchange_sink::build_global_shuffle_outbound_channels;
use crate::servers::flight::v1::exchange::exchange_sink_writer::create_writer_item;
use crate::servers::flight::v1::network::NetworkInboundChannelSet;
use crate::servers::flight::v1::network::create_local_channels;
use crate::servers::flight::v1::partition::PartitionStream;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

pub struct ExchangeTransform;

impl ExchangeTransform {
    pub fn local_shuffle(
        pipeline: &mut Pipeline,
        schema: &DataSchemaRef,
        partition_streams: Vec<Box<dyn PartitionStream>>,
        parallelism: usize,
    ) -> Result<()> {
        if partition_streams.len() != parallelism {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Local shuffle expected {parallelism} partition streams, got {}",
                partition_streams.len()
            )));
        }
        pipeline.resize(parallelism, false)?;

        let channel_set = NetworkInboundChannelSet::new(parallelism);
        let channels = create_local_channels(&channel_set);
        let waker = pipeline.get_waker();

        let mut send_items = Vec::with_capacity(parallelism);
        for (worker_id, partition_stream) in partition_streams.into_iter().enumerate() {
            send_items.push(PartitionSendTransform::create_item(
                worker_id,
                worker_id,
                partition_stream,
                channels.clone(),
                waker.clone(),
            ));
        }
        pipeline.add_pipe(Pipe::create(parallelism, parallelism, send_items));

        let mut recv_items = Vec::with_capacity(parallelism);
        for worker_id in 0..parallelism {
            recv_items.push(ExchangeRecvTransform::create_item(
                worker_id,
                channel_set.create_receiver(worker_id, schema),
                waker.clone(),
            ));
        }
        pipeline.add_pipe(Pipe::create(parallelism, parallelism, recv_items));
        Ok(())
    }

    pub fn via(
        ctx: &Arc<QueryContext>,
        params: &mut ExchangeParams,
        pipeline: &mut Pipeline,
        injector: Arc<dyn ExchangeInjector>,
    ) -> Result<()> {
        match params {
            ExchangeParams::MergeExchange(params) => {
                via_exchange_source(ctx.clone(), params, pipeline)
            }
            ExchangeParams::BroadcastExchange(params) => {
                Self::broadcast_exchange(ctx, pipeline, params)
            }
            ExchangeParams::NodeShuffleExchange(params) => {
                Self::node_shuffle(ctx, pipeline, injector, params)
            }
            ExchangeParams::GlobalShuffleExchange(params) => {
                Self::global_shuffle(ctx, pipeline, params)
            }
        }
    }

    fn node_shuffle(
        ctx: &Arc<QueryContext>,
        pipeline: &mut Pipeline,
        injector: Arc<dyn ExchangeInjector>,
        params: &ShuffleExchangeParams,
    ) -> Result<()> {
        exchange_shuffle(ctx, params, pipeline)?;

        let len = params.destination_ids.len();
        let local_pipe = if params.allow_adjust_parallelism {
            ctx.get_settings().get_max_threads()? as usize
        } else {
            1
        };

        let mut items = Vec::with_capacity(len);
        let exchange_params = ExchangeParams::NodeShuffleExchange(params.clone());
        let exchange_manager = ctx.get_exchange_manager();
        let flight_senders = exchange_manager.get_flight_sender(&exchange_params)?;

        for (destination_id, sender) in flight_senders {
            items.push(if destination_id == params.executor_id {
                if local_pipe == 1 {
                    create_dummy_item()
                } else {
                    create_resize_item(1, local_pipe)
                }
            } else {
                create_writer_item(sender, false)
            });
        }

        let mut nodes_source = 0;
        let receivers = exchange_manager.get_flight_receiver(&exchange_params)?;
        for receiver in receivers {
            nodes_source += 1;
            items.push(create_reader_item(receiver));
        }

        pipeline.add_pipe(Pipe::create(len, local_pipe + nodes_source, items));

        if params.allow_adjust_parallelism {
            pipeline.try_resize(ctx.get_settings().get_max_threads()? as usize)?;
        }

        injector.apply_shuffle_deserializer(params, pipeline)
    }

    fn global_shuffle(
        ctx: &Arc<QueryContext>,
        pipeline: &mut Pipeline,
        params: &mut GlobalExchangeParams,
    ) -> Result<()> {
        let mut local_pos = 0;
        let mut local_threads = None;
        for (destination, channels) in &params.destination_channels {
            if destination == &params.executor_id {
                local_threads = Some(channels.len());
                break;
            }
            local_pos += channels.len();
        }
        let local_threads = local_threads.ok_or_else(|| {
            databend_common_exception::ErrorCode::Internal(
                "Global shuffle has no local destination",
            )
        })?;

        pipeline.resize(local_threads, false)?;

        let exchange_manager = DataExchangeManager::instance();
        let channel_set = exchange_manager.get_or_create_exchange_channel_set(
            &params.query_id,
            &params.exchange_id,
            local_threads,
        )?;
        if channel_set.channels.len() != local_threads {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Global shuffle channel count mismatch: expected {local_threads}, got {}",
                channel_set.channels.len()
            )));
        }

        let compression = ctx.get_settings().get_query_flight_compression()?;
        let local_outbound = create_local_channels(&channel_set);
        let channels = build_global_shuffle_outbound_channels(params, local_outbound, compression)?;
        let waker = pipeline.get_waker();

        let mut send_items = Vec::with_capacity(local_threads);
        let partition_streams = params.take_partition_streams(local_threads)?;
        for (worker_id, partition_stream) in partition_streams.into_iter().enumerate() {
            send_items.push(PartitionSendTransform::create_item(
                worker_id,
                local_pos + worker_id,
                partition_stream,
                channels.clone(),
                waker.clone(),
            ));
        }
        pipeline.add_pipe(Pipe::create(local_threads, local_threads, send_items));

        let mut recv_items = Vec::with_capacity(local_threads);
        for worker_id in 0..local_threads {
            recv_items.push(ExchangeRecvTransform::create_item(
                worker_id,
                channel_set.create_receiver_with_codec(
                    worker_id,
                    &params.schema,
                    params.codec.clone(),
                ),
                waker.clone(),
            ));
        }
        pipeline.add_pipe(Pipe::create(local_threads, local_threads, recv_items));
        Ok(())
    }

    fn broadcast_exchange(
        ctx: &Arc<QueryContext>,
        pipeline: &mut Pipeline,
        params: &BroadcastExchangeParams,
    ) -> Result<()> {
        let mut local_pos = 0;
        let mut local_threads = 0;

        for (idx, (dest, threads)) in params.destination_channels.iter().enumerate() {
            if dest == &params.executor_id {
                local_pos = idx;
                local_threads = threads.len();
            }
        }

        let compression = ctx.get_settings().get_query_flight_compression()?;
        let waker = pipeline.get_waker();

        pipeline.resize(local_threads, false)?;

        let query_id = &params.query_id;
        let exchange_id = &params.exchange_id;
        let exchange_manager = DataExchangeManager::instance();

        let channel_set = exchange_manager.get_or_create_exchange_channel_set(
            query_id,
            exchange_id,
            local_threads,
        )?;

        assert_eq!(channel_set.channels.len(), local_threads);

        let local_outbound = create_local_channels(&channel_set);
        let channels = build_broadcast_outbound_channels(params, local_outbound, compression)?;

        let mut items = Vec::with_capacity(local_threads);

        for idx in 0..local_threads {
            items.push(BroadcastSendTransform::create_item(
                idx,
                local_pos,
                channels.clone(),
                waker.clone(),
            ));
        }

        pipeline.add_pipe(Pipe::create(local_threads, local_threads, items));

        let mut items = Vec::with_capacity(local_threads);
        for idx in 0..channel_set.channels.len() {
            items.push(ExchangeRecvTransform::create_item(
                idx,
                channel_set.create_receiver(idx, &params.schema),
                waker.clone(),
            ));
        }

        pipeline.add_pipe(Pipe::create(local_threads, local_threads, items));
        Ok(())
    }
}
