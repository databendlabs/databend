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
use super::hash_send_transform::HashSendTransform;
use crate::servers::flight::v1::exchange::BroadcastRecvTransform;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::exchange::ShuffleExchangeParams;
use crate::servers::flight::v1::exchange::exchange_sink::build_broadcast_outbound_channels;
use crate::servers::flight::v1::exchange::exchange_sink::build_hash_outbound_channels;
use crate::servers::flight::v1::exchange::exchange_sink::build_legacy_packet_sinks;
use crate::servers::flight::v1::exchange::exchange_sink::build_node_shuffle_packet_sinks;
use crate::servers::flight::v1::exchange::local_channel::create_local_channels;
use crate::servers::flight::v1::scatter::HashFlightScatter;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

pub struct ExchangeTransform;

impl ExchangeTransform {
    pub fn via(
        ctx: &Arc<QueryContext>,
        params: &ExchangeParams,
        pipeline: &mut Pipeline,
        injector: Arc<dyn ExchangeInjector>,
    ) -> Result<()> {
        match params {
            ExchangeParams::MergeExchange(params) => {
                via_exchange_source(ctx.clone(), params, injector, pipeline)
            }
            ExchangeParams::BroadcastExchange(params) => {
                Self::broadcast_exchange(ctx, pipeline, params)
            }
            ExchangeParams::NodeShuffleExchange(params) => {
                Self::node_shuffle(ctx, pipeline, injector, params)
            }
            ExchangeParams::GlobalShuffleExchange(params) => {
                Self::hash_exchange(ctx, pipeline, params)
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

        // exchange writer sink and resize and exchange reader
        let len = params.destination_ids.len();
        let local_pipe = if params.allow_adjust_parallelism
            && params.exchange_injector.exchange_sorting().is_none()
        {
            ctx.get_settings().get_max_threads()? as usize
        } else {
            1
        };

        let mut items = Vec::with_capacity(len);
        let exchange_manager = ctx.get_exchange_manager();
        let new_flight = ctx.get_settings().get_enable_experiment_new_flight()?;

        let nodes_source = if new_flight {
            items.extend(build_node_shuffle_packet_sinks(
                ctx, params, pipeline, local_pipe,
            )?);

            let exchange_params = ExchangeParams::NodeShuffleExchange(params.clone());
            let receivers = exchange_manager.take_packet_receivers(&exchange_params)?;
            let nodes_source = receivers.len();
            items.extend(receivers.into_iter().map(create_reader_item));
            nodes_source
        } else {
            let exchange_params = ExchangeParams::NodeShuffleExchange(params.clone());
            let streams = exchange_manager
                .take_fragment_outbound_streams(&exchange_params)?
                .into_iter()
                .map(|(_, stream)| stream)
                .collect();
            items.extend(build_legacy_packet_sinks(streams, false, || {
                if local_pipe == 1 {
                    create_dummy_item()
                } else {
                    create_resize_item(1, local_pipe)
                }
            }));

            let receivers = exchange_manager.take_packet_receivers(&exchange_params)?;
            let nodes_source = receivers.len();
            items.extend(receivers.into_iter().map(create_reader_item));
            nodes_source
        };

        let new_outputs = local_pipe + nodes_source;
        pipeline.add_pipe(Pipe::create(len, new_outputs, items));

        if params.exchange_injector.exchange_sorting().is_none() && params.allow_adjust_parallelism
        {
            pipeline.try_resize(ctx.get_settings().get_max_threads()? as usize)?;
        }

        injector.apply_shuffle_deserializer(params, pipeline)
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
        let new_flight = ctx.get_settings().get_enable_experiment_new_flight()?;
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

        assert_eq!(channel_set.receivers.len(), local_threads);

        let local_outbound = create_local_channels(&channel_set);
        let channels =
            build_broadcast_outbound_channels(params, local_outbound, compression, new_flight)?;
        channels.install_failure_handler(pipeline);

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
        for idx in 0..channel_set.receivers.len() {
            items.push(BroadcastRecvTransform::create_item(
                idx,
                channel_set.create_receiver(idx, &params.schema),
                waker.clone(),
            ));
        }

        pipeline.add_pipe(Pipe::create(local_threads, local_threads, items));
        Ok(())
    }

    fn hash_exchange(
        ctx: &Arc<QueryContext>,
        pipeline: &mut Pipeline,
        params: &GlobalExchangeParams,
    ) -> Result<()> {
        let mut local_pos = 0;
        let mut local_threads = 0;

        for (dest, threads) in params.destination_channels.iter() {
            if dest == &params.executor_id {
                local_threads = threads.len();
                break;
            }

            local_pos += threads.len();
        }

        let waker = pipeline.get_waker();
        let compression = ctx.get_settings().get_query_flight_compression()?;
        let new_flight = ctx.get_settings().get_enable_experiment_new_flight()?;
        let rows_threshold = ctx.get_settings().get_hash_shuffle_rows_threshold()?;
        let bytes_threshold = ctx.get_settings().get_hash_shuffle_bytes_threshold()?;

        pipeline.resize(local_threads, false)?;

        let query_id = &params.query_id;
        let exchange_id = &params.exchange_id;
        let exchange_manager = DataExchangeManager::instance();

        let channel_set = exchange_manager.get_or_create_exchange_channel_set(
            query_id,
            exchange_id,
            local_threads,
        )?;
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
            items.push(HashSendTransform::create_item(
                idx,
                local_pos + idx,
                scatter.clone(),
                remote_outbound.clone(),
                waker.clone(),
                rows_threshold,
                bytes_threshold,
            ));
        }

        pipeline.add_pipe(Pipe::create(local_threads, local_threads, items));

        let mut items = Vec::with_capacity(local_threads);
        for idx in 0..channel_set.receivers.len() {
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
