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
use databend_common_pipeline::basic::create_resize_item;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline_transforms::processors::create_dummy_item;

use super::exchange_params::BroadcastExchangeParams;
use super::exchange_params::ExchangeParams;
use super::exchange_sink::build_broadcast_outbound_channels;
use super::exchange_sink_writer::create_writer_item;
use super::exchange_source::via_exchange_source;
use super::exchange_source_reader::create_reader_item;
use super::exchange_transform_shuffle::exchange_shuffle;
use super::thread_channel_reader::ThreadChannelReader;
use super::thread_channel_writer::ThreadChannelWriter;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::exchange::ShuffleExchangeParams;
use crate::servers::flight::v1::network::create_local_channels;
use crate::servers::flight::v1::scatter::BroadcastFlightScatter;
use crate::servers::flight::v1::scatter::FlightScatter;
use crate::sessions::QueryContext;

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
            ExchangeParams::GlobalShuffleExchange(_params) => Ok(()),
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
        let exchange_params = ExchangeParams::NodeShuffleExchange(params.clone());
        let exchange_manager = ctx.get_exchange_manager();
        let flight_senders = exchange_manager.get_flight_sender(&exchange_params)?;

        for (destination_id, sender) in flight_senders {
            items.push(match destination_id == params.executor_id {
                true => {
                    if local_pipe == 1 {
                        create_dummy_item()
                    } else {
                        create_resize_item(1, local_pipe)
                    }
                }
                false => create_writer_item(sender, false),
            });
        }

        let mut nodes_source = 0;
        let receivers = exchange_manager.get_flight_receiver(&exchange_params)?;
        for receiver in receivers {
            nodes_source += 1;
            items.push(create_reader_item(receiver));
        }

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
        let num_destinations = params.destination_channels.len();
        let mut local_pos = 0;
        let mut local_threads = 0;

        for (idx, (dest, threads)) in params.destination_channels.iter().enumerate() {
            if dest == &params.executor_id {
                local_pos = idx;
                local_threads = threads.len();
            }
        }

        let scatter: Arc<Box<dyn FlightScatter>> = Arc::new(Box::new(
            BroadcastFlightScatter::try_create(num_destinations)?,
        ));

        let compression = ctx.get_settings().get_query_flight_compression()?;

        let max_bytes = 20 * 1024 * 1024;
        let (local_outbound, local_inbound) = create_local_channels(local_threads, max_bytes);
        let channels = build_broadcast_outbound_channels(params, local_outbound, compression)?;

        let output_len = pipeline.output_len();
        let mut output_items = Vec::with_capacity(output_len);

        for _idx in 0..output_len {
            output_items.push(ThreadChannelWriter::create_item(
                channels.clone(),
                local_pos,
                scatter.clone(),
                4096,
                4 * 1024 * 1024,
            ));
        }

        pipeline.add_pipe(Pipe::create(output_len, output_len, output_items));

        let query_id = &params.query_id;
        let exchange_id = &params.exchange_id;
        let exchange_manager = DataExchangeManager::instance();
        let channels = exchange_manager.get_exchange_source_channel(query_id, exchange_id)?;

        let local_threads = local_inbound.len();

        assert_eq!(channels.len(), output_len);
        assert_eq!(local_threads, output_len);

        let mut items = Vec::with_capacity(output_len);
        for (remote, local) in channels.into_iter().zip(local_inbound.into_iter()) {
            items.push(ThreadChannelReader::create_item(vec![remote, local]));
        }

        pipeline.add_pipe(Pipe::create(output_len, output_len, items));
        Ok(())
    }
}
