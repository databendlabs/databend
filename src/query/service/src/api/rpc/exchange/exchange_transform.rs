// Copyright 2022 Datafuse Labs.
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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::processors::create_resize_item;
use common_pipeline_core::Pipeline;

use crate::api::rpc::exchange::exchange_params::ExchangeParams;
use crate::api::rpc::exchange::exchange_sink_writer::create_writer_item;
use crate::api::rpc::exchange::exchange_source::via_exchange_source;
use crate::api::rpc::exchange::exchange_source_reader::create_reader_item;
use crate::api::rpc::exchange::exchange_transform_shuffle::exchange_shuffle;
use crate::api::rpc::exchange::serde::exchange_deserializer::create_deserializer_items;
use crate::api::rpc::exchange::serde::exchange_serializer::create_serializer_item;
use crate::pipelines::processors::create_dummy_item;
use crate::pipelines::processors::create_dummy_items;
use crate::sessions::QueryContext;

pub struct ExchangeTransform;

impl ExchangeTransform {
    pub fn via(
        ctx: &Arc<QueryContext>,
        params: &ExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        match params {
            ExchangeParams::MergeExchange(params) => {
                via_exchange_source(ctx.clone(), params, pipeline)
            }
            ExchangeParams::ShuffleExchange(params) => {
                exchange_shuffle(params, pipeline)?;

                // exchange serialize transform
                let len = params.destination_ids.len();
                let mut items = Vec::with_capacity(len);
                for destination_id in &params.destination_ids {
                    items.push(match destination_id == &params.executor_id {
                        true => create_dummy_item(),
                        false => create_serializer_item(&params.schema),
                    });
                }

                pipeline.add_pipe(Pipe::create(len, len, items));

                // exchange writer sink and resize and exchange reader
                let len = params.destination_ids.len();
                let max_threads = ctx.get_settings().get_max_threads()? as usize;

                let mut items = Vec::with_capacity(len);
                let exchange_params = ExchangeParams::ShuffleExchange(params.clone());
                let exchange_manager = ctx.get_exchange_manager();
                let flight_exchanges = exchange_manager.get_flight_exchanges(&exchange_params)?;

                let exchanges = flight_exchanges.iter().cloned();
                for (destination_id, exchange) in params.destination_ids.iter().zip(exchanges) {
                    items.push(match destination_id == &params.executor_id {
                        true if max_threads == 1 => create_dummy_item(),
                        true => create_resize_item(1, max_threads),
                        false => create_writer_item(exchange),
                    });
                }

                let mut nodes_source = 0;
                let exchanges = flight_exchanges.into_iter();
                for (destination_id, exchange) in params.destination_ids.iter().zip(exchanges) {
                    if destination_id != &params.executor_id {
                        nodes_source += 1;
                        items.push(create_reader_item(exchange));
                    }
                }

                let new_outputs = max_threads + nodes_source;
                pipeline.add_pipe(Pipe::create(len, new_outputs, items));

                let mut items = create_dummy_items(max_threads, new_outputs);
                items.extend(create_deserializer_items(nodes_source, &params.schema));
                pipeline.add_pipe(Pipe::create(new_outputs, new_outputs, items));

                Ok(())
            }
        }
    }
}
