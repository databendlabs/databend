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

use super::exchange_params::ExchangeParams;
use super::exchange_sink_writer::create_writer_item;
use super::exchange_source::via_exchange_source;
use super::exchange_source_reader::create_reader_item;
use super::exchange_transform_shuffle::exchange_shuffle;
use crate::servers::flight::v1::exchange::ExchangeInjector;
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
            ExchangeParams::BroadcastExchange(_params) => Ok(()),
            ExchangeParams::NodeShuffleExchange(params) => {
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

                if params.exchange_injector.exchange_sorting().is_none()
                    && params.allow_adjust_parallelism
                {
                    pipeline.try_resize(ctx.get_settings().get_max_threads()? as usize)?;
                }

                injector.apply_shuffle_deserializer(params, pipeline)
            }
        }
    }
}
