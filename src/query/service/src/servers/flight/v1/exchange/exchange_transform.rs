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
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_transforms::processors::create_dummy_item;

use super::exchange_params::ExchangeParams;
use super::exchange_sink_writer::create_writer_item;
use super::exchange_source::via_exchange_source;
use super::exchange_source_reader::create_reader_item;
use super::exchange_transform_shuffle::exchange_shuffle;
use crate::clusters::ClusterHelper;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateDeserializer;
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
                exchange_shuffle(ctx, params, pipeline)?;

                // exchange writer sink and resize and exchange reader
                let len = params.destination_ids.len();
                let max_threads = ctx.get_settings().get_max_threads()? as usize;

                let mut items = Vec::with_capacity(len);
                let exchange_params = ExchangeParams::ShuffleExchange(params.clone());
                let exchange_manager = ctx.get_exchange_manager();
                let flight_senders = exchange_manager.get_flight_sender(&exchange_params)?;

                let senders = flight_senders.into_iter();
                for (destination_id, sender) in params.destination_ids.iter().zip(senders) {
                    items.push(match destination_id == &params.executor_id {
                        true => create_dummy_item(),
                        false => create_writer_item(
                            sender,
                            false,
                            destination_id,
                            params.fragment_id,
                            &ctx.get_cluster().local_id(),
                        ),
                    });
                }

                let receivers = exchange_manager.get_flight_receiver(&exchange_params)?;
                let nodes_source = receivers.len();
                let mut idx = 1;
                let mut reorder = vec![0_usize; nodes_source];

                for (index, (destination_id, receiver)) in receivers.into_iter().enumerate() {
                    if destination_id == params.executor_id {
                        reorder[0] = index;
                        continue;
                    }

                    reorder[idx] = index;
                    idx += 1;
                    items.push(create_reader_item(
                        receiver,
                        &destination_id,
                        &params.executor_id,
                        params.fragment_id,
                    ));
                }

                pipeline.add_pipe(Pipe::create(len, nodes_source, items));

                match params.enable_multiway_sort {
                    true => pipeline.reorder_inputs(reorder),
                    false => pipeline.try_resize(max_threads)?,
                };

                pipeline.add_transform(|input, output| {
                    TransformAggregateDeserializer::try_create(input, output, &params.schema)
                })
            }
        }
    }
}
