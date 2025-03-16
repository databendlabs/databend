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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::Pipeline;

use super::exchange_params::ExchangeParams;
use super::exchange_sink_writer::create_writer_item;
use super::exchange_transform_shuffle::exchange_shuffle;
use crate::clusters::ClusterHelper;
use crate::pipelines::processors::transforms::aggregator::FlightExchange;
use crate::servers::flight::v1::scatter::MergeFlightScatter;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ExchangeSink;

impl ExchangeSink {
    pub fn via(
        ctx: &Arc<QueryContext>,
        params: &ExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let exchange_manager = ctx.get_exchange_manager();
        let mut senders = exchange_manager.get_flight_sender(params)?;

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

                let settings = ctx.get_settings();
                let compression = settings.get_query_flight_compression()?;

                let nodes = vec![];
                match params.enable_multiway_sort {
                    true => pipeline.exchange(
                        1,
                        FlightExchange::<true>::create(
                            nodes,
                            compression,
                            Arc::new(Box::new(MergeFlightScatter)),
                        ),
                    ),
                    false => pipeline.exchange(
                        1,
                        FlightExchange::<false>::create(
                            nodes,
                            compression,
                            Arc::new(Box::new(MergeFlightScatter)),
                        ),
                    ),
                };

                assert_eq!(senders.len(), 1);
                pipeline.add_pipe(Pipe::create(1, 0, vec![create_writer_item(
                    senders.remove(0),
                    params.ignore_exchange,
                    &params.destination_id,
                    params.fragment_id,
                    &ctx.get_cluster().local_id(),
                )]));
                Ok(())
            }
            ExchangeParams::ShuffleExchange(params) => {
                exchange_shuffle(ctx, params, pipeline)?;

                // exchange writer sink
                let len = pipeline.output_len();
                let mut items = Vec::with_capacity(senders.len());

                for (destination_id, sender) in params.destination_ids.iter().zip(senders) {
                    items.push(create_writer_item(
                        sender,
                        false,
                        destination_id,
                        params.fragment_id,
                        &ctx.get_cluster().local_id(),
                    ));
                }

                pipeline.add_pipe(Pipe::create(len, 0, items));
                Ok(())
            }
        }
    }
}
