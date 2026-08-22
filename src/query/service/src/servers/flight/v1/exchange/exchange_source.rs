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
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline_transforms::processors::TransformDummy;

use super::exchange_params::ExchangeParams;
use super::exchange_params::GlobalExchangeParams;
use super::exchange_params::MergeExchangeParams;
use super::exchange_params::validate_broadcast_destinations;
use super::exchange_source_reader::ExchangeSourceReader;
use super::hash_send_source::HashSendSource;
use crate::clusters::ClusterHelper;
use crate::servers::flight::v1::exchange::BroadcastExchange;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::network::NetworkInboundChannelSet;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;

/// Add Exchange Source to the pipeline.
pub fn via_exchange_source(
    ctx: Arc<QueryContext>,
    params: &MergeExchangeParams,
    injector: Arc<dyn ExchangeInjector>,
    pipeline: &mut Pipeline,
) -> Result<()> {
    // UpstreamTransform --->  DummyTransform   --->    DummyTransform      --->  DownstreamTransform
    //      ...          --->      ...          --->        ...             --->        ...
    // UpstreamTransform --->  DummyTransform   --->    DummyTransform      --->        ...
    //                         ExchangeSource   --->  DeserializeTransform  --->        ...
    //                             ...          --->        ...             --->        ...
    //                         ExchangeSource   --->  DeserializeTransform  --->  DownstreamTransform

    if params.destination_id != ctx.get_cluster().local_id() {
        return Err(ErrorCode::Internal(format!(
            "Locally depends on merge exchange, but the localhost is not a coordination node. executor: {}, destination_id: {}, fragment id: {}",
            ctx.get_cluster().local_id(),
            params.destination_id,
            params.fragment_id
        )));
    }

    let exchange_params = ExchangeParams::MergeExchange(params.clone());
    let exchange_manager = ctx.get_exchange_manager();
    let flight_receivers = exchange_manager.get_flight_receiver(&exchange_params)?;

    let last_output_len = pipeline.output_len();
    let mut items = Vec::with_capacity(last_output_len + flight_receivers.len());

    for _index in 0..last_output_len {
        let input = InputPort::create();
        let output = OutputPort::create();

        items.push(PipeItem::create(
            TransformDummy::create(input.clone(), output.clone()),
            vec![input],
            vec![output],
        ));
    }

    for flight_exchange in flight_receivers {
        let output = OutputPort::create();
        items.push(PipeItem::create(
            ExchangeSourceReader::create(output.clone(), flight_exchange),
            vec![],
            vec![output],
        ));
    }

    pipeline.add_pipe(Pipe::create(last_output_len, items.len(), items));

    if params.allow_adjust_parallelism {
        pipeline.try_resize(last_output_len)?;
    }

    injector.apply_merge_deserializer(params, pipeline)
}

/// Add a source-only receiver pipeline for a broadcast exchange whose source
/// fragment is not scheduled on this executor.
pub fn via_broadcast_exchange_source(
    ctx: &Arc<QueryContext>,
    query_id: &str,
    schema: &DataSchemaRef,
    exchange: &BroadcastExchange,
    pipeline: &mut Pipeline,
) -> Result<()> {
    let executor_id = ctx.get_cluster().local_id();
    let local_threads = validate_broadcast_destinations(
        &exchange.id,
        &executor_id,
        &exchange.destination_ids,
        &exchange.destination_channels,
    )?
    .num_threads;

    let exchange_manager = DataExchangeManager::instance();
    let channel_set = exchange_manager.get_or_create_exchange_channel_set(
        query_id,
        &exchange.id,
        local_threads,
    )?;
    add_inbound_receiver_pipe(&channel_set, schema, pipeline);
    Ok(())
}

fn add_inbound_receiver_pipe(
    channel_set: &NetworkInboundChannelSet,
    schema: &DataSchemaRef,
    pipeline: &mut Pipeline,
) {
    let last_output_len = pipeline.output_len();
    let num_receivers = channel_set.channels.len();
    let mut items = Vec::with_capacity(last_output_len + num_receivers);

    for _ in 0..last_output_len {
        let input = InputPort::create();
        let output = OutputPort::create();
        items.push(PipeItem::create(
            TransformDummy::create(input.clone(), output.clone()),
            vec![input],
            vec![output],
        ));
    }

    let waker = pipeline.get_waker();
    for idx in 0..num_receivers {
        items.push(HashSendSource::create_item(
            idx,
            channel_set.create_receiver(idx, schema),
            waker.clone(),
        ));
    }

    pipeline.add_pipe(Pipe::create(last_output_len, items.len(), items));
}

/// Add HashSendSource receivers to the pipeline for hash exchange source-only path.
/// Existing pipeline outputs pass through as DummyTransforms; remote InboundChannels
/// are added as HashSendSource processors.
#[allow(dead_code)]
pub fn via_hash_exchange_source(
    _ctx: &Arc<QueryContext>,
    params: &GlobalExchangeParams,
    pipeline: &mut Pipeline,
) -> Result<()> {
    let query_id = &params.query_id;
    let exchange_id = &params.exchange_id;
    let exchange_manager = DataExchangeManager::instance();

    let channel_set = exchange_manager.get_exchange_channel_set(query_id, exchange_id)?;
    add_inbound_receiver_pipe(&channel_set, &params.schema, pipeline);
    Ok(())
}
