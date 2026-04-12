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

use super::exchange_params::BroadcastExchangeParams;
use super::exchange_params::ExchangeParams;
use super::exchange_params::GlobalExchangeParams;
use super::exchange_params::MergeExchangeParams;
use super::exchange_params::ShuffleExchangeParams;
use super::exchange_source_reader::ExchangeSourceReader;
use super::exchange_source_reader::create_reader_item;
use super::hash_send_source::HashSendSource;
use crate::clusters::ClusterHelper;
use crate::servers::flight::v1::exchange::BroadcastExchange;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::exchange::DefaultExchangeInjector;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::exchange::NodeToNodeExchange;
use crate::servers::flight::v1::scatter::BroadcastFlightScatter;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;

fn add_source_pipe(pipeline: &mut Pipeline, source_items: Vec<PipeItem>) {
    let last_output_len = pipeline.output_len();
    let mut items = Vec::with_capacity(last_output_len + source_items.len());

    for _index in 0..last_output_len {
        let input = InputPort::create();
        let output = OutputPort::create();

        items.push(PipeItem::create(
            TransformDummy::create(input.clone(), output.clone()),
            vec![input],
            vec![output],
        ));
    }

    items.extend(source_items);
    pipeline.add_pipe(Pipe::create(last_output_len, items.len(), items));
}

fn local_exchange_threads(
    executor_id: &str,
    destination_channels: &[(String, Vec<String>)],
) -> Result<usize> {
    destination_channels
        .iter()
        .find_map(|(destination, channels)| (destination == executor_id).then_some(channels.len()))
        .filter(|threads| *threads > 0)
        .ok_or_else(|| {
            ErrorCode::Internal(format!(
                "Executor {} is not a destination of exchange",
                executor_id
            ))
        })
}

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
    let mut source_items = Vec::with_capacity(flight_receivers.len());

    for flight_exchange in flight_receivers {
        let output = OutputPort::create();
        source_items.push(PipeItem::create(
            ExchangeSourceReader::create(output.clone(), flight_exchange),
            vec![],
            vec![output],
        ));
    }

    add_source_pipe(pipeline, source_items);

    if params.allow_adjust_parallelism && last_output_len > 0 {
        pipeline.try_resize(last_output_len)?;
    }

    injector.apply_merge_deserializer(params, pipeline)
}

pub fn via_hash_exchange_source(
    _ctx: &Arc<QueryContext>,
    params: &GlobalExchangeParams,
    pipeline: &mut Pipeline,
) -> Result<()> {
    let query_id = &params.query_id;
    let exchange_id = &params.exchange_id;
    let exchange_manager = DataExchangeManager::instance();
    let local_threads = local_exchange_threads(&params.executor_id, &params.destination_channels)?;

    let channel_set = exchange_manager.get_or_create_exchange_channel_set(
        query_id,
        exchange_id,
        local_threads,
    )?;
    let waker = pipeline.get_waker();

    let num_receivers = channel_set.channels.len();
    let mut source_items = Vec::with_capacity(num_receivers);

    for idx in 0..num_receivers {
        source_items.push(HashSendSource::create_item(
            idx,
            channel_set.create_receiver(idx, &params.schema),
            waker.clone(),
        ));
    }

    add_source_pipe(pipeline, source_items);
    Ok(())
}

pub fn via_broadcast_exchange_source(
    _ctx: &Arc<QueryContext>,
    params: &BroadcastExchangeParams,
    pipeline: &mut Pipeline,
) -> Result<()> {
    let exchange_manager = DataExchangeManager::instance();
    let local_threads = local_exchange_threads(&params.executor_id, &params.destination_channels)?;
    let channel_set = exchange_manager.get_or_create_exchange_channel_set(
        &params.query_id,
        &params.exchange_id,
        local_threads,
    )?;
    let waker = pipeline.get_waker();
    let mut source_items = Vec::with_capacity(channel_set.channels.len());

    for idx in 0..channel_set.channels.len() {
        source_items.push(HashSendSource::create_item(
            idx,
            channel_set.create_receiver(idx, &params.schema),
            waker.clone(),
        ));
    }

    add_source_pipe(pipeline, source_items);
    Ok(())
}

pub fn via_shuffle_exchange_source(
    _ctx: &Arc<QueryContext>,
    params: &ShuffleExchangeParams,
    injector: Arc<dyn ExchangeInjector>,
    pipeline: &mut Pipeline,
) -> Result<()> {
    let _local_channels =
        local_exchange_threads(&params.executor_id, &params.destination_channels)?;

    let exchange_manager = DataExchangeManager::instance();
    let exchange_params = ExchangeParams::NodeShuffleExchange(params.clone());
    let flight_receivers = exchange_manager.get_flight_receiver(&exchange_params)?;
    let mut source_items = Vec::with_capacity(flight_receivers.len());

    for receiver in flight_receivers {
        source_items.push(create_reader_item(receiver));
    }

    add_source_pipe(pipeline, source_items);
    injector.apply_shuffle_deserializer(params, pipeline)
}

fn create_broadcast_source_params(
    ctx: &Arc<QueryContext>,
    query_id: &str,
    schema: &DataSchemaRef,
    exchange: &BroadcastExchange,
) -> BroadcastExchangeParams {
    BroadcastExchangeParams {
        query_id: query_id.to_string(),
        executor_id: ctx.get_cluster().local_id(),
        schema: schema.clone(),
        exchange_id: exchange.id.clone(),
        destination_channels: exchange.destination_channels.clone(),
    }
}

fn create_shuffle_source_params(
    ctx: &Arc<QueryContext>,
    query_id: &str,
    fragment_id: usize,
    schema: &DataSchemaRef,
    exchange: &NodeToNodeExchange,
) -> ShuffleExchangeParams {
    ShuffleExchangeParams {
        query_id: query_id.to_string(),
        executor_id: ctx.get_cluster().local_id(),
        fragment_id,
        schema: schema.clone(),
        destination_ids: exchange.destination_ids.clone(),
        destination_channels: exchange.destination_channels.clone(),
        shuffle_scatter: Arc::new(Box::new(
            BroadcastFlightScatter::try_create(1).expect("broadcast scatter must create"),
        )),
        exchange_injector: DefaultExchangeInjector::create(),
        allow_adjust_parallelism: exchange.allow_adjust_parallelism,
    }
}

pub fn via_remote_exchange_source(
    ctx: Arc<QueryContext>,
    query_id: &str,
    fragment_id: usize,
    schema: &DataSchemaRef,
    exchange: &DataExchange,
    injector: Arc<dyn ExchangeInjector>,
    pipeline: &mut Pipeline,
) -> Result<()> {
    match exchange {
        DataExchange::Merge(exchange) => via_exchange_source(
            ctx,
            &MergeExchangeParams {
                query_id: query_id.to_string(),
                fragment_id,
                destination_id: exchange.destination_id.clone(),
                channel_id: exchange.channel_id.clone(),
                schema: schema.clone(),
                ignore_exchange: exchange.ignore_exchange,
                allow_adjust_parallelism: exchange.allow_adjust_parallelism,
                exchange_injector: DefaultExchangeInjector::create(),
            },
            injector,
            pipeline,
        ),
        DataExchange::Broadcast(exchange) => via_broadcast_exchange_source(
            &ctx,
            &create_broadcast_source_params(&ctx, query_id, schema, exchange),
            pipeline,
        ),
        DataExchange::NodeToNodeExchange(exchange) => via_shuffle_exchange_source(
            &ctx,
            &create_shuffle_source_params(&ctx, query_id, fragment_id, schema, exchange),
            injector,
            pipeline,
        ),
        DataExchange::GlobalShuffleExchange(exchange) => via_hash_exchange_source(
            &ctx,
            &GlobalExchangeParams {
                query_id: query_id.to_string(),
                executor_id: ctx.get_cluster().local_id(),
                schema: schema.clone(),
                exchange_id: exchange.id.clone(),
                shuffle_keys: exchange.shuffle_keys.clone(),
                destination_channels: exchange.destination_channels.clone(),
            },
            pipeline,
        ),
    }
}
