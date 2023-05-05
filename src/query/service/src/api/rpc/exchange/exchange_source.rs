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

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::Pipeline;

use crate::api::rpc::exchange::exchange_params::ExchangeParams;
use crate::api::rpc::exchange::exchange_params::MergeExchangeParams;
use crate::api::rpc::exchange::exchange_source_reader;
use crate::api::ExchangeInjector;
use crate::clusters::ClusterHelper;
use crate::sessions::QueryContext;

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
    exchange_source_reader::via_reader(last_output_len, pipeline, flight_receivers);

    pipeline.resize(last_output_len)?;
    injector.apply_merge_deserializer(params, pipeline)
}
