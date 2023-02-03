//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoPtr;
use common_pipeline_core::Pipeline;
use serde::Deserializer;
use serde::Serializer;

use crate::api::rpc::exchange::exchange_params::ExchangeParams;
use crate::api::rpc::exchange::exchange_params::MergeExchangeParams;
use crate::api::rpc::exchange::exchange_source_deserializer;
use crate::api::rpc::exchange::exchange_source_reader;
use crate::api::DataPacket;
use crate::clusters::ClusterHelper;
use crate::sessions::QueryContext;

pub struct ExchangeSourceMeta {
    pub packet: Option<DataPacket>,
}

impl ExchangeSourceMeta {
    pub fn create(packet: DataPacket) -> BlockMetaInfoPtr {
        Box::new(ExchangeSourceMeta {
            packet: Some(packet),
        })
    }
}

impl Debug for ExchangeSourceMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExchangeSourceMeta").finish()
    }
}

impl serde::Serialize for ExchangeSourceMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        unimplemented!("Unimplemented serialize ExchangeSourceMeta")
    }
}

impl<'de> serde::Deserialize<'de> for ExchangeSourceMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        unimplemented!("Unimplemented deserialize ExchangeSourceMeta")
    }
}

#[typetag::serde(name = "exchange_source")]
impl BlockMetaInfo for ExchangeSourceMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals ExchangeSourceMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone ExchangeSourceMeta")
    }
}

pub fn via_exchange_source(
    ctx: Arc<QueryContext>,
    params: &MergeExchangeParams,
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
    let flight_exchanges = exchange_manager.get_flight_exchanges(&exchange_params)?;

    let last_output_len = pipeline.output_len();
    let flight_exchanges_len = flight_exchanges.len();
    exchange_source_reader::via_reader(last_output_len, flight_exchanges, pipeline);
    exchange_source_deserializer::via_deserializer(
        last_output_len,
        flight_exchanges_len,
        params,
        pipeline,
    );

    Ok(())
}
