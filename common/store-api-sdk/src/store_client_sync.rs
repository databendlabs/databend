//  Copyright 2021 Datafuse Labs.
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
//

use std::convert::TryInto;
use std::time::Duration;

use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_arrow::arrow_flight::Action;
use common_arrow::arrow_flight::BasicAuth;
use common_arrow::arrow_flight::HandshakeRequest;
use common_exception::ErrorCode;
use common_exception::Result;
use common_runtime::Runtime;
use common_store_api::util::STORE_RUNTIME;
use common_tracing::tracing;
use futures::stream;
use futures::StreamExt;
use log::info;
use prost::Message;
use serde::de::DeserializeOwned;
use tonic::codegen::InterceptedService;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::Request;

use crate::common::flight_result_to_str;
use crate::store_client_conf::StoreClientConf;
use crate::store_do_action::RequestFor;
use crate::store_do_action::StoreDoAction;
use crate::ConnectionFactory;
use crate::RpcClientTlsConfig;
use crate::StoreClient;

impl StoreClient {
    pub fn try_new_sync(conf: &StoreClientConf) -> Result<StoreClient> {
        let cfg = conf.clone();
        STORE_RUNTIME.block_on(StoreClient::try_new(cfg), None)?
    }

    #[tracing::instrument(level = "debug", skip(self, runtime, v))]
    pub(crate) fn do_action_sync<T, R>(&self, runtime: &Runtime, v: T) -> Result<R>
    where
        T: RequestFor<Reply = R>,
        T: Into<StoreDoAction>,
        T: Send + 'static,
        R: DeserializeOwned + Send + 'static,
    {
        let client = self.clone();
        runtime.block_on(async move { client.do_action(v).await }, None)?
    }
}
