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

// use std::sync::Arc;
// use async_channel::Sender;
// use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
// use common_base::base::{Runtime, TrySpawn};
// use crate::api::rpc::packet::DataPacket;
// use common_exception::Result;
// use common_grpc::ConnectionFactory;
// use crate::api::FlightClient;
// use crate::Config;
// use crate::sessions::QueryContext;
//
// pub struct ExchangeSender {
//     runtime: Arc<Runtime>,
//     ctx: Arc<QueryContext>,
//
//     config: Config,
//     address: String,
//     query_id: String,
// }
//
// impl ExchangeSender {
//     pub fn listen(self: &Arc<Self>, source: String) -> Result<Sender<DataPacket>> {
//         let ctx = self.ctx.clone();
//         let config = self.config.clone();
//         let address = self.address.clone();
//         let query_id = self.query_id.clone();
//         let (tx, rx) = async_channel::bounded(2);
//
//         self.runtime.spawn(async {
//             let mut connection = Self::create_client(config, &address).await?;
//
//             if let Err(status) = connection.do_put(&query_id, &source, rx).await {
//                 common_tracing::tracing::warn!("Flight connection failure: {:?}", status);
//
//                 // Shutdown all query fragments executor and report error to request server.
//                 let exchange_manager = ctx.get_exchange_manager();
//                 if let Err(cause) = exchange_manager.shutdown_query(&query_id, Some(status)) {
//                     common_tracing::tracing::warn!("Cannot shutdown query, cause {:?}", cause);
//                 }
//             }
//
//             Ok(())
//         });
//
//         unimplemented!()
//     }
//
//     async fn create_client(config: Config, address: &str) -> Result<FlightClient> {
//         return match config.tls_query_cli_enabled() {
//             true => Ok(FlightClient::new(FlightServiceClient::new(
//                 ConnectionFactory::create_rpc_channel(
//                     address.to_owned(),
//                     None,
//                     Some(config.query.to_rpc_client_tls_config()),
//                 )
//                     .await?,
//             ))),
//             false => Ok(FlightClient::new(FlightServiceClient::new(
//                 ConnectionFactory::create_rpc_channel(address.to_owned(), None, None).await?,
//             ))),
//         };
//     }
// }
