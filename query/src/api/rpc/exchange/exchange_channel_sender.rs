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

use std::collections::HashMap;
use std::sync::Arc;
use async_channel::{Recv, RecvError, Sender};
use futures_util::future::Either;
use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_base::base::{Runtime, TrySpawn};
use common_base::base::tokio::sync::futures::Notified;
use common_base::base::tokio::sync::Notify;
use common_base::base::tokio::task::JoinHandle;
use common_base::infallible::Mutex;
use crate::api::rpc::packet::DataPacket;
use common_exception::{ErrorCode, Result};
use common_grpc::ConnectionFactory;
use crate::api::{FlightClient, PrepareChannel};
use crate::api::rpc::exchange::exchange_channel::FragmentSender;
use crate::Config;
use crate::sessions::QueryContext;

pub struct ExchangeSender {
    query_id: String,
    runtime: Arc<Runtime>,
    ctx: Arc<QueryContext>,
    target_fragments: Vec<usize>,
    notifies: HashMap<usize, Arc<Notify>>,
    join_handlers: Mutex<Vec<JoinHandle<()>>>,
    flight_client: Mutex<Option<FlightClient>>,
}

impl ExchangeSender {
    pub async fn create(config: Config, packet: &PrepareChannel, target: &str) -> Result<ExchangeSender> {
        let target_node_info = &packet.target_nodes_info[target];
        let address = target_node_info.flight_address.to_string();
        let flight_client = Self::create_client(config, &address).await?;
        let target_fragments_id = &packet.target_2_fragments[target];

        let mut target_fragments_notifies = HashMap::with_capacity(target_fragments_id.len());

        for target_fragment_id in target_fragments_id {
            target_fragments_notifies.insert(
                *target_fragment_id,
                Arc::new(Notify::new()),
            );
        }

        unimplemented!()
        // Self::create_client(config, channel.)
        // Ok(ExchangeSender {})
    }

    pub fn is_to_request_server(&self) -> bool {
        false
    }

    pub fn listen(&self, source: String) -> Result<HashMap<usize, FragmentSender>> {
        let flight_tx = self.listen_flight(&source)?;
        let mut target_fragments_senders = HashMap::with_capacity(self.target_fragments.len());

        for target_fragment_id in &self.target_fragments {
            let sender = self.listen_fragment(flight_tx.clone(), *target_fragment_id)?;
            target_fragments_senders.insert(
                *target_fragment_id,
                FragmentSender::create_unrecorded(sender),
            );
        }

        // TODO: send other type data if need

        Ok(target_fragments_senders)
    }

    fn listen_fragment(&self, c_tx: Sender<DataPacket>, fragment_id: usize) -> Result<Sender<DataPacket>> {
        let mut join_handlers = self.join_handlers.lock();

        let notify = self.notifies[&fragment_id].clone();
        let (f_tx, f_rx) = async_channel::bounded(1);

        join_handlers.push(self.runtime.spawn(async {
            let mut notified = notify.notified();

            'fragment_loop: loop {
                match futures::future::select(notified, f_rx.recv()).await {
                    Either::Left((_, _)) => { break 'fragment_loop; }
                    Either::Right((recv_message, n)) => {
                        notified = n;

                        if let Ok(recv_packet) = recv_message {
                            if let Err(_cause) = c_tx.send(recv_packet).await {
                                // common_tracing::tracing::warn!(
                                //     "{} to {} channel closed.",
                                //     source_clone,
                                //     target_clone
                                // );

                                break 'fragment_loop;
                            }

                            continue 'fragment_loop;
                        }

                        // Disconnect channel, exit loop
                        if let Err(_cause) = c_tx.send(DataPacket::EndFragment(fragment_id)).await {
                            // common_tracing::tracing::warn!(
                            //     "{} to {} channel closed.",
                            //     source_clone,
                            //     target_clone
                            // );
                        }

                        break 'fragment_loop;
                    }
                }
            }
        }));

        Ok(f_tx)
    }

    pub async fn send_error(&self, error: ErrorCode) {
        // self.
        // request_server_tx.send(DataPacket::ErrorCode(cause))
    }

    fn listen_flight(&self, source: &String) -> Result<Sender<DataPacket>> {
        let ctx = self.ctx.clone();
        let query_id = self.query_id.clone();
        let mut connection = self.get_flight_client()?;

        let (flight_tx, rx) = async_channel::bounded(2);

        self.runtime.spawn(async {
            if let Err(status) = connection.do_put(&query_id, &source, rx).await {
                common_tracing::tracing::warn!("Flight connection failure: {:?}", status);

                // Shutdown all query fragments executor and report error to request server.
                let exchange_manager = ctx.get_exchange_manager();
                if let Err(cause) = exchange_manager.shutdown_query(&query_id, Some(status)) {
                    common_tracing::tracing::warn!("Cannot shutdown query, cause {:?}", cause);
                }
            }
        });

        Ok(flight_tx)
    }

    fn get_flight_client(self: &Arc<Self>) -> Result<FlightClient> {
        match self.flight_client.lock().take() {
            Some(flight_client) => Ok(flight_client),
            None => Err(ErrorCode::LogicalError("Cannot get flight client. It's a bug.")),
        }
    }

    async fn create_client(config: Config, address: &str) -> Result<FlightClient> {
        return match config.tls_query_cli_enabled() {
            true => Ok(FlightClient::new(FlightServiceClient::new(
                ConnectionFactory::create_rpc_channel(
                    address.to_owned(),
                    None,
                    Some(config.query.to_rpc_client_tls_config()),
                )
                    .await?,
            ))),
            false => Ok(FlightClient::new(FlightServiceClient::new(
                ConnectionFactory::create_rpc_channel(address.to_owned(), None, None).await?,
            ))),
        };
    }
}
