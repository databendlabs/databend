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
use std::time::Duration;
use common_base::base::tokio::time::sleep;
use async_channel::{Recv, RecvError, Sender};
use futures_util::future::Either;
use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_base::base::{Runtime, TrySpawn};
use common_base::base::tokio::sync::futures::Notified;
use common_base::base::tokio::sync::Notify;
use common_base::base::tokio::task::JoinHandle;
use common_base::infallible::Mutex;
use crate::api::rpc::packets::DataPacket;
use common_exception::{ErrorCode, Result};
use common_grpc::ConnectionFactory;
use crate::api::{FlightClient, InitNodesChannelPacket};
use crate::api::rpc::exchange::exchange_channel::FragmentSender;
use crate::Config;
use crate::sessions::QueryContext;

pub struct ExchangeSender {
    query_id: String,
    target_fragments: Vec<usize>,
    request_server_notify: Arc<Notify>,
    notifies: HashMap<usize, Arc<Notify>>,
    join_handlers: Mutex<Vec<JoinHandle<()>>>,
    flight_client: Mutex<Option<FlightClient>>,
    target_is_request_server: bool,
}

impl ExchangeSender {
    pub async fn create(config: Config, packet: &InitNodesChannelPacket, target: &str) -> Result<ExchangeSender> {
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

        Ok(ExchangeSender {
            query_id: packet.query_id.clone(),
            notifies: target_fragments_notifies,
            target_fragments: target_fragments_id.to_owned(),
            join_handlers: Mutex::new(vec![]),
            flight_client: Mutex::new(Some(flight_client)),
            target_is_request_server: target == packet.request_server,
            request_server_notify: Arc::new(Notify::new()),
        })
    }

    pub fn is_to_request_server(&self) -> bool {
        self.target_is_request_server
    }

    pub fn listen(&self, ctx: Arc<QueryContext>, source: String, runtime: Arc<Runtime>) -> Result<(Sender<DataPacket>, HashMap<usize, FragmentSender>)> {
        let flight_tx = self.listen_flight(ctx.clone(), &source, runtime.clone())?;
        let mut target_fragments_senders = HashMap::with_capacity(self.target_fragments.len());

        for target_fragment_id in &self.target_fragments {
            let runtime = runtime.clone();
            let sender = self.listen_fragment(flight_tx.clone(), *target_fragment_id, runtime)?;
            target_fragments_senders.insert(
                *target_fragment_id,
                FragmentSender::create_unrecorded(sender),
            );
        }

        if self.target_is_request_server {
            let flight_tx = flight_tx.clone();
            let request_notify = self.request_server_notify.clone();
            runtime.spawn(async move {
                let mut notify = Box::pin(request_notify.notified());
                'listen_loop: loop {
                    println!("send progress");
                    if let Err(cause) = flight_tx.send(DataPacket::Progress(ctx.get_scan_progress_value())).await {
                        println!("send progress failure");
                        // TODO: report error and shutdown query
                        break 'listen_loop;
                    }

                    let sleep = Box::pin(sleep(Duration::from_millis(500)));
                    match futures::future::select(notify, sleep).await {
                        Either::Left((_, _)) => { break 'listen_loop; }
                        Either::Right((_, y)) => { notify = y; }
                    }
                }
            });
        }

        Ok((flight_tx, target_fragments_senders))
    }

    fn listen_fragment(&self, c_tx: Sender<DataPacket>, fragment_id: usize, runtime: Arc<Runtime>) -> Result<Sender<DataPacket>> {
        let mut join_handlers = self.join_handlers.lock();

        let notify = self.notifies[&fragment_id].clone();
        let (f_tx, f_rx) = async_channel::bounded(1);

        join_handlers.push(runtime.spawn(async move {
            let mut notified = Box::pin(notify.notified());

            'fragment_loop: loop {
                match futures::future::select(notified, f_rx.recv()).await {
                    Either::Left((_, _)) => { break 'fragment_loop; }
                    Either::Right((recv_message, n)) => {
                        notified = n;

                        if let Ok(recv_packet) = recv_message {
                            if c_tx.send(recv_packet).await.is_err() {
                                break 'fragment_loop;
                            }

                            continue 'fragment_loop;
                        }

                        // Disconnect channel, exit loop
                        c_tx.send(DataPacket::EndFragment(fragment_id)).await;
                        break 'fragment_loop;
                    }
                }
            }

            // TODO: if join handler is end, shutdown query.
        }));

        Ok(f_tx)
    }

    pub async fn abort(&self) -> Result<()> {
        for notify in self.notifies.values() {
            notify.notify_one();
        }

        while let Some(join_handler) = self.join_handlers.lock().pop() {
            if let Err(join_cause) = join_handler.await {
                if join_cause.is_panic() {
                    let cause = join_cause.into_panic();
                    return match cause.downcast_ref::<&'static str>() {
                        None => match cause.downcast_ref::<String>() {
                            None => Err(ErrorCode::PanicError("Sorry, unknown panic message")),
                            Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                        },
                        Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                    };
                }
            }
        }

        Ok(())
    }

    fn listen_flight(&self, ctx: Arc<QueryContext>, source: &String, runtime: Arc<Runtime>) -> Result<Sender<DataPacket>> {
        let source = source.clone();
        let query_id = self.query_id.clone();
        let mut connection = self.get_flight_client()?;

        let (flight_tx, rx) = async_channel::bounded(2);

        let mut join_handlers = self.join_handlers.lock();
        join_handlers.push(runtime.spawn(async move {
            if let Err(status) = connection.do_put(&query_id, &source, rx).await {
                common_tracing::tracing::warn!("Flight connection failure: {:?}", status);

                // Shutdown all query fragments executor and report error to request server.
                let exchange_manager = ctx.get_exchange_manager();
                if let Err(cause) = exchange_manager.shutdown_query(&query_id, Some(status)) {
                    common_tracing::tracing::warn!("Cannot shutdown query, cause {:?}", cause);
                }
            }
        }));

        Ok(flight_tx)
    }

    fn get_flight_client(&self) -> Result<FlightClient> {
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
