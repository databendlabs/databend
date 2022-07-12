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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use async_channel::Sender;
use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_base::base::tokio::task::JoinHandle;
use common_base::base::tokio::time::sleep;
use common_base::base::Runtime;
use common_base::base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_grpc::ConnectionFactory;
use futures_util::future::Either;
use parking_lot::Mutex;

use crate::api::rpc::exchange::exchange_channel::FragmentSender;
use crate::api::rpc::packets::DataPacket;
use crate::api::rpc::packets::FragmentData;
use crate::api::rpc::packets::PrecommitBlock;
use crate::api::rpc::packets::ProgressInfo;
use crate::api::FlightClient;
use crate::api::InitNodesChannelPacket;
use crate::sessions::QueryContext;
use crate::Config;

pub struct ExchangeSender {
    config: Config,
    query_id: String,
    target_address: String,
    target_fragments: Vec<usize>,
    target_fragments_finished: HashMap<usize, Arc<AtomicBool>>,
    join_handlers: Mutex<Vec<JoinHandle<()>>>,
    flight_client: Arc<Mutex<Option<FlightClient>>>,
    target_is_request_server: bool,
}

impl ExchangeSender {
    pub async fn create(
        config: Config,
        packet: &InitNodesChannelPacket,
        target: &str,
    ) -> Result<ExchangeSender> {
        let target_node_info = &packet.target_nodes_info[target];
        let target_address = target_node_info.flight_address.to_string();
        let target_fragments_id = &packet.target_2_fragments[target];

        let mut target_fragments_finished = HashMap::with_capacity(target_fragments_id.len());

        for target_fragment_id in target_fragments_id {
            target_fragments_finished.insert(*target_fragment_id, Arc::new(AtomicBool::new(false)));
        }

        Ok(ExchangeSender {
            config,
            target_address,
            target_fragments_finished,
            query_id: packet.query_id.clone(),
            target_fragments: target_fragments_id.to_owned(),
            join_handlers: Mutex::new(vec![]),
            flight_client: Arc::new(Mutex::new(None)),
            target_is_request_server: target == packet.request_server,
        })
    }

    pub fn connect_flight(&mut self, runtime: &Arc<Runtime>) -> Result<()> {
        let config = self.config.clone();
        let target_address = self.target_address.clone();
        let flight_client = self.flight_client.clone();
        let connect_res_future = runtime.spawn(async move {
            let connection = Self::create_client(config, &target_address).await?;
            let mut flight_client = flight_client.lock();
            *flight_client = Some(connection);
            Ok(())
        });

        futures::executor::block_on(async move {
            match connect_res_future.await {
                Ok(res) => res,
                Err(join_error) => match join_error.is_panic() {
                    false => Err(ErrorCode::TokioError("Cancel tokio task.")),
                    true => {
                        let cause = join_error.into_panic();
                        match cause.downcast_ref::<&'static str>() {
                            None => match cause.downcast_ref::<String>() {
                                None => Err(ErrorCode::PanicError("Sorry, unknown panic message")),
                                Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                            },
                            Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                        }
                    }
                },
            }
        })
    }

    pub fn is_to_request_server(&self) -> bool {
        self.target_is_request_server
    }

    pub fn listen(
        &self,
        ctx: Arc<QueryContext>,
        source: String,
        runtime: Arc<Runtime>,
    ) -> Result<(Sender<DataPacket>, HashMap<usize, FragmentSender>)> {
        let flight_tx = self.listen_flight(ctx.clone(), &source, runtime.clone())?;
        let mut target_fragments_senders = HashMap::with_capacity(self.target_fragments.len());

        for target_fragment_id in &self.target_fragments {
            let runtime = runtime.clone();
            let sender =
                self.listen_fragment(ctx.clone(), flight_tx.clone(), *target_fragment_id, runtime)?;
            target_fragments_senders.insert(
                *target_fragment_id,
                FragmentSender::create_unrecorded(sender),
            );
        }

        Ok((flight_tx, target_fragments_senders))
    }

    fn listen_fragment(
        &self,
        ctx: Arc<QueryContext>,
        c_tx: Sender<DataPacket>,
        fragment_id: usize,
        runtime: Arc<Runtime>,
    ) -> Result<Sender<DataPacket>> {
        let mut join_handlers = self.join_handlers.lock();

        let to_request_server = self.is_to_request_server();
        let is_finished = self.target_fragments_finished[&fragment_id].clone();
        let (f_tx, f_rx) = async_channel::bounded(1);

        join_handlers.push(runtime.spawn(async move {
            // flight connect is closed if c_tx is closed.
            'fragment_loop: while !is_finished.load(Ordering::Relaxed) && !c_tx.is_closed() {
                let sleep_future = Box::pin(sleep(Duration::from_millis(500)));

                match futures::future::select(sleep_future, f_rx.recv()).await {
                    Either::Left((_, _)) => {
                        if to_request_server {
                            ExchangeSender::send_progress_if_need(&ctx, &c_tx).await;
                            ExchangeSender::send_precommit_if_need(&ctx, &c_tx).await;
                        }
                    }
                    Either::Right((recv_message, _)) => {
                        if let Ok(recv_packet) = recv_message {
                            if c_tx.send(recv_packet).await.is_err() {
                                return;
                            }

                            continue 'fragment_loop;
                        }

                        break 'fragment_loop;
                    }
                };
            }

            while let Ok(recv_message) = f_rx.try_recv() {
                if c_tx.send(recv_message).await.is_err() {
                    return;
                }
            }

            // Disconnect channel, exit loop
            if to_request_server {
                ExchangeSender::send_progress_if_need(&ctx, &c_tx).await;
                ExchangeSender::send_precommit_if_need(&ctx, &c_tx).await;
            }

            let fragment_end = FragmentData::End(fragment_id);
            c_tx.send(DataPacket::FragmentData(fragment_end)).await.ok();
        }));

        Ok(f_tx)
    }

    async fn send_progress_if_need(ctx: &Arc<QueryContext>, c_tx: &Sender<DataPacket>) {
        let scan_progress = ctx.get_scan_progress();
        let scan_progress_values = scan_progress.fetch();

        if scan_progress_values.rows != 0 || scan_progress_values.bytes != 0 {
            let progress = ProgressInfo::ScanProgress(scan_progress_values);
            if c_tx.send(DataPacket::Progress(progress)).await.is_err() {
                common_tracing::tracing::warn!(
                    "Send scan progress values error, because flight connection is closed."
                );
                return;
            }
        }

        let write_progress = ctx.get_write_progress();
        let write_progress_values = write_progress.fetch();

        if write_progress_values.rows != 0 || write_progress_values.bytes != 0 {
            let progress = ProgressInfo::WriteProgress(write_progress_values);
            if c_tx.send(DataPacket::Progress(progress)).await.is_err() {
                common_tracing::tracing::warn!(
                    "Send write progress values error, because flight connection is closed."
                );
                return;
            }
        }

        let result_progress = ctx.get_result_progress();
        let result_progress_values = result_progress.fetch();

        if result_progress_values.rows != 0 || result_progress_values.bytes != 0 {
            let progress = ProgressInfo::ResultProgress(result_progress_values);
            if c_tx.send(DataPacket::Progress(progress)).await.is_err() {
                common_tracing::tracing::warn!(
                    "Send result progress values error, because flight connection is closed."
                );
            }
        }
    }

    async fn send_precommit_if_need(ctx: &Arc<QueryContext>, c_tx: &Sender<DataPacket>) {
        let precommit_blocks = ctx.consume_precommit_blocks();

        if !precommit_blocks.is_empty() {
            for precommit_block in precommit_blocks {
                if !precommit_block.is_empty() {
                    let precommit_block = PrecommitBlock(precommit_block);
                    if c_tx
                        .send(DataPacket::PrecommitBlock(precommit_block))
                        .await
                        .is_err()
                    {
                        common_tracing::tracing::warn!(
                            "Send precommit block error, because flight connection is closed."
                        );
                        return;
                    }
                }
            }
        }
    }

    pub fn abort(&self) {
        for is_finished in self.target_fragments_finished.values() {
            is_finished.store(true, Ordering::SeqCst);
        }
    }

    pub async fn join(&self) -> Result<()> {
        while let Some(join_handler) = self.pop_join_handler() {
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

    fn pop_join_handler(&self) -> Option<JoinHandle<()>> {
        self.join_handlers.lock().pop()
    }

    fn listen_flight(
        &self,
        ctx: Arc<QueryContext>,
        source: &str,
        runtime: Arc<Runtime>,
    ) -> Result<Sender<DataPacket>> {
        let source = source.to_owned();
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
            None => Err(ErrorCode::LogicalError(
                "Cannot get flight client. It's a bug.",
            )),
        }
    }

    async fn create_client(config: Config, address: &str) -> Result<FlightClient> {
        match config.tls_query_cli_enabled() {
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
        }
    }
}
