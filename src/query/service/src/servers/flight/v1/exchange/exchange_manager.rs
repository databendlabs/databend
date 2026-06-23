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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::FlightData;
use async_channel::Receiver;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::ExecutorStatsSnapshot;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_settings::FlightKeepAliveParams;
use log::warn;
use parking_lot::Mutex;
use tonic::Status;

use super::exchange_params::ExchangeParams;
use super::query_handle::QueryExchangeRegistry;
use super::query_handle::QueryHandle;
use super::query_handle::QueryRuntime;
use super::query_handle::create_flight_client;
use crate::clusters::ClusterHelper;
use crate::clusters::FlightParams;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::QueryFragmentsActions;
use crate::servers::flight::FlightClient;
use crate::servers::flight::FlightReceiver;
use crate::servers::flight::FlightSender;
use crate::servers::flight::v1::actions::INIT_QUERY_FRAGMENTS;
use crate::servers::flight::v1::actions::START_PREPARED_QUERY;
use crate::servers::flight::v1::actions::init_query_fragments;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::network::NetworkInboundChannelSet;
use crate::servers::flight::v1::network::NetworkInboundSender;
use crate::servers::flight::v1::network::PingPongExchange;
use crate::servers::flight::v1::packets::QueryEnv;
use crate::servers::flight::v1::packets::QueryFragments;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextSettings;

pub struct DataExchangeManager {
    queries: QueriesCoordinator,
}

pub(crate) struct QueriesCoordinator {
    queries: Mutex<HashMap<String, Arc<QueryHandle>>>,
}

impl DataExchangeManager {
    pub fn init() -> Result<()> {
        GlobalInstance::set(Arc::new(DataExchangeManager {
            queries: QueriesCoordinator::create(),
        }));

        Ok(())
    }

    pub fn instance() -> Arc<DataExchangeManager> {
        GlobalInstance::get()
    }

    pub fn get_running_query_graph_dump(&self, query_id: &str) -> Result<String> {
        let Ok(handle) = self.queries.get_query(query_id) else {
            return Ok(format!("Unknown query {}", query_id));
        };

        Ok(match handle.runtime.running_executor() {
            None => format!("Unknown running query {}", query_id),
            Some(executor) => executor.get_inner().format_graph_nodes(),
        })
    }

    pub fn get_query_execution_stats(&self) -> Vec<(String, ExecutorStatsSnapshot)> {
        self.queries
            .handles()
            .into_iter()
            .filter_map(|handle| {
                handle.runtime.running_executor().map(|executor| {
                    (
                        handle.query_id.clone(),
                        executor.get_inner().get_query_execution_stats(),
                    )
                })
            })
            .collect()
    }

    pub fn get_query_ctx(&self, query_id: &str) -> Result<Arc<QueryContext>> {
        self.queries.get_query(query_id)?.runtime.query_ctx()
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn init_query_env(
        &self,
        env: &QueryEnv,
        ctx: Option<Arc<QueryContext>>,
    ) -> Result<()> {
        let handle = self.queries.create_query(env.query_id.clone())?;
        let remove_leak_query_worker = ctx.as_ref().map(|_| {
            let query_id = env.query_id.clone();
            GlobalIORuntime::instance().spawn(async move {
                let _ = tokio::time::sleep(Duration::from_secs(180)).await;
                DataExchangeManager::instance().remove_if_leak_query(query_id);
            })
        });

        handle
            .runtime
            .init_env(env, ctx, &handle.exchanges, remove_leak_query_worker)
            .await
    }

    #[async_backtrace::framed]
    pub async fn create_client(
        address: &str,
        use_current_rt: bool,
        keep_alive: FlightKeepAliveParams,
    ) -> Result<FlightClient> {
        create_flight_client(address.to_string(), use_current_rt, keep_alive).await
    }

    pub fn set_ctx(&self, query_id: &str, ctx: Arc<QueryContext>) -> Result<()> {
        let handle = self.queries.create_query(query_id.to_string())?;
        handle.runtime.set_ctx(ctx, None)
    }

    #[fastrace::trace]
    pub fn execute_partial_query(&self, query_id: &str) -> Result<()> {
        let handle = self.queries.get_query(query_id)?;
        handle.runtime.execute(&handle.exchanges)
    }

    #[fastrace::trace]
    pub fn init_query_fragments_plan(&self, fragments: &QueryFragments) -> Result<()> {
        let handle = self.queries.get_query(&fragments.query_id)?;
        handle.runtime.prepare_fragments(fragments)?;
        Ok(())
    }

    #[fastrace::trace]
    pub fn handle_statistics_exchange(
        &self,
        id: String,
        target: String,
    ) -> Result<Receiver<std::result::Result<FlightData, Status>>> {
        self.queries
            .create_query(id)?
            .exchanges
            .add_statistics_exchange(target)
    }

    #[fastrace::trace]
    pub fn handle_exchange_fragment(
        &self,
        query: String,
        channel_id: String,
    ) -> Result<Receiver<std::result::Result<FlightData, Status>>> {
        self.queries
            .create_query(query)?
            .exchanges
            .register_flight_channel_sender(channel_id)
    }

    #[fastrace::trace]
    pub fn handle_do_exchange(
        &self,
        query_id: &str,
        channel_id: &str,
        num_threads: usize,
    ) -> Result<NetworkInboundSender> {
        self.queries
            .create_query(query_id.to_string())?
            .exchanges
            .create_inbound_sender(channel_id, num_threads)
    }

    pub fn get_exchange_channel_set(
        &self,
        query_id: &str,
        channel_id: &str,
    ) -> Result<Arc<NetworkInboundChannelSet>> {
        self.queries
            .get_query(query_id)?
            .exchanges
            .get_exchange_channel_set(channel_id)
    }

    pub fn take_ping_pong_exchanges(
        &self,
        query_id: &str,
        channel_id: &str,
    ) -> Result<HashMap<String, PingPongExchange>> {
        Ok(self
            .queries
            .get_query(query_id)?
            .exchanges
            .take_ping_pong_exchanges(channel_id))
    }

    pub fn shutdown_query(&self, query_id: &str, cause: Option<ErrorCode>) {
        if let Ok(handle) = self.queries.get_query(query_id) {
            handle.runtime.shutdown(cause.clone());
        }
    }

    #[fastrace::trace]
    pub fn on_finished_query(&self, query_id: &str, cause: Option<ErrorCode>) {
        if let Some(handle) = self.queries.remove_query(query_id) {
            handle.runtime.shutdown(cause.clone());
        }
    }

    #[fastrace::trace]
    pub async fn commit_actions(
        &self,
        ctx: Arc<QueryContext>,
        actions: QueryFragmentsActions,
    ) -> Result<PipelineBuildResult> {
        let handle = self.queries.create_query(ctx.get_id())?;
        let settings = ctx.get_settings();
        let flight_params = FlightParams {
            timeout: settings.get_flight_client_timeout()?,
            retry_times: settings.get_flight_max_retry_times()?,
            retry_interval: settings.get_flight_retry_interval()?,
            keep_alive: settings.get_flight_keep_alive_params()?,
        };
        let mut root_fragment_ids = actions.get_root_fragment_ids()?;

        let query_env = actions.get_query_env()?;
        query_env.init(&ctx, flight_params).await?;

        let cluster = ctx.get_cluster();
        let mut query_fragments = actions.get_query_fragments()?;
        let local_fragments = query_fragments.remove(
            &databend_common_config::GlobalConfig::instance()
                .query
                .node_id,
        );

        let _: HashMap<String, ()> = cluster
            .do_action(INIT_QUERY_FRAGMENTS, query_fragments, flight_params)
            .await?;

        self.set_ctx(&ctx.get_id(), ctx.clone())?;
        if let Some(query_fragments) = local_fragments {
            init_query_fragments(query_fragments).await?;
        }

        let main_fragment_id = root_fragment_ids.pop().unwrap();
        let build_res = handle.runtime.root_pipeline(
            ctx,
            &handle.exchanges,
            main_fragment_id,
            root_fragment_ids,
        )?;

        let prepared_query = actions.prepared_query()?;
        let _: HashMap<String, ()> = cluster
            .do_action(START_PREPARED_QUERY, prepared_query, flight_params)
            .await?;

        Ok(build_res)
    }

    pub fn get_flight_sender(
        &self,
        params: &ExchangeParams,
    ) -> Result<Vec<(String, FlightSender)>> {
        self.queries
            .get_query(&params.get_query_id())?
            .exchanges
            .take_flight_sender(params)
    }

    pub fn get_flight_receiver(&self, params: &ExchangeParams) -> Result<Vec<FlightReceiver>> {
        self.queries
            .get_query(&params.get_query_id())?
            .exchanges
            .take_flight_receiver(params)
    }

    pub fn get_fragment_source(
        &self,
        query_id: &str,
        fragment_id: usize,
        injector: Arc<dyn ExchangeInjector>,
    ) -> Result<PipelineBuildResult> {
        let handle = self.queries.get_query(query_id)?;
        let ctx = handle.runtime.query_ctx()?;
        handle
            .runtime
            .subscribe_fragment(&ctx, fragment_id, injector)
    }

    pub(crate) fn remove_if_leak_query(&self, query_id: String) {
        let should_remove = match self.queries.get_query(&query_id) {
            Ok(handle) => !handle.runtime.is_started(),
            Err(_) => false,
        };

        if should_remove {
            warn!(
                "Query {} cannot start command while in 180 seconds",
                query_id
            );
            self.on_finished_query(
                &query_id,
                Some(ErrorCode::Internal(format!(
                    "Query {} cannot start command while in 180 seconds",
                    query_id
                ))),
            );
        }
    }
}

impl QueriesCoordinator {
    pub(crate) fn create() -> Self {
        QueriesCoordinator {
            queries: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn create_query(&self, query_id: String) -> Result<Arc<QueryHandle>> {
        let mut queries = self.queries.lock();
        match queries.entry(query_id.clone()) {
            Entry::Occupied(v) => Ok(v.get().clone()),
            Entry::Vacant(v) => {
                let handle = Arc::new(QueryHandle {
                    query_id,
                    exchanges: QueryExchangeRegistry::create(),
                    runtime: QueryRuntime::create(),
                });
                v.insert(handle.clone());
                Ok(handle)
            }
        }
    }

    pub(crate) fn get_query(&self, query_id: &str) -> Result<Arc<QueryHandle>> {
        self.queries.lock().get(query_id).cloned().ok_or_else(|| {
            ErrorCode::Internal(format!(
                "Query {} not found in queries coordinator.",
                query_id
            ))
        })
    }

    pub(crate) fn remove_query(&self, query_id: &str) -> Option<Arc<QueryHandle>> {
        self.queries.lock().remove(query_id)
    }

    pub(crate) fn handles(&self) -> Vec<Arc<QueryHandle>> {
        self.queries.lock().values().cloned().collect()
    }
}
