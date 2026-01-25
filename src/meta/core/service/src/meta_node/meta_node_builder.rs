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
use std::sync::atomic::AtomicI32;

use databend_common_meta_runtime_api::SpawnApi;
use databend_common_meta_sled_store::openraft::Config;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::MetaStartupError;
use databend_common_meta_types::raft_types::NodeId;
use log::info;
use semver::Version;
use tokio::sync::Mutex;
use tokio::sync::watch;
use watcher::dispatch::Dispatcher;

use crate::meta_node::meta_node::MetaRaft;
use crate::meta_service::MetaNode;
use crate::meta_service::runtime_config::RuntimeConfig;
use crate::meta_service::watcher::DispatcherHandle;
use crate::meta_service::watcher::WatchTypes;
use crate::network::NetworkFactory;
use crate::store::RaftStore;

pub struct MetaNodeBuilder<SP> {
    pub(crate) node_id: Option<NodeId>,
    pub(crate) raft_config: Option<Config>,
    pub(crate) sto: Option<RaftStore<SP>>,
    pub(crate) raft_service_endpoint: Option<Endpoint>,
    pub(crate) version: Option<Version>,
}

impl<SP: SpawnApi> MetaNodeBuilder<SP> {
    pub async fn build(mut self) -> Result<Arc<MetaNode<SP>>, MetaStartupError> {
        let node_id = self
            .node_id
            .ok_or_else(|| MetaStartupError::InvalidConfig(String::from("node_id is not set")))?;

        let config = self
            .raft_config
            .take()
            .ok_or_else(|| MetaStartupError::InvalidConfig(String::from("config is not set")))?;

        let sto = self
            .sto
            .take()
            .ok_or_else(|| MetaStartupError::InvalidConfig(String::from("sto is not set")))?;

        let version = self
            .version
            .ok_or_else(|| MetaStartupError::InvalidConfig(String::from("version is not set")))?;

        let net = NetworkFactory::<SP>::new(sto.clone());

        let log_store = sto.log().clone();
        let sm_store = sto.state_machine().clone();

        let raft = MetaRaft::new(node_id, Arc::new(config), net, log_store, sm_store)
            .await
            .map_err(|e| MetaStartupError::MetaServiceError(e.to_string()))?;

        let runtime_config = RuntimeConfig::default();

        let (tx, rx) = watch::channel::<()>(());

        let (dispatcher_handle, dispatcher_fut) = Dispatcher::<WatchTypes>::create();
        #[allow(unused_must_use)]
        SP::spawn(dispatcher_fut, Some("watcher-dispatcher".into()));
        let handle = DispatcherHandle::new(dispatcher_handle, node_id);
        let handle = Arc::new(handle);

        let on_change_applied = {
            let h = handle.clone();
            let broadcast = runtime_config.broadcast_state_machine_changes.clone();
            move |change| {
                if broadcast.load(std::sync::atomic::Ordering::Relaxed) {
                    h.send_change(change)
                } else {
                    info!(
                        "broadcast_state_machine_changes is disabled, ignoring change: {:?}",
                        change
                    );
                }
            }
        };

        sto.get_sm_v003()
            .set_on_change_applied(Box::new(on_change_applied));

        let meta_node = Arc::new(MetaNode {
            raft_store: sto.clone(),
            dispatcher_handle: handle,
            raft: raft.clone(),
            runtime_config,
            running_tx: tx,
            running_rx: rx,
            join_handles: Mutex::new(Vec::new()),
            joined_tasks: AtomicI32::new(1),
            version,
        });

        MetaNode::subscribe_metrics(meta_node.clone(), raft.metrics()).await;

        let endpoint = if let Some(a) = self.raft_service_endpoint.take() {
            a
        } else {
            sto.get_node_raft_endpoint(&node_id).await.ok_or_else(|| {
                MetaStartupError::InvalidConfig(format!(
                    "Node {} not found in state machine",
                    node_id
                ))
            })?
        };

        MetaNode::start_raft_service(meta_node.clone(), &endpoint).await?;

        Ok(meta_node)
    }

    #[must_use]
    pub fn node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    #[must_use]
    pub fn sto(mut self, sto: RaftStore<SP>) -> Self {
        self.sto = Some(sto);
        self
    }

    #[must_use]
    pub fn raft_service_endpoint(mut self, endpoint: Endpoint) -> Self {
        self.raft_service_endpoint = Some(endpoint);
        self
    }

    #[must_use]
    pub fn version(mut self, version: Version) -> Self {
        self.version = Some(version);
        self
    }
}
