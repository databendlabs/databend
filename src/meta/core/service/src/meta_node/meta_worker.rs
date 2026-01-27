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

use databend_common_meta_runtime_api::RuntimeApi;
use databend_common_meta_types::MetaStartupError;
use log::error;
use log::info;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::configs::MetaServiceConfig;
use crate::meta_node::meta_handle::MetaFnOnce;
use crate::meta_node::meta_handle::MetaHandle;
use crate::meta_service::MetaNode;

/// A worker running in background receiving and handling meta node request.
pub struct MetaWorker<RT: RuntimeApi> {
    worker_rx: mpsc::Receiver<MetaFnOnce<Arc<MetaNode<RT>>, ()>>,
    meta_node: Arc<MetaNode<RT>>,
}

impl<RT: RuntimeApi> MetaWorker<RT> {
    /// Create and start a meta worker on the provided runtime.
    ///
    /// This spawns a background task that:
    /// 1. Starts a `MetaNode` with the given config
    /// 2. Runs a worker loop to process requests via `MetaHandle`
    ///
    /// The returned `MetaHandle` keeps the runtime alive and provides
    /// an interface to communicate with the `MetaNode`.
    pub async fn create_meta_worker(
        config: MetaServiceConfig,
        runtime: Arc<RT>,
    ) -> Result<MetaHandle<RT>, MetaStartupError> {
        let rt_for_handle = runtime.clone();

        let (ret_tx, ret_rx) = oneshot::channel();

        #[allow(unused_must_use)]
        runtime.spawn_on(
            async move {
                let (handle_tx, worker_rx) = mpsc::channel(1024);

                let res = MetaNode::<RT>::start(&config).await;
                let meta_node = match res {
                    Ok(x) => x,
                    Err(e) => {
                        error!("Failed to start MetaNode: {}", e);
                        return;
                    }
                };

                let id = meta_node.raft_store.id;

                let worker = MetaWorker::<RT> {
                    worker_rx,
                    meta_node,
                };

                let handle = MetaHandle::<RT>::new(id, handle_tx, rt_for_handle);

                ret_tx
                    .send(handle)
                    .inspect_err(|_meta_handle| {
                        error!("MetaWorker::work stopped before sending handle")
                    })
                    .ok();

                info!("MetaWorker.run() starting");

                worker.run().await;

                info!("MetaWorker.run() finished");
            },
            None,
        );

        let meta_handle = ret_rx.await.map_err(|e| {
            MetaStartupError::MetaServiceError(format!(
                "MetaWorker::work stopped before sending handle: {}",
                e
            ))
        })?;

        Ok(meta_handle)
    }

    pub async fn run(mut self) {
        while let Some(box_fn) = self.worker_rx.recv().await {
            (box_fn)(self.meta_node.clone()).await;
        }

        info!(
            "MetaWorker stopped because input channel closed, ref count to meta-node: {}",
            Arc::strong_count(&self.meta_node)
        );
    }
}
