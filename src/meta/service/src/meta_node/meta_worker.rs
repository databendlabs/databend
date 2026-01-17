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

use databend_common_base::runtime::Runtime;
use databend_common_meta_types::MetaStartupError;
use databend_common_version::BUILD_INFO;
use log::error;
use log::info;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::configs::Config;
use crate::meta_node::meta_handle::MetaFnOnce;
use crate::meta_node::meta_handle::MetaHandle;
use crate::meta_service::MetaNode;

/// A worker running in background receiving and handling meta node request.
pub struct MetaWorker {
    worker_rx: mpsc::Receiver<MetaFnOnce<()>>,
    meta_node: Arc<MetaNode>,
}

impl MetaWorker {
    pub async fn create_meta_worker_in_rt(config: Config) -> Result<MetaHandle, MetaStartupError> {
        let meta_io_rt =
            Runtime::with_worker_threads(32, Some("meta-io-rt".to_string())).map_err(|e| {
                MetaStartupError::MetaServiceError(format!("Cannot create meta IO runtime: {}", e))
            })?;

        let meta_io_rt = Arc::new(meta_io_rt);
        let rt = meta_io_rt.clone();

        let (ret_tx, ret_rx) = oneshot::channel();

        meta_io_rt.spawn(async move {
            let (handle_tx, worker_rx) = mpsc::channel(1024);

            let res = MetaNode::start(&config, BUILD_INFO.semantic.clone()).await;
            let meta_node = match res {
                Ok(x) => x,
                Err(e) => {
                    error!("Failed to start MetaNode: {}", e);
                    return;
                }
            };

            let id = meta_node.raft_store.id;
            let version = meta_node.version.clone();

            let worker = MetaWorker {
                worker_rx,
                meta_node,
            };

            let handle = MetaHandle::new(id, version, handle_tx, rt);

            ret_tx
                .send(handle)
                .inspect_err(|_meta_handle| {
                    error!("MetaWorker::work stopped before sending handle")
                })
                .ok();

            info!("MetaWorker.run() starting");

            worker.run().await;

            info!("MetaWorker.run() finished");
        });

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
