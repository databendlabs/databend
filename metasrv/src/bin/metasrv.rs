// Copyright 2021 Datafuse Labs.
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

use common_base::RuntimeTracker;
use common_base::StopHandle;
use common_base::Stoppable;
use common_macros::databend_main;
use common_meta_sled_store::init_sled_db;
use common_metrics::init_default_metrics_recorder;
use common_tracing::init_global_tracing;
use common_tracing::tracing;
use databend_meta::api::GrpcServer;
use databend_meta::api::HttpService;
use databend_meta::configs::Config;
use databend_meta::meta_service::MetaNode;
use databend_meta::metrics::MetricService;

#[databend_main]
async fn main(_global_tracker: Arc<RuntimeTracker>) -> common_exception::Result<()> {
    let conf = Config::load()?;

    let _guards = init_global_tracing(
        "databend-meta",
        conf.log_dir.as_str(),
        conf.log_level.as_str(),
    );

    tracing::info!("{:?}", conf.clone());

    init_sled_db(conf.raft_config.raft_dir.clone());
    init_default_metrics_recorder();

    tracing::info!(
        "Starting MetaNode single: {} with config: {:?}",
        conf.raft_config.single,
        conf
    );

    let meta_node = MetaNode::start(&conf.raft_config).await?;

    let mut stop_handler = StopHandle::create();
    let stop_tx = StopHandle::install_termination_handle();

    // Metric API service.
    {
        let mut srv = MetricService::create(conf.clone());
        srv.start().await.expect("Failed to start metrics server");
        tracing::info!("Metric API server listening on {}", conf.metric_api_address);
        stop_handler.push(srv);
    }

    // HTTP API service.
    {
        let mut srv = HttpService::create(conf.clone(), meta_node.clone());
        tracing::info!("HTTP API server listening on {}", conf.admin_api_address);
        srv.start().await.expect("Failed to start http server");
        stop_handler.push(srv);
    }

    // gRPC API service.
    {
        let mut srv = GrpcServer::create(conf.clone(), meta_node.clone());
        tracing::info!(
            "Databend meta server listening on {}",
            conf.grpc_api_address
        );
        srv.start().await.expect("Databend meta service error");
        stop_handler.push(Box::new(srv));
    }

    stop_handler.wait_to_terminate(stop_tx).await;
    tracing::info!("Databend-meta is done shutting down");

    Ok(())
}
