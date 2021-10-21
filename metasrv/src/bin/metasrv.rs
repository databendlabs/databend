// Copyright 2020 Datafuse Labs.
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

use common_base::tokio;
use common_base::BlockingWait;
use common_base::Runtime;
use common_base::RuntimeTracker;
use common_exception::ErrorCode;
use common_exception::ToErrorCode;
use common_meta_sled_store::init_sled_db;
use common_tracing::init_tracing_with_file;
use databend_meta::api::FlightServer;
use databend_meta::api::HttpService;
use databend_meta::configs::Config;
use databend_meta::metrics::MetricService;
use log::info;
use structopt::StructOpt;

// TODO: replace with proc macro
fn main() {
    let global_runtime = Runtime::with_default_worker_threads().unwrap();
    let main_entity = async_main(global_runtime.get_tracker());
    main_entity.wait_in(&global_runtime, None).unwrap().unwrap();
}

async fn async_main(_global_tracker: Arc<RuntimeTracker>) -> common_exception::Result<()> {
    let conf = Config::from_args();
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(conf.log_level.to_lowercase().as_str()),
    )
    .init();

    let _guards = init_tracing_with_file(
        "databend-meta",
        conf.log_dir.as_str(),
        conf.log_level.as_str(),
    );

    info!("{:?}", conf.clone());
    info!(
        "Databend-meta v-{}",
        *databend_meta::configs::config::DATABEND_COMMIT_VERSION
    );

    init_sled_db(conf.raft_config.raft_dir.clone());

    // Metric API service.
    {
        let srv = MetricService::create(conf.clone());
        tokio::spawn(async move {
            srv.make_server().expect("Metrics service error");
        });
        info!("Metric API server listening on {}", conf.metric_api_address);
    }

    // HTTP API service.
    {
        let mut srv = HttpService::create(conf.clone());
        info!("HTTP API server listening on {}", conf.admin_api_address);
        tokio::spawn(async move {
            srv.start().await.expect("HTTP: admin api error");
        });
    }

    // Flight API service.
    {
        let srv = FlightServer::create(conf.clone());
        info!(
            "Databend-meta API server listening on {}",
            conf.flight_api_address
        );
        let (_stop_tx, fin_rx) = srv.start().await.expect("Databend-meta service error");
        fin_rx.await.map_err_to_code(ErrorCode::TokioError, || {
            "Cannot receive data from Flight API service fin_rx"
        })?;
    }

    Ok(())
}
