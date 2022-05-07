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

use std::net::SocketAddr;

use common_base::tokio::sync::broadcast;
use common_base::HttpShutdownHandler;
use common_base::Stoppable;
use common_exception::ErrorCode;
use common_exception::Result;
use common_metrics::PrometheusHandle;
use poem::web::Data;
use poem::EndpointExt;
use poem::IntoResponse;

use crate::configs::Config;

pub struct MetricService {
    conf: Config,
    shutdown_handler: HttpShutdownHandler,
}

#[poem::handler]
pub async fn metric_handler(prom_extension: Data<&PrometheusHandle>) -> impl IntoResponse {
    prom_extension.0.render()
}

impl MetricService {
    // TODO add session tls handler
    pub fn create(conf: Config) -> Box<MetricService> {
        Box::new(MetricService {
            conf,
            shutdown_handler: HttpShutdownHandler::create("metric api".to_string()),
        })
    }

    async fn start_without_tls(&mut self, listening: String) -> Result<SocketAddr> {
        let prometheus_handle = common_metrics::try_handle().ok_or_else(|| {
            ErrorCode::InitPrometheusFailure("Prometheus recorder has not been initialized yet.")
        })?;

        let app = poem::Route::new()
            .at("/metrics", poem::get(metric_handler))
            .data(prometheus_handle);
        let addr = self
            .shutdown_handler
            .start_service(listening, None, app)
            .await?;
        Ok(addr)
    }
}

#[async_trait::async_trait]
impl Stoppable for MetricService {
    async fn start(&mut self) -> Result<()> {
        let config = self.conf.clone();
        self.start_without_tls(config.metric_api_address)
            .await
            .expect("Failed to start the metrics server");
        Ok(())
    }

    async fn stop(&mut self, force: Option<broadcast::Receiver<()>>) -> Result<()> {
        self.shutdown_handler.stop(force).await
    }
}
