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

use anyhow::anyhow;
use anyhow::Result;
use metrics_exporter_prometheus::PrometheusBuilder;

use crate::configs::Config;

pub struct MetricService {
    conf: Config,
}

impl MetricService {
    pub fn create(conf: Config) -> Self {
        MetricService { conf }
    }

    pub fn make_server(&self) -> Result<()> {
        let addr = self
            .conf
            .metric_api_address
            .parse::<std::net::SocketAddr>()?;

        PrometheusBuilder::new()
            .listen_address(addr)
            .install()
            .map_err(|e| anyhow!(format!("Metrics prometheus exporter error: {:?}", e)))
    }
}
