// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::anyhow;
use anyhow::Result;
use metrics_exporter_prometheus::PrometheusBuilder;

use crate::configs::Config;

pub struct MetricService {
    conf: Config
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
