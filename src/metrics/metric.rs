// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use metrics_exporter_prometheus::PrometheusBuilder;

use crate::configs::Config;
use crate::error::FuseQueryResult;

pub struct Metric {
    cfg: Config,
}

impl Metric {
    pub fn create(cfg: Config) -> Self {
        Metric { cfg }
    }

    pub fn start(&self) -> FuseQueryResult<()> {
        // Run a Prometheus scrape endpoint on 127.0.0.1:9000.
        let _ = PrometheusBuilder::new()
            .listen_address(
                self.cfg
                    .prometheus_exporter_address
                    .parse::<std::net::SocketAddr>()
                    .expect("Failed to parse prometheus exporter address"),
            )
            .install()
            .expect("Failed to install prometheus exporter");
        Ok(())
    }
}
