// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use metrics_exporter_prometheus::PrometheusBuilder;

use crate::configs::Config;
use crate::error::{Error, Result};

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
            .map_err(|e| {
                Error::build_internal_error(format!("Metrics prometheus exporter error: {:?}", e))
            })
    }
}
