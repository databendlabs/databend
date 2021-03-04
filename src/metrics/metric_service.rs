// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;

use metrics_exporter_prometheus::PrometheusBuilder;

use crate::error::{FuseQueryError, FuseQueryResult};

pub struct MetricService {}

impl MetricService {
    pub fn make_server(addr: SocketAddr) -> FuseQueryResult<()> {
        PrometheusBuilder::new()
            .listen_address(addr)
            .install()
            .map_err(|e| {
                FuseQueryError::Internal(format!("Metrics prometheus exporter error: {:?}", e))
            })
    }
}
