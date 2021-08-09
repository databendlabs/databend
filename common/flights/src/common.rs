// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow_flight;

pub fn flight_result_to_str(r: &arrow_flight::Result) -> String {
    match std::str::from_utf8(&r.body) {
        Ok(v) => v.to_string(),
        Err(_e) => format!("{:?}", r.body),
    }
}

#[derive(Clone, Debug)]
pub struct RpcClientTlsConfig {
    pub rpc_tls_server_root_ca_cert: String,
    pub domain_name: String,
}
impl RpcClientTlsConfig {
    pub fn enabled(&self) -> bool {
        !self.rpc_tls_server_root_ca_cert.is_empty() && !self.domain_name.is_empty()
    }
}
