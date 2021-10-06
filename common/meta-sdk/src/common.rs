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

use common_arrow::arrow_flight;

pub fn flight_result_to_str(r: &arrow_flight::Result) -> String {
    match std::str::from_utf8(&r.body) {
        Ok(v) => v.to_string(),
        Err(_e) => format!("{:?}", r.body),
    }
}

#[derive(Clone, Debug, Default)]
pub struct RpcClientTlsConfig {
    pub rpc_tls_server_root_ca_cert: String,
    pub domain_name: String,
}
impl RpcClientTlsConfig {
    pub fn enabled(&self) -> bool {
        !self.rpc_tls_server_root_ca_cert.is_empty() && !self.domain_name.is_empty()
    }
}
