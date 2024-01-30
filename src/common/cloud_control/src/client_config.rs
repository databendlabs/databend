// Copyright 2021 Datafuse Labs
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

use std::fmt::Debug;
use std::time::Duration;

use crate::cloud_api::CLOUD_REQUEST_TIMEOUT_SEC;
use crate::cloud_api::QUERY_ID;
use crate::cloud_api::REQUESTER;
use crate::cloud_api::TENANT_ID;

#[derive(Debug, Clone, PartialEq)]
pub struct ClientConfig {
    metadata: Vec<(String, String)>,
    timeout: Duration,
}

impl ClientConfig {
    pub fn new(timeout: Duration) -> Self {
        ClientConfig {
            metadata: Vec::new(),
            timeout,
        }
    }

    pub fn add_metadata<K, V>(&mut self, key: K, value: V)
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.metadata.push((key.into(), value.into()));
    }

    pub fn get_metadata(&self) -> &Vec<(String, String)> {
        &self.metadata
    }

    pub fn get_timeout(&self) -> Duration {
        self.timeout
    }
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self::new(Duration::from_secs(CLOUD_REQUEST_TIMEOUT_SEC))
    }
}

pub fn build_client_config(
    tenant: String,
    user: String,
    query_id: String,
    timeout: Duration,
) -> ClientConfig {
    let mut config = ClientConfig::new(timeout);
    config.add_metadata(TENANT_ID, tenant);
    config.add_metadata(REQUESTER, user);
    config.add_metadata(QUERY_ID, query_id);
    config
}
