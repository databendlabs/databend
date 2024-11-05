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

use databend_common_base::headers::HEADER_QUERY_ID;
use databend_common_base::headers::HEADER_TENANT;
use databend_common_base::headers::HEADER_USER;
use tonic::Request;

use crate::cloud_api::CLOUD_REQUEST_TIMEOUT_SEC;

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

    pub fn add_task_version_info(&mut self) {
        self.add_metadata(
            crate::task_client::TASK_CLIENT_VERSION_NAME,
            crate::task_client::TASK_CLIENT_VERSION,
        );
    }

    pub fn add_notification_version_info(&mut self) {
        self.add_metadata(
            crate::notification_client::NOTIFICATION_CLIENT_VERSION_NAME,
            crate::notification_client::NOTIFICATION_CLIENT_VERSION,
        );
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
    config.add_metadata(HEADER_TENANT, tenant);
    config.add_metadata(HEADER_USER, user);
    config.add_metadata(HEADER_QUERY_ID, query_id);
    config
}

// add necessary metadata and client request setup for auditing and tracing purpose
pub fn make_request<T>(t: T, config: ClientConfig) -> Request<T> {
    let mut request = Request::new(t);
    request.set_timeout(config.get_timeout());
    let metadata = request.metadata_mut();
    let config_meta = config.get_metadata().clone();
    for (k, v) in config_meta {
        let key = k
            .parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>()
            .unwrap();
        metadata.insert(key, v.parse().unwrap());
    }
    // metadata.insert(
    //     TASK_CLIENT_VERSION_NAME
    //         .to_string()
    //         .parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>()
    //         .unwrap(),
    //     TASK_CLIENT_VERSION.to_string().parse().unwrap(),
    // );
    request
}
