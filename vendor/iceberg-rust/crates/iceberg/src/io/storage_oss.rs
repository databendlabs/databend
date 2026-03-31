// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;

use opendal::services::OssConfig;
use opendal::{Configurator, Operator};
use url::Url;

use crate::{Error, ErrorKind, Result};

/// Required configuration arguments for creating an Aliyun OSS Operator with OpenDAL:
/// - `oss.endpoint`: The OSS service endpoint URL
/// - `oss.access-key-id`: The access key ID for authentication
/// - `oss.access-key-secret`: The access key secret for authentication
///   Aliyun oss endpoint.
pub const OSS_ENDPOINT: &str = "oss.endpoint";
/// Aliyun oss access key id.
pub const OSS_ACCESS_KEY_ID: &str = "oss.access-key-id";
/// Aliyun oss access key secret.
pub const OSS_ACCESS_KEY_SECRET: &str = "oss.access-key-secret";

/// Parse iceberg props to oss config.
pub(crate) fn oss_config_parse(mut m: HashMap<String, String>) -> Result<OssConfig> {
    let mut cfg: OssConfig = OssConfig::default();
    if let Some(endpoint) = m.remove(OSS_ENDPOINT) {
        cfg.endpoint = Some(endpoint);
    };
    if let Some(access_key_id) = m.remove(OSS_ACCESS_KEY_ID) {
        cfg.access_key_id = Some(access_key_id);
    };
    if let Some(access_key_secret) = m.remove(OSS_ACCESS_KEY_SECRET) {
        cfg.access_key_secret = Some(access_key_secret);
    };

    Ok(cfg)
}

/// Build new opendal operator from give path.
pub(crate) fn oss_config_build(cfg: &OssConfig, path: &str) -> Result<Operator> {
    let url = Url::parse(path)?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid oss url: {path}, missing bucket"),
        )
    })?;

    let builder = cfg.clone().into_builder().bucket(bucket);

    Ok(Operator::new(builder)?.finish())
}
