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
//! Google Cloud Storage properties

use std::collections::HashMap;

use opendal::Operator;
use opendal::services::GcsConfig;
use url::Url;

use crate::io::is_truthy;
use crate::{Error, ErrorKind, Result};

// Reference: https://github.com/apache/iceberg/blob/main/gcp/src/main/java/org/apache/iceberg/gcp/GCPProperties.java

/// Google Cloud Project ID
pub const GCS_PROJECT_ID: &str = "gcs.project-id";
/// Google Cloud Storage endpoint
pub const GCS_SERVICE_PATH: &str = "gcs.service.path";
/// Google Cloud user project
pub const GCS_USER_PROJECT: &str = "gcs.user-project";
/// Allow unauthenticated requests
pub const GCS_NO_AUTH: &str = "gcs.no-auth";
/// Google Cloud Storage credentials JSON string, base64 encoded.
///
/// E.g. base64::prelude::BASE64_STANDARD.encode(serde_json::to_string(credential).as_bytes())
pub const GCS_CREDENTIALS_JSON: &str = "gcs.credentials-json";
/// Google Cloud Storage token
pub const GCS_TOKEN: &str = "gcs.oauth2.token";

/// Option to skip signing requests (e.g. for public buckets/folders).
pub const GCS_ALLOW_ANONYMOUS: &str = "gcs.allow-anonymous";
/// Option to skip loading the credential from GCE metadata server (typically used in conjunction with `GCS_ALLOW_ANONYMOUS`).
pub const GCS_DISABLE_VM_METADATA: &str = "gcs.disable-vm-metadata";
/// Option to skip loading configuration from config file and the env.
pub const GCS_DISABLE_CONFIG_LOAD: &str = "gcs.disable-config-load";

/// Parse iceberg properties to [`GcsConfig`].
pub(crate) fn gcs_config_parse(mut m: HashMap<String, String>) -> Result<GcsConfig> {
    let mut cfg = GcsConfig::default();

    if let Some(cred) = m.remove(GCS_CREDENTIALS_JSON) {
        cfg.credential = Some(cred);
    }

    if let Some(token) = m.remove(GCS_TOKEN) {
        cfg.token = Some(token);
    }

    if let Some(endpoint) = m.remove(GCS_SERVICE_PATH) {
        cfg.endpoint = Some(endpoint);
    }

    if m.remove(GCS_NO_AUTH).is_some() {
        cfg.allow_anonymous = true;
        cfg.disable_vm_metadata = true;
        cfg.disable_config_load = true;
    }

    if let Some(allow_anonymous) = m.remove(GCS_ALLOW_ANONYMOUS)
        && is_truthy(allow_anonymous.to_lowercase().as_str())
    {
        cfg.allow_anonymous = true;
    }
    if let Some(disable_ec2_metadata) = m.remove(GCS_DISABLE_VM_METADATA)
        && is_truthy(disable_ec2_metadata.to_lowercase().as_str())
    {
        cfg.disable_vm_metadata = true;
    };
    if let Some(disable_config_load) = m.remove(GCS_DISABLE_CONFIG_LOAD)
        && is_truthy(disable_config_load.to_lowercase().as_str())
    {
        cfg.disable_config_load = true;
    };

    Ok(cfg)
}

/// Build a new OpenDAL [`Operator`] based on a provided [`GcsConfig`].
pub(crate) fn gcs_config_build(cfg: &GcsConfig, path: &str) -> Result<Operator> {
    let url = Url::parse(path)?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid gcs url: {path}, bucket is required"),
        )
    })?;

    let mut cfg = cfg.clone();
    cfg.bucket = bucket.to_string();
    Ok(Operator::from_config(cfg)?.finish())
}
