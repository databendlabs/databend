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

use std::collections::BTreeMap;

use databend_common_meta_app::storage::StorageParams;
use databend_common_storage::Scheme;
use opendal::raw::normalize_root;

#[derive(Debug)]
pub struct ExternalStorageConnection {
    pub uri: String,
    pub params: BTreeMap<String, String>,
}

impl ExternalStorageConnection {
    pub fn new(params: BTreeMap<String, String>) -> Self {
        Self {
            uri: "".to_string(),
            params,
        }
    }

    pub fn set_value(&mut self, key: String, value: String) {
        if value.is_empty() {
            return;
        }
        self.params.insert(key, value);
    }

    pub fn set_uri(&mut self, typ: String, bucket: String, root: String) {
        let normalized_root = normalize_root(&root);

        self.uri = format!("{}://{}{}", typ, bucket, normalized_root);
    }

    /// Generate CONNECTION clause for CREATE STAGE statement
    pub fn to_connection_string(&self) -> String {
        let mut connection_params = Vec::new();
        for (key, value) in &self.params {
            if !value.is_empty() {
                connection_params.push(format!("{} = '{}'", key.to_uppercase(), value));
            }
        }
        connection_params.join(" ")
    }

    pub fn to_create_stage_sql(&self, stage_name: &str) -> String {
        let uri = format!("{}stage/internal/{}/", self.uri, stage_name);
        let connection_str = self.to_connection_string();
        format!(
            "CREATE STAGE IF NOT EXISTS {} URL='{}' CONNECTION = ({})",
            stage_name, uri, connection_str
        )
    }

    pub fn to_create_table_sql(&self, table_definition: &str, table_name: &str) -> String {
        let connection_str = self.to_connection_string();
        let uri = format!("{}{}/", self.uri, table_name);

        // Check if table_definition contains CLUSTER BY clause
        // `CREATE TABLE ... CLUSTER BY () ... URI CONNECTION = (...)` is not valid
        // We expect `CREATE TABLE ... URI CONNECTION = (...) CLUSTER BY ()`
        if let Some(cluster_pos) = table_definition.to_uppercase().find(" CLUSTER BY ") {
            let table_part = table_definition[..cluster_pos].trim();
            let cluster_part = table_definition[cluster_pos..].trim();
            format!(
                "{} '{}' CONNECTION = ({}) {}",
                table_part, uri, connection_str, cluster_part
            )
        } else {
            format!(
                "{} '{}' CONNECTION = ({})",
                table_definition, uri, connection_str
            )
        }
    }
}

pub fn get_external_storage_connection(
    storage_params: &StorageParams,
) -> ExternalStorageConnection {
    let mut info = ExternalStorageConnection::new(BTreeMap::new());
    match storage_params {
        StorageParams::S3(config) => {
            info.set_uri(
                Scheme::S3.to_string(),
                config.bucket.clone(),
                config.root.clone(),
            );

            info.set_value("endpoint_url".to_string(), config.endpoint_url.clone());
            info.set_value("region".to_string(), config.region.clone());
            info.set_value("access_key_id".to_string(), config.access_key_id.clone());
            info.set_value(
                "secret_access_key".to_string(),
                config.secret_access_key.clone(),
            );
            info.set_value("security_token".to_string(), config.security_token.clone());
            info.set_value("master_key".to_string(), config.master_key.clone());
            info.set_value(
                "enable_virtual_host_style".to_string(),
                config.enable_virtual_host_style.to_string(),
            );
            info.set_value("role_arn".to_string(), config.role_arn.clone());
            info.set_value("external_id".to_string(), config.external_id.clone());
        }
        StorageParams::Azblob(config) => {
            info.set_uri(
                Scheme::Azblob.to_string(),
                config.container.clone(),
                config.root.clone(),
            );

            info.set_value("endpoint_url".to_string(), config.endpoint_url.clone());
            info.set_value("account_name".to_string(), config.account_name.clone());
            info.set_value("account_key".to_string(), config.account_key.clone());
        }
        StorageParams::Gcs(config) => {
            info.set_uri(
                Scheme::Gcs.to_string(),
                config.bucket.clone(),
                config.root.clone(),
            );

            info.set_value("endpoint_url".to_string(), config.endpoint_url.clone());
            info.set_value("credential".to_string(), config.credential.clone());
        }
        StorageParams::Oss(config) => {
            info.set_uri(
                Scheme::Oss.to_string(),
                config.bucket.clone(),
                config.root.clone(),
            );

            info.set_value("endpoint_url".to_string(), config.endpoint_url.clone());
            info.set_value(
                "presign_endpoint_url".to_string(),
                config.presign_endpoint_url.clone(),
            );
            info.set_value("access_key_id".to_string(), config.access_key_id.clone());
            info.set_value(
                "access_key_secret".to_string(),
                config.access_key_secret.clone(),
            );
            info.set_value(
                "server_side_encryption".to_string(),
                config.server_side_encryption.to_string(),
            );
            info.set_value(
                "server_side_encryption_key_id".to_string(),
                config.server_side_encryption_key_id.clone(),
            );
        }
        StorageParams::Cos(config) => {
            info.set_uri(
                Scheme::Cos.to_string(),
                config.bucket.clone(),
                config.root.clone(),
            );

            info.set_value("secret_id".to_string(), config.secret_id.clone());
            info.set_value("secret_key".to_string(), config.secret_key.clone());
            info.set_value("endpoint_url".to_string(), config.endpoint_url.clone());
        }
        StorageParams::Obs(config) => {
            info.set_uri(
                Scheme::Obs.to_string(),
                config.bucket.clone(),
                config.root.clone(),
            );

            info.set_value("endpoint_url".to_string(), config.endpoint_url.clone());
            info.set_value("access_key_id".to_string(), config.access_key_id.clone());
            info.set_value(
                "secret_access_key".to_string(),
                config.secret_access_key.clone(),
            );
        }
        StorageParams::Fs(config) => {
            info.uri = format!("file://{}", normalize_root(&config.root));
        }
        _ => unimplemented!("Not implemented for storage type: {:?}", storage_params),
    }
    info
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_create_stage_sql() {
        let mut connection = ExternalStorageConnection::new(BTreeMap::new());
        connection.set_uri(
            "s3".to_string(),
            "test-bucket".to_string(),
            "test".to_string(),
        );
        connection.set_value("access_key_id".to_string(), "test_key".to_string());

        let stage_name = "log_1f93b76af0bd4b1d8e018667865fbc65";
        let result = connection.to_create_stage_sql(stage_name);

        assert_eq!(
            result,
            "CREATE STAGE IF NOT EXISTS log_1f93b76af0bd4b1d8e018667865fbc65 URL='s3://test-bucket/test/stage/internal/log_1f93b76af0bd4b1d8e018667865fbc65/' CONNECTION = (ACCESS_KEY_ID = 'test_key')"
        );
    }

    #[test]
    fn test_to_create_table_sql_with_cluster_by() {
        let mut connection = ExternalStorageConnection::new(BTreeMap::new());
        connection.set_uri(
            "s3".to_string(),
            "test-bucket".to_string(),
            "test".to_string(),
        );
        connection.set_value("access_key_id".to_string(), "test_key".to_string());

        let table_definition = "CREATE TABLE system_history.log_history (timestamp TIMESTAMP, level STRING, message STRING) CLUSTER BY (timestamp, level)";
        let table_name = "log_history";
        let result = connection.to_create_table_sql(table_definition, table_name);

        assert_eq!(
            result,
            "CREATE TABLE system_history.log_history (timestamp TIMESTAMP, level STRING, message STRING) 's3://test-bucket/test/log_history/' CONNECTION = (ACCESS_KEY_ID = 'test_key') CLUSTER BY (timestamp, level)"
        );
    }
}
