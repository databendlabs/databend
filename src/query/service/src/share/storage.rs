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

use databend_common_ast::ast::UriLocation;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::tenant::Tenant;

pub async fn resolve_share_storage_params(
    provider: &Tenant,
    connection_name: &str,
    provider_storage: StorageParams,
) -> Result<StorageParams> {
    let connection = databend_common_users::UserApiProvider::instance()
        .get_connection(provider, connection_name)
        .await
        .map_err(|err| {
            ErrorCode::InvalidOperation(format!(
                "Cannot resolve connection '{}' for data sharing: {}",
                connection_name, err
            ))
        })?;

    if !connection
        .storage_type
        .eq_ignore_ascii_case(provider_storage.storage_type())
    {
        return Err(ErrorCode::InvalidOperation(format!(
            "Connection '{}' storage type '{}' does not match provider table storage type '{}'",
            connection_name,
            connection.storage_type,
            provider_storage.storage_type()
        )));
    }

    if matches!(provider_storage, StorageParams::Fs(_)) && connection.storage_params.is_empty() {
        return Ok(provider_storage);
    }

    let location = provider_storage.url().ok_or_else(|| {
        ErrorCode::InvalidOperation(format!(
            "Provider storage type '{}' cannot be resolved from a data sharing connection",
            provider_storage.storage_type()
        ))
    })?;
    let mut location = UriLocation::from_uri(location, connection.storage_params)?;
    let connection_storage = databend_common_sql::binder::parse_storage_params_from_uri(
        &mut location,
        "when resolving data share connection",
    )
    .await?;

    merge_share_storage_params(provider_storage, connection_storage)
}

fn merge_share_storage_params(
    provider_storage: StorageParams,
    connection_storage: StorageParams,
) -> Result<StorageParams> {
    match (&provider_storage, &connection_storage) {
        (StorageParams::S3(_), StorageParams::S3(_))
        | (StorageParams::Azblob(_), StorageParams::Azblob(_))
        | (StorageParams::Gcs(_), StorageParams::Gcs(_)) => {
            provider_storage.apply_update(connection_storage)
        }
        _ if provider_storage.storage_type() != connection_storage.storage_type() => {
            Err(ErrorCode::InvalidOperation(format!(
                "Connection storage type '{}' does not match provider table storage type '{}'",
                connection_storage.storage_type(),
                provider_storage.storage_type()
            )))
        }
        _ if !provider_storage.has_credentials() && !connection_storage.has_credentials() => {
            Ok(provider_storage)
        }
        _ => Err(ErrorCode::InvalidOperation(format!(
            "Data sharing connection credential override is not supported for storage type '{}'",
            provider_storage.storage_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_app::storage::StorageS3Config;

    use super::*;

    #[test]
    fn merge_s3_connection_replaces_credentials_but_preserves_provider_location() {
        let provider = StorageParams::S3(StorageS3Config {
            endpoint_url: "https://provider.example.com".to_string(),
            region: "provider-region".to_string(),
            bucket: "provider-bucket".to_string(),
            root: "/provider/root".to_string(),
            access_key_id: "provider-key".to_string(),
            secret_access_key: "provider-secret".to_string(),
            security_token: "provider-token".to_string(),
            ..Default::default()
        });
        let connection = StorageParams::S3(StorageS3Config {
            endpoint_url: "https://connection.example.com".to_string(),
            region: "connection-region".to_string(),
            bucket: "connection-bucket".to_string(),
            root: "/connection/root".to_string(),
            access_key_id: "share-key".to_string(),
            secret_access_key: "share-secret".to_string(),
            security_token: "share-token".to_string(),
            role_arn: "share-role".to_string(),
            external_id: "share-external-id".to_string(),
            ..Default::default()
        });

        let merged = merge_share_storage_params(provider, connection).unwrap();
        let StorageParams::S3(merged) = merged else {
            unreachable!("S3 merge must return S3 storage params");
        };
        assert_eq!("https://provider.example.com", merged.endpoint_url);
        assert_eq!("provider-region", merged.region);
        assert_eq!("provider-bucket", merged.bucket);
        assert_eq!("/provider/root", merged.root);
        assert_eq!("share-key", merged.access_key_id);
        assert_eq!("share-secret", merged.secret_access_key);
        assert_eq!("share-token", merged.security_token);
        assert_eq!("share-role", merged.role_arn);
        assert_eq!("share-external-id", merged.external_id);
    }
}
