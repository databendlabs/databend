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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::anyhow;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use iceberg::TableIdent;
use iceberg::io::AwsCredential;
use iceberg::io::AwsCredentialLoad;
use iceberg::io::CustomAwsCredentialLoader;
use iceberg::io::FileIOBuilder;
use reqwest::Client;
use tokio::sync::Mutex;

const CREDENTIAL_REFRESH_MARGIN: Duration = Duration::from_secs(300);
const CREDENTIAL_REFRESH_RETRY_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct VendedCredential {
    credential: AwsCredential,
    refresh_at: Instant,
    expires_at: Instant,
}

impl VendedCredential {
    fn from_table(table: &iceberg::table::Table) -> Option<Self> {
        let (_, props, _) = table.file_io().clone().into_builder().into_parts();
        Self::from_props(&props, SystemTime::now(), Instant::now())
    }

    fn from_props(
        props: &HashMap<String, String>,
        now_system: SystemTime,
        now_instant: Instant,
    ) -> Option<Self> {
        let expires_at_ms = property(props, "expires-at-ms")?.parse::<u64>().ok()?;
        let expires_at_datetime =
            DateTime::<Utc>::from_timestamp_millis(i64::try_from(expires_at_ms).ok()?)?;
        let expires_at_system = UNIX_EPOCH.checked_add(Duration::from_millis(expires_at_ms))?;
        let remaining = expires_at_system
            .duration_since(now_system)
            .unwrap_or_default();
        let margin = CREDENTIAL_REFRESH_MARGIN.min(remaining / 10);

        Some(Self {
            credential: AwsCredential {
                access_key_id: props.get("s3.access-key-id")?.clone(),
                secret_access_key: props.get("s3.secret-access-key")?.clone(),
                session_token: props.get("s3.session-token").cloned(),
                expires_in: Some(expires_at_datetime),
            },
            refresh_at: now_instant.checked_add(remaining.saturating_sub(margin))?,
            expires_at: now_instant.checked_add(remaining)?,
        })
    }
}

fn property<'a>(props: &'a HashMap<String, String>, suffix: &str) -> Option<&'a str> {
    props
        .get(suffix)
        .or_else(|| {
            props
                .iter()
                .find(|(key, _)| key.ends_with(suffix))
                .map(|(_, value)| value)
        })
        .map(String::as_str)
}

#[async_trait]
trait VendedCredentialProvider: Send + Sync {
    async fn load(&self) -> anyhow::Result<VendedCredential>;
}

struct CatalogCredentialProvider {
    catalog: Arc<dyn iceberg::Catalog>,
    table_ident: TableIdent,
}

#[async_trait]
impl VendedCredentialProvider for CatalogCredentialProvider {
    async fn load(&self) -> anyhow::Result<VendedCredential> {
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(|error| anyhow!("failed to refresh Iceberg table credentials: {error:?}"))?;
        VendedCredential::from_table(&table).ok_or_else(|| {
            anyhow!("Iceberg load_table response did not contain vended credentials")
        })
    }
}

struct RefreshingAwsCredentialLoader {
    provider: Arc<dyn VendedCredentialProvider>,
    current: Mutex<VendedCredential>,
}

impl RefreshingAwsCredentialLoader {
    fn new(provider: Arc<dyn VendedCredentialProvider>, current: VendedCredential) -> Self {
        Self {
            provider,
            current: Mutex::new(current),
        }
    }
}

#[async_trait]
impl AwsCredentialLoad for RefreshingAwsCredentialLoader {
    async fn load_credential(&self, _client: Client) -> anyhow::Result<Option<AwsCredential>> {
        let mut current = self.current.lock().await;
        let now = Instant::now();
        if now < current.refresh_at {
            return Ok(Some(current.credential.clone()));
        }

        match self.provider.load().await {
            Ok(refreshed) => {
                let credential = refreshed.credential.clone();
                *current = refreshed;
                Ok(Some(credential))
            }
            Err(error) if now < current.expires_at => {
                log::warn!("failed to refresh Iceberg vended credentials early: {error}");
                current.refresh_at = current
                    .expires_at
                    .min(now + CREDENTIAL_REFRESH_RETRY_INTERVAL);
                Ok(Some(current.credential.clone()))
            }
            Err(error) => Err(error),
        }
    }
}

pub(crate) fn credential_refresh_at(table: &iceberg::table::Table) -> Option<Instant> {
    VendedCredential::from_table(table).map(|credential| credential.refresh_at)
}

pub(crate) fn with_refreshing_credentials(
    catalog: Arc<dyn iceberg::Catalog>,
    table: iceberg::table::Table,
) -> Result<iceberg::table::Table> {
    let Some(credential) = VendedCredential::from_table(&table) else {
        return Ok(table);
    };

    let table_ident = table.identifier().clone();
    let provider = Arc::new(CatalogCredentialProvider {
        catalog,
        table_ident: table_ident.clone(),
    });
    let loader = CustomAwsCredentialLoader::new(Arc::new(RefreshingAwsCredentialLoader::new(
        provider, credential,
    )));
    let (scheme, props, extensions) = table.file_io().clone().into_builder().into_parts();
    let file_io = FileIOBuilder::new(scheme)
        .with_props(props)
        .with_extensions(extensions)
        .with_extension(loader)
        .build()
        .map_err(|error| {
            ErrorCode::ReadTableDataError(format!(
                "Rebuild Iceberg table with refreshing credentials failed: {error:?}"
            ))
        })?;

    let builder = iceberg::table::Table::builder()
        .identifier(table_ident)
        .metadata(table.metadata_ref())
        .file_io(file_io)
        .readonly(table.readonly());
    let builder = match table.metadata_location() {
        Some(metadata_location) => builder.metadata_location(metadata_location),
        None => builder,
    };
    builder.build().map_err(|error| {
        ErrorCode::ReadTableDataError(format!(
            "Rebuild Iceberg table with refreshing credentials failed: {error:?}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    struct StaticProvider {
        calls: AtomicUsize,
        credential: VendedCredential,
    }

    #[async_trait]
    impl VendedCredentialProvider for StaticProvider {
        async fn load(&self) -> anyhow::Result<VendedCredential> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.credential.clone())
        }
    }

    fn test_credential(
        access_key_id: &str,
        refresh_at: Instant,
        expires_at: Instant,
    ) -> VendedCredential {
        VendedCredential {
            credential: AwsCredential {
                access_key_id: access_key_id.to_string(),
                secret_access_key: "secret".to_string(),
                session_token: Some("token".to_string()),
                expires_in: None,
            },
            refresh_at,
            expires_at,
        }
    }

    #[test]
    fn test_vended_credential_uses_safety_margin() {
        let now_system = UNIX_EPOCH + Duration::from_secs(1_000);
        let now_instant = Instant::now();
        let props = HashMap::from([
            ("s3.access-key-id".to_string(), "access".to_string()),
            ("s3.secret-access-key".to_string(), "secret".to_string()),
            ("s3.session-token".to_string(), "token".to_string()),
            ("expires-at-ms".to_string(), "4600000".to_string()),
        ]);
        let credential = VendedCredential::from_props(&props, now_system, now_instant).unwrap();

        assert_eq!(
            credential.refresh_at.duration_since(now_instant),
            Duration::from_secs(3_300)
        );
        assert_eq!(
            credential.expires_at.duration_since(now_instant),
            Duration::from_secs(3_600)
        );
    }

    #[tokio::test]
    async fn test_loader_refreshes_credentials_during_query() {
        let now = Instant::now();
        let provider = Arc::new(StaticProvider {
            calls: AtomicUsize::new(0),
            credential: test_credential(
                "refreshed",
                now + Duration::from_secs(600),
                now + Duration::from_secs(900),
            ),
        });
        let loader = RefreshingAwsCredentialLoader::new(
            provider.clone(),
            test_credential("expired", now, now),
        );

        let credential = loader
            .load_credential(Client::new())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(credential.access_key_id, "refreshed");
        assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
    }
}
