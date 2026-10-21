// Copyright 2026 Datafuse Labs
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

use std::result::Result as StdResult;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use databend_common_base::runtime::spawn;
use databend_common_config::InnerConfig;
use databend_common_meta_app::principal::ExternalAccessIntegration;
use databend_common_meta_app::principal::NetworkRule;
use databend_common_version::BUILD_INFO;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::KVStream;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::types::Change;
use databend_meta_client::types::TxnReply;
use databend_meta_client::types::TxnRequest;
use databend_meta_client::types::UpsertKV;
use databend_meta_client::types::errors::IncompleteStream;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use futures::stream::BoxStream;
use parking_lot::Mutex as SyncMutex;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tokio::time::timeout;

use super::*;

struct CountingKv {
    inner: Arc<dyn KVApi<Error = MetaError> + Send + Sync>,
    batches: SyncMutex<Vec<Vec<String>>>,
    fail: AtomicBool,
}

#[async_trait::async_trait]
impl KVApi for CountingKv {
    type Error = MetaError;

    async fn upsert_kv(&self, req: UpsertKV) -> StdResult<Change<Vec<u8>>, MetaError> {
        self.inner.upsert_kv(req).await
    }

    async fn get_many_kv(
        &self,
        keys: BoxStream<'static, StdResult<String, MetaError>>,
    ) -> StdResult<KVStream<MetaError>, MetaError> {
        let keys = keys.try_collect::<Vec<_>>().await?;
        self.batches.lock().push(keys.clone());
        if self.fail.load(Ordering::Relaxed) {
            return Err(IncompleteStream::new(keys.len() as u64, 0).into());
        }
        self.inner
            .get_many_kv(stream::iter(keys.into_iter().map(Ok)).boxed())
            .await
    }

    async fn list_kv(
        &self,
        req: ListOptions<'_, str>,
    ) -> StdResult<KVStream<MetaError>, MetaError> {
        self.inner.list_kv(req).await
    }

    async fn transaction(&self, req: TxnRequest) -> StdResult<TxnReply, MetaError> {
        self.inner.transaction(req).await
    }
}

async fn provider(
    enable_udf: bool,
) -> anyhow::Result<(Arc<UserApiProvider>, Arc<CountingKv>, Tenant)> {
    let name = std::thread::current().name().unwrap().to_owned();
    GlobalInstance::init_testing(&name);
    let mut config = InnerConfig::default();
    config.query.common.enable_udf_server = enable_udf;
    GlobalConfig::init(&config, &BUILD_INFO)?;
    let tenant = Tenant::new_literal("external_access_read_tests");
    let mut provider = UserApiProvider::try_create_simple(RpcClientConf::empty(), &tenant).await?;
    let api = Arc::get_mut(&mut provider).unwrap();
    let count = Arc::new(CountingKv {
        inner: api.client.clone(),
        batches: SyncMutex::new(Vec::new()),
        fail: AtomicBool::new(false),
    });
    api.client = count.clone();
    api.network_rule_api(&tenant)
        .add(
            NetworkRule {
                name: "shared".into(),
                value_list: vec!["allowed.example.com:443".into()],
            },
            &CreateOption::Create,
        )
        .await?;
    for name in ["a", "b"] {
        api.external_access_integration_api(&tenant)
            .add(
                ExternalAccessIntegration {
                    name: name.into(),
                    allowed_network_rules: vec!["shared".into(), "shared".into()],
                    enabled: true,
                },
                &CreateOption::Create,
            )
            .await?;
    }
    count.batches.lock().clear();
    Ok((provider, count, tenant))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn external_access_busy_cache_batches_and_fails_closed() -> anyhow::Result<()> {
    let (provider, count, tenant) = provider(true).await?;
    // Hold the outer locks. Validation must not queue behind these guards.
    let _integrations = provider
        .external_access_integration_cache
        .as_ref()
        .unwrap()
        .lock()
        .await;
    let _rules = provider.network_rule_cache.as_ref().unwrap().lock().await;
    let names = vec!["b".into(), "a".into(), "a".into()];
    timeout(
        Duration::from_secs(1),
        provider.validate_external_access(&tenant, "https://allowed.example.com", &names),
    )
    .await??;
    let batches = count.batches.lock().clone();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].len(), 2); // duplicate integration removed
    assert_eq!(batches[1].len(), 1); // shared / duplicate rules removed

    count.fail.store(true, Ordering::Relaxed);
    let err = timeout(
        Duration::from_secs(1),
        provider.validate_external_access(&tenant, "https://allowed.example.com", &names),
    )
    .await?
    .unwrap_err();
    assert_eq!(err.code(), ErrorCode::META_SERVICE_ERROR);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn external_access_initializing_watch_does_not_block() -> anyhow::Result<()> {
    let (mut provider, count, tenant) = provider(true).await?;
    // TCP can connect, but no server accepts/answers the handshake. Ordinary KV
    // reads still use the healthy provider client. This leaves cache sync pending.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = MetaGrpcClient::<DatabendRuntime>::try_create(
        vec![listener.local_addr()?.to_string()],
        "root",
        "test",
        Some(Duration::from_secs(60)),
        Some(Duration::from_secs(60)),
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )?;
    let api = Arc::get_mut(&mut provider).unwrap();
    api.external_access_integration_cache = Some(Mutex::new(
        Cache::new(
            client.clone(),
            "unavailable_integrations",
            "unavailable integrations",
        )
        .await,
    ));
    api.network_rule_cache = Some(Mutex::new(
        Cache::new(client, "unavailable_rules", "unavailable rules").await,
    ));
    let mut tasks = Vec::new();
    for _ in 0..32 {
        let provider = provider.clone();
        let tenant = tenant.clone();
        tasks.push(spawn(async move {
            provider
                .validate_external_access(&tenant, "https://allowed.example.com", &[
                    "a".into(),
                    "b".into(),
                ])
                .await
        }));
    }
    timeout(Duration::from_secs(2), async {
        for task in tasks {
            task.await??;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let batches = count.batches.lock();
    assert_eq!(batches.len(), 64); // two batched reads per request, no singleflight
    assert!(
        batches
            .iter()
            .all(|keys| keys.len() == 1 || keys.len() == 2)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn external_access_disabled_udf_creates_no_watchers() -> anyhow::Result<()> {
    let (provider, count, tenant) = provider(false).await?;
    assert!(provider.network_rule_cache.is_none());
    assert!(provider.external_access_integration_cache.is_none());
    provider
        .validate_external_access(&tenant, "https://allowed.example.com", &["a".into()])
        .await?;
    assert_eq!(count.batches.lock().len(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn external_access_watch_is_tenant_scoped() -> anyhow::Result<()> {
    let (provider, count, tenant) = provider(true).await?;
    let foreign = Tenant::new_literal("external_access_read_tests_other");
    provider
        .network_rule_api(&foreign)
        .add(
            NetworkRule {
                name: "shared".into(),
                value_list: vec!["foreign.example.com:443".into()],
            },
            &CreateOption::Create,
        )
        .await?;
    provider
        .external_access_integration_api(&foreign)
        .add(
            ExternalAccessIntegration {
                name: "a".into(),
                allowed_network_rules: vec!["shared".into()],
                enabled: true,
            },
            &CreateOption::Create,
        )
        .await?;
    sleep(Duration::from_millis(500)).await;
    let mut rules = provider.network_rule_cache.as_ref().unwrap().lock().await;
    let prefix = TIdent::<NetworkRuleResource>::new(&tenant, "").to_string_key();
    assert_eq!(rules.try_list_dir(&prefix).await?.len(), 1);
    let prefix = TIdent::<NetworkRuleResource>::new(&foreign, "").to_string_key();
    assert!(rules.try_list_dir(&prefix).await?.is_empty());
    drop(rules);
    let mut integrations = provider
        .external_access_integration_cache
        .as_ref()
        .unwrap()
        .lock()
        .await;
    let prefix = TIdent::<ExternalAccessIntegrationResource>::new(&tenant, "").to_string_key();
    assert_eq!(integrations.try_list_dir(&prefix).await?.len(), 2);
    let prefix = TIdent::<ExternalAccessIntegrationResource>::new(&foreign, "").to_string_key();
    assert!(integrations.try_list_dir(&prefix).await?.is_empty());
    drop(integrations);
    count.batches.lock().clear();
    provider
        .validate_external_access(&foreign, "https://foreign.example.com", &["a".into()])
        .await?;
    assert_eq!(count.batches.lock().len(), 2);
    Ok(())
}
