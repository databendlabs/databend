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

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use databend_common_meta_store::MetaStore;
use databend_meta_client::ClientHandle;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::kvapi;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_test_harness::MetaSrvTestContext;
use databend_meta_test_harness::start_metasrv;
use databend_meta_test_harness::start_metasrv_cluster;

async fn grpc_client(
    tc: &MetaSrvTestContext<DatabendRuntime>,
) -> anyhow::Result<Arc<ClientHandle<DatabendRuntime>>> {
    let addr = tc
        .config
        .grpc
        .api_address()
        .ok_or_else(|| anyhow::anyhow!("gRPC port not assigned yet"))?;

    let client = MetaGrpcClient::<DatabendRuntime>::try_create(
        vec![addr],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )?;
    Ok(client)
}

/// Build metasrv or metasrv cluster, returns the clients
#[derive(Clone)]
pub struct MetaSrvBuilder {
    pub test_contexts: Arc<Mutex<Vec<MetaSrvTestContext<DatabendRuntime>>>>,
}

#[async_trait]
impl kvapi::ApiBuilder<MetaStore> for MetaSrvBuilder {
    async fn build(&self) -> MetaStore {
        let (tc, addr) = start_metasrv::<DatabendRuntime>().await.unwrap();

        let client = MetaGrpcClient::<DatabendRuntime>::try_create(
            vec![addr],
            "root",
            "xxx",
            None,
            None,
            None,
            DEFAULT_GRPC_MESSAGE_SIZE,
        )
        .unwrap();

        {
            let mut tcs = self.test_contexts.lock().unwrap();
            tcs.push(tc);
        }

        MetaStore::R(client)
    }

    async fn build_cluster(&self) -> Vec<MetaStore> {
        let tcs = start_metasrv_cluster::<DatabendRuntime>(&[0, 1, 2])
            .await
            .unwrap();

        let cluster = vec![
            MetaStore::R(grpc_client(&tcs[0]).await.unwrap()),
            MetaStore::R(grpc_client(&tcs[1]).await.unwrap()),
            MetaStore::R(grpc_client(&tcs[2]).await.unwrap()),
        ];

        {
            let mut test_contexts = self.test_contexts.lock().unwrap();
            test_contexts.extend(tcs);
        }

        cluster
    }
}
