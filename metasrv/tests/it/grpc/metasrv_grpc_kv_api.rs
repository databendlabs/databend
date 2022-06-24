// Copyright 2021 Datafuse Labs.
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
use common_base::base::tokio;
use common_meta_api::KVApiBuilder;
use common_meta_api::KVApiTestSuite;
use common_meta_grpc::ClientHandle;
use common_meta_grpc::MetaGrpcClient;
use common_tracing::tracing;

use crate::init_meta_ut;
use crate::tests::service::start_metasrv_cluster;
use crate::tests::service::MetaSrvTestContext;
use crate::tests::start_metasrv;

struct Builder {
    pub test_contexts: Arc<Mutex<Vec<MetaSrvTestContext>>>,
}

#[async_trait]
impl KVApiBuilder<Arc<ClientHandle>> for Builder {
    async fn build(&self) -> Arc<ClientHandle> {
        let (tc, addr) = start_metasrv().await.unwrap();

        let client = MetaGrpcClient::try_create(
            vec![addr],
            "root",
            "xxx",
            None,
            Some(Duration::from_secs(10)),
            None,
        )
        .unwrap();

        {
            let mut tcs = self.test_contexts.lock().unwrap();
            tcs.push(tc);
        }

        client
    }

    async fn build_cluster(&self) -> Vec<Arc<ClientHandle>> {
        let tcs = start_metasrv_cluster(&[0, 1, 2]).await.unwrap();

        let cluster = vec![
            tcs[0].grpc_client().await.unwrap(),
            tcs[1].grpc_client().await.unwrap(),
            tcs[2].grpc_client().await.unwrap(),
        ];

        {
            let mut test_contexts = self.test_contexts.lock().unwrap();
            test_contexts.extend(tcs);
        }

        cluster
    }
}

#[async_entry::test(worker_threads = 3, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_metasrv_kv_api() -> anyhow::Result<()> {
    let builder = Builder {
        test_contexts: Arc::new(Mutex::new(vec![])),
    };

    KVApiTestSuite {}.test_all(builder).await
}
