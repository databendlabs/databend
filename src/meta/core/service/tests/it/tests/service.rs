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

use async_trait::async_trait;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_kvapi::kvapi;
use databend_common_version::BUILD_INFO;
use databend_meta_runtime::DatabendRuntime;
pub use databend_meta_test_harness::MetaSrvTestContext;
// Re-export from test-harness crate
pub use databend_meta_test_harness::make_grpc_client;
pub use databend_meta_test_harness::start_metasrv;
pub use databend_meta_test_harness::start_metasrv_cluster;
pub use databend_meta_test_harness::start_metasrv_with_context;

/// Build metasrv or metasrv cluster, returns the clients
#[derive(Clone)]
pub struct MetaSrvBuilder {
    pub test_contexts: Arc<Mutex<Vec<MetaSrvTestContext>>>,
}

#[async_trait]
impl kvapi::ApiBuilder<Arc<ClientHandle<DatabendRuntime>>> for MetaSrvBuilder {
    async fn build(&self) -> Arc<ClientHandle<DatabendRuntime>> {
        let (tc, addr) = start_metasrv().await.unwrap();

        let client = MetaGrpcClient::<DatabendRuntime>::try_create(
            vec![addr],
            BUILD_INFO.semver(),
            "root",
            "xxx",
            None,
            None,
            None,
        )
        .unwrap();

        {
            let mut tcs = self.test_contexts.lock().unwrap();
            tcs.push(tc);
        }

        client
    }

    async fn build_cluster(&self) -> Vec<Arc<ClientHandle<DatabendRuntime>>> {
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
