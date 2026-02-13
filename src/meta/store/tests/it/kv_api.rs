// Copyright 2022 Datafuse Labs.
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

use async_trait::async_trait;
use databend_common_meta_store::MetaStore;
use databend_meta_kvapi::kvapi;
use databend_meta_runtime_api::TokioRuntime;

#[derive(Clone)]
struct MetaNodeUnitTestBuilder {}

#[async_trait]
impl kvapi::ApiBuilder<MetaStore> for MetaNodeUnitTestBuilder {
    async fn build(&self) -> MetaStore {
        MetaStore::new_local_testing::<TokioRuntime>().await
    }

    async fn build_cluster(&self) -> Vec<MetaStore> {
        todo!()
    }
}

/// It just tests the basic kv api to ensure the internal meta client handle works.
#[tokio::test]
async fn test_meta_node_kv_api() -> anyhow::Result<()> {
    let builder = MetaNodeUnitTestBuilder {};

    databend_meta_kvapi_test_suite::TestSuite {}
        .test_single_node(&builder)
        .await
}
