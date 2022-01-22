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

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use common_base::tokio;
use common_meta_api::KVApiBuilder;
use common_meta_api::KVApiTestSuite;
use common_tracing::tracing::Instrument;
use databend_meta::meta_service::MetaNode;
use maplit::btreeset;

use crate::init_meta_ut;
use crate::meta_node::meta_node_all::start_meta_node_cluster;
use crate::meta_node::meta_node_all::start_meta_node_leader;
use crate::tests::service::MetaSrvTestContext;

struct MetaNodeUnitTestBuilder {
    pub test_contexts: Arc<Mutex<Vec<MetaSrvTestContext>>>,
}

#[async_trait]
impl KVApiBuilder<Arc<MetaNode>> for MetaNodeUnitTestBuilder {
    async fn build(&self) -> Arc<MetaNode> {
        let (_id, tc) = start_meta_node_leader().await.unwrap();

        let meta_node = tc.meta_node();

        {
            let mut tcs = self.test_contexts.lock().unwrap();
            tcs.push(tc);
        }

        meta_node
    }

    async fn build_cluster(&self) -> Vec<Arc<MetaNode>> {
        let (_log_index, tcs) = start_meta_node_cluster(btreeset! {0,1,2}, btreeset! {3,4})
            .await
            .unwrap();

        let cluster = vec![
            tcs[0].meta_node(),
            tcs[1].meta_node(),
            tcs[2].meta_node(),
            tcs[3].meta_node(),
            tcs[4].meta_node(),
        ];

        {
            let mut test_contexts = self.test_contexts.lock().unwrap();
            test_contexts.extend(tcs);
        }

        cluster
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_meta_node_kv_api() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();

    let builder = MetaNodeUnitTestBuilder {
        test_contexts: Arc::new(Mutex::new(vec![])),
    };

    async { KVApiTestSuite {}.test_all(builder).await }
        .instrument(ut_span)
        .await
}
