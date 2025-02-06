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

use async_trait::async_trait;
use databend_common_meta_api::BackgroundApiTestSuite;
use databend_common_meta_api::SchemaApiTestSuite;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_raft_store::mem_meta::MemMeta;
use test_harness::test;

use crate::testing::mem_meta_test_harness;

#[derive(Clone)]
pub struct MemMetaBuilder {}

#[async_trait]
impl kvapi::ApiBuilder<MemMeta> for MemMetaBuilder {
    async fn build(&self) -> MemMeta {
        MemMeta::default()
    }

    async fn build_cluster(&self) -> Vec<MemMeta> {
        unimplemented!("embedded meta does not support cluster mode")
    }
}

#[test(harness = mem_meta_test_harness)]
#[fastrace::trace]
async fn test_mem_meta_schema_api() -> anyhow::Result<()> {
    SchemaApiTestSuite::test_single_node(MemMetaBuilder {}).await?;
    Ok(())
}

#[test(harness = mem_meta_test_harness)]
#[fastrace::trace]
async fn test_mem_meta_background_api() -> anyhow::Result<()> {
    BackgroundApiTestSuite::test_single_node(MemMetaBuilder {}).await?;
    Ok(())
}
