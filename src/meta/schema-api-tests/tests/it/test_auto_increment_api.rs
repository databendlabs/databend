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

//! Test AutoIncrementApiTestSuite against metasrv.

use std::sync::Arc;
use std::sync::Mutex;

use databend_common_meta_api::AutoIncrementApiTestSuite;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_test_harness::meta_service_test_harness;
use test_harness::test;

use crate::metasrv_builder::MetaSrvBuilder;

#[test(harness = meta_service_test_harness::<DatabendRuntime, _, _>)]
#[fastrace::trace]
async fn test_auto_increment_api_single_node() -> anyhow::Result<()> {
    let builder = MetaSrvBuilder {
        test_contexts: Arc::new(Mutex::new(vec![])),
    };

    AutoIncrementApiTestSuite::test_single_node(builder).await?;

    Ok(())
}
