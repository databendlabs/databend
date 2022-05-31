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
use common_base::base::tokio;
use common_meta_api::SchemaApiTestSuite;
use common_meta_raft_store::state_machine::StateMachine;

use crate::testing::new_raft_test_context;

#[cfg(feature = "mock_utc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_embedded_table_drop_out_of_retention_time_history() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_raft_store_ut!();
    let _ent = ut_span.enter();
    let tc = new_raft_test_context();
    let sm = StateMachine::open(&tc.raft_config, 1).await?;

    SchemaApiTestSuite {}
        .table_drop_out_of_retention_time_history(&sm)
        .await
}

#[cfg(feature = "mock_utc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_embedded_database_drop_out_of_retention_time_history() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_raft_store_ut!();
    let _ent = ut_span.enter();
    let tc = new_raft_test_context();
    let sm = StateMachine::open(&tc.raft_config, 1).await?;

    SchemaApiTestSuite {}
        .database_drop_out_of_retention_time_history(&sm)
        .await
}
