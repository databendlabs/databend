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

use common_base::base::tokio;
use common_meta_raft_store::state_machine::testing::pretty_snapshot;
use common_meta_raft_store::state_machine::testing::snapshot_logs;
use common_meta_raft_store::state_machine::StateMachine;
use common_meta_types::new_log_id;

use crate::init_raft_store_ut;
use crate::testing::new_raft_test_context;

#[async_entry::test(
    worker_threads = 3,
    init = "init_raft_store_ut!()",
    tracing_span = "debug"
)]
async fn test_state_machine_snapshot() -> anyhow::Result<()> {
    // - Feed logs into state machine.
    // - Take a snapshot and examine the data

    let tc = new_raft_test_context();
    let sm = StateMachine::open(&tc.raft_config, 0).await?;

    let (logs, want) = snapshot_logs();

    for l in logs.iter() {
        sm.apply(l).await?;
    }

    // take snapshot and check it

    {
        let (snap, last_applied, last_membership, id) = sm.build_snapshot()?;

        assert_eq!(Some(new_log_id(1, 0, 9)), last_applied);
        assert_eq!(&Some(new_log_id(1, 0, 5)), last_membership.log_id());
        assert!(id.to_string().starts_with(&format!("{}-{}-{}-", 1, 0, 9)));

        let res = pretty_snapshot(&snap.kvs);
        assert_eq!(want, res);
    }

    Ok(())
}
