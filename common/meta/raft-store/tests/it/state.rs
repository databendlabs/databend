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
use common_base::tokio;
use common_meta_raft_store::state::RaftState;
use common_meta_sled_store::openraft;
use openraft::storage::HardState;

use crate::init_raft_store_ut;
use crate::testing::new_raft_test_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_create() -> anyhow::Result<()> {
    // - create a raft state
    // - creating another raft state in the same sled db should fail

    let (_log_guards, ut_span) = init_raft_store_ut!();
    let _ent = ut_span.enter();

    let mut tc = new_raft_test_context();
    let db = &tc.db;
    tc.raft_config.id = 3;
    let rs = RaftState::open_create(db, &tc.raft_config, None, Some(())).await?;
    let is_open = rs.is_open();

    assert_eq!(3, rs.id);
    assert!(!is_open);

    tc.raft_config.id = 4;
    let res = RaftState::open_create(db, &tc.raft_config, None, Some(())).await;
    assert!(res.is_err());
    assert_eq!(
        "raft state present id=3, can not create",
        res.unwrap_err().to_string()
    );

    tc.raft_config.id = 3;
    let res = RaftState::open_create(db, &tc.raft_config, None, Some(())).await;
    assert!(res.is_err());
    assert_eq!(
        "raft state present id=3, can not create",
        res.unwrap_err().to_string()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_open() -> anyhow::Result<()> {
    // - create a raft state
    // - open it.

    let (_log_guards, ut_span) = init_raft_store_ut!();
    let _ent = ut_span.enter();

    let mut tc = new_raft_test_context();
    let db = &tc.db;
    tc.raft_config.id = 3;
    let rs = RaftState::open_create(db, &tc.raft_config, None, Some(())).await?;
    let is_open = rs.is_open();

    assert_eq!(3, rs.id);
    assert!(!is_open);

    tc.raft_config.id = 1000;
    let rs = RaftState::open_create(db, &tc.raft_config, Some(()), None).await?;
    let is_open = rs.is_open();
    assert_eq!(3, rs.id);
    assert!(is_open);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_open_or_create() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_raft_store_ut!();
    let _ent = ut_span.enter();

    let mut tc = new_raft_test_context();
    let db = &tc.db;
    tc.raft_config.id = 3;
    let rs = RaftState::open_create(db, &tc.raft_config, Some(()), Some(())).await?;
    let is_open = rs.is_open();

    assert_eq!(3, rs.id);
    assert!(!is_open);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_write_read_hard_state() -> anyhow::Result<()> {
    // - create a raft state
    // - write hard_state and the read it.
    let (_log_guards, ut_span) = init_raft_store_ut!();
    let _ent = ut_span.enter();

    let mut tc = new_raft_test_context();
    let db = &tc.db;
    tc.raft_config.id = 3;
    let rs = RaftState::open_create(db, &tc.raft_config, None, Some(())).await?;

    assert_eq!(3, rs.id);

    // read got a None

    let got = rs.read_hard_state()?;
    assert_eq!(None, got);

    // write hard state

    let hs = HardState {
        current_term: 10,
        voted_for: Some(3),
    };

    rs.write_hard_state(&hs).await?;

    // read the written

    let got = rs.read_hard_state()?;
    assert_eq!(Some(hs), got);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_raft_state_write_read_state_machine_id() -> anyhow::Result<()> {
    // - create a raft state
    // - write state machine id and the read it.
    let (_log_guards, ut_span) = init_raft_store_ut!();
    let _ent = ut_span.enter();

    let mut tc = new_raft_test_context();
    let db = &tc.db;
    tc.raft_config.id = 3;
    let rs = RaftState::open_create(db, &tc.raft_config, None, Some(())).await?;

    // read got a None

    let got = rs.read_state_machine_id()?;
    assert_eq!((0, 0), got);

    // write hard state

    let smid = (1, 2);

    rs.write_state_machine_id(&smid).await?;

    // read the written

    let got = rs.read_state_machine_id()?;
    assert_eq!((1, 2), got);
    Ok(())
}
