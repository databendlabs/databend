// Copyright 2020 Datafuse Labs.
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

use async_raft::raft::Entry;
use async_raft::raft::EntryConfigChange;
use async_raft::raft::EntryPayload;
use async_raft::raft::MembershipConfig;
use async_raft::storage::HardState;
use async_raft::LogId;
use async_raft::RaftStorage;
use common_runtime::tokio;
use common_tracing::tracing;
use maplit::btreeset;

use crate::meta_service::state_machine::SerializableSnapshot;
use crate::meta_service::state_machine_meta::StateMachineMetaKey::LastMembership;
use crate::meta_service::testing::pretty_snapshot;
use crate::meta_service::testing::snapshot_logs;
use crate::meta_service::LogIndex;
use crate::meta_service::MetaStore;
use crate::meta_service::StateMachineMetaValue;
use crate::tests::service::init_store_unittest;
use crate::tests::service::new_test_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_store_restart() -> anyhow::Result<()> {
    // - Create a MetaStore
    // - Update MetaStore
    // - Close and reopen it
    // - Test state is restored

    // TODO check log
    // TODO check state machine

    let _log_guards = init_store_unittest(&common_tracing::func_name!());

    let id = 3;
    let mut tc = new_test_context();
    tc.config.id = id;

    tracing::info!("--- new MetaStore");
    {
        let ms = MetaStore::open_create(&tc.config, None, Some(())).await?;
        assert_eq!(id, ms.id);
        assert!(!ms.is_open());
        assert_eq!(None, ms.read_hard_state().await?);

        tracing::info!("--- update MetaStore");

        ms.save_hard_state(&HardState {
            current_term: 10,
            voted_for: Some(5),
        })
        .await?;
    }

    tracing::info!("--- reopen MetaStore");
    {
        let ms = MetaStore::open_create(&tc.config, Some(()), None).await?;
        assert_eq!(id, ms.id);
        assert!(ms.is_open());
        assert_eq!(
            Some(HardState {
                current_term: 10,
                voted_for: Some(5),
            }),
            ms.read_hard_state().await?
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_store_get_membership_from_log() -> anyhow::Result<()> {
    // - Create a MetaStore
    // - Append logs
    // - Get membership from log.

    let _log_guards = init_store_unittest(&common_tracing::func_name!());

    let id = 3;
    let mut tc = new_test_context();
    tc.config.id = id;

    tracing::info!("--- new MetaStore");
    let ms = MetaStore::open_create(&tc.config, None, Some(())).await?;

    let c0 = MembershipConfig {
        members: btreeset![4],
        members_after_consensus: None,
    };

    let c1 = MembershipConfig {
        members: btreeset![1, 2, 3],
        members_after_consensus: None,
    };

    let c2 = MembershipConfig {
        members: btreeset![1],
        members_after_consensus: Some(btreeset![2, 3,]),
    };

    let logs = vec![
        Entry {
            log_id: LogId { term: 1, index: 2 },
            payload: EntryPayload::ConfigChange(EntryConfigChange {
                membership: c1.clone(),
            }),
        },
        Entry {
            log_id: LogId { term: 1, index: 5 },
            payload: EntryPayload::ConfigChange(EntryConfigChange {
                membership: c2.clone(),
            }),
        },
    ];

    for l in logs.iter() {
        ms.log.insert(l).await?;
    }

    // no snapshot meta:

    let got = ms.get_membership_from_log(Some(2)).await?;
    assert_eq!(&c1, &got);

    let got = ms.get_membership_from_log(Some(1)).await?;
    assert_eq!(
        MembershipConfig {
            members: btreeset![3],
            members_after_consensus: None,
        },
        got,
        "no membership found in log or state machine, returning a default value"
    );

    // with snapshot meta:
    ms.state_machine
        .write()
        .await
        .sm_meta()
        .insert(
            &LastMembership,
            &StateMachineMetaValue::Membership(c0.clone()),
        )
        .await?;

    let got = ms.get_membership_from_log(None).await?;
    assert_eq!(&c2, &got);

    let got = ms.get_membership_from_log(Some(5)).await?;
    assert_eq!(&c2, &got);

    let got = ms.get_membership_from_log(Some(4)).await?;
    assert_eq!(&c1, &got);

    let got = ms.get_membership_from_log(Some(2)).await?;
    assert_eq!(&c1, &got);

    let got = ms.get_membership_from_log(Some(1)).await?;
    assert_eq!(
        MembershipConfig {
            members: btreeset![4],
            members_after_consensus: None,
        },
        got,
        "load from state machine"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_store_do_log_compaction_empty() -> anyhow::Result<()> {
    // - Create a MetaStore
    // - Create a snapshot check snapshot state

    let _log_guards = init_store_unittest(&common_tracing::func_name!());

    let id = 3;
    let mut tc = new_test_context();
    tc.config.id = id;

    let ms = MetaStore::open_create(&tc.config, None, Some(())).await?;

    tracing::info!("--- snapshot without any data");

    let curr_snap = ms.do_log_compaction().await?;
    assert_eq!(LogId { term: 0, index: 0 }, curr_snap.meta.last_log_id);

    assert_eq!(
        MembershipConfig {
            members: btreeset![],
            members_after_consensus: None,
        },
        curr_snap.meta.membership
    );

    tracing::info!("--- check snapshot");
    {
        let data = curr_snap.snapshot.into_inner();

        let ser_snap: SerializableSnapshot = serde_json::from_slice(&data)?;
        let res = pretty_snapshot(&ser_snap.kvs);
        tracing::debug!("res: {:?}", res);

        let want = vec![
            "[3, 2]:{\"Bool\":true}", // sm meta: init
        ]
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>();

        assert_eq!(want, res);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_store_do_log_compaction_1_snap_ptr_1_log() -> anyhow::Result<()> {
    // - Create a MetaStore
    // - Apply logs
    // - Create a snapshot check snapshot state

    let _log_guards = init_store_unittest(&common_tracing::func_name!());

    let id = 3;
    let mut tc = new_test_context();
    tc.config.id = id;

    let ms = MetaStore::open_create(&tc.config, None, Some(())).await?;

    tracing::info!("--- feed logs and state machine");

    let (logs, _want) = snapshot_logs();

    for (i, l) in logs.iter().enumerate() {
        ms.log.insert(l).await?;

        if i < 2 {
            ms.state_machine.write().await.apply(&l).await?;
        }
    }

    let curr_snap = ms.do_log_compaction().await?;
    assert_eq!(LogId { term: 1, index: 4 }, curr_snap.meta.last_log_id);
    assert_eq!(
        MembershipConfig {
            members: btreeset![1, 2, 3],
            members_after_consensus: None,
        },
        curr_snap.meta.membership
    );

    tracing::info!("--- check snapshot");
    {
        let data = curr_snap.snapshot.into_inner();

        let ser_snap: SerializableSnapshot = serde_json::from_slice(&data)?;
        let res = pretty_snapshot(&ser_snap.kvs);
        tracing::debug!("res: {:?}", res);

        let want = vec![
            "[3, 1]:{\"LogId\":{\"term\":1,\"index\":4}}", // sm meta: LastApplied
            "[3, 2]:{\"Bool\":true}",                      // sm meta: init
            "[3, 3]:{\"Membership\":{\"members\":[1,2,3],\"members_after_consensus\":null}}", // membership
            "[6, 97]:[1,{\"meta\":null,\"value\":[65]}]", // generic kv
            "[7, 103, 101, 110, 101, 114, 105, 99, 95, 107, 118]:1", // sequence: by upsertkv
        ]
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>();

        assert_eq!(want, res);
    }

    tracing::info!("--- check logs");
    {
        let log_indexes = ms.log.range_keys(..)?;
        assert_eq!(vec![5u64, 6, 8, 9], log_indexes);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_store_do_log_compaction_all_logs_with_memberchange() -> anyhow::Result<()> {
    // - Create a MetaStore
    // - Apply logs
    // - Create a snapshot check snapshot state

    let _log_guards = init_store_unittest(&common_tracing::func_name!());

    let id = 3;
    let mut tc = new_test_context();
    tc.config.id = id;

    let ms = MetaStore::open_create(&tc.config, None, Some(())).await?;

    tracing::info!("--- feed logs and state machine");

    let (logs, want) = snapshot_logs();

    for l in logs.iter() {
        ms.log.insert(l).await?;
        ms.state_machine.write().await.apply(&l).await?;
    }

    let curr_snap = ms.do_log_compaction().await?;
    assert_eq!(LogId { term: 1, index: 9 }, curr_snap.meta.last_log_id);
    assert_eq!(
        MembershipConfig {
            members: btreeset![4, 5, 6],
            members_after_consensus: None,
        },
        curr_snap.meta.membership
    );

    tracing::info!("--- check snapshot");
    {
        let data = curr_snap.snapshot.into_inner();

        let ser_snap: SerializableSnapshot = serde_json::from_slice(&data)?;
        let res = pretty_snapshot(&ser_snap.kvs);
        tracing::debug!("res: {:?}", res);

        assert_eq!(want, res);
    }

    tracing::info!("--- check logs");
    {
        let log_indexes = ms.log.range_keys(..)?;
        assert_eq!(Vec::<LogIndex>::new(), log_indexes);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_store_do_log_compaction_current_snapshot() -> anyhow::Result<()> {
    // - Create a MetaStore
    // - Apply logs
    // - Create a snapshot check snapshot state

    let _log_guards = init_store_unittest(&common_tracing::func_name!());

    let id = 3;
    let mut tc = new_test_context();
    tc.config.id = id;

    let ms = MetaStore::open_create(&tc.config, None, Some(())).await?;

    tracing::info!("--- feed logs and state machine");

    let (logs, want) = snapshot_logs();

    for l in logs.iter() {
        ms.log.insert(l).await?;
        ms.state_machine.write().await.apply(l).await?;
    }

    ms.do_log_compaction().await?;

    tracing::info!("--- check get_current_snapshot");

    let curr_snap = ms.get_current_snapshot().await?.unwrap();
    assert_eq!(LogId { term: 1, index: 9 }, curr_snap.meta.last_log_id);
    assert_eq!(
        MembershipConfig {
            members: btreeset![4, 5, 6],
            members_after_consensus: None,
        },
        curr_snap.meta.membership
    );

    tracing::info!("--- check snapshot");
    {
        let data = curr_snap.snapshot.into_inner();

        let ser_snap: SerializableSnapshot = serde_json::from_slice(&data)?;
        let res = pretty_snapshot(&ser_snap.kvs);
        tracing::debug!("res: {:?}", res);

        assert_eq!(want, res);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_store_install_snapshot() -> anyhow::Result<()> {
    // - Create a MetaStore
    // - Feed logs
    // - Create a snapshot
    // - Create a new MetaStore and restore it by install the snapshot

    let _log_guards = init_store_unittest(&common_tracing::func_name!());

    let (logs, want) = snapshot_logs();

    let id = 3;
    let snap;
    {
        let mut tc = new_test_context();
        tc.config.id = id;

        let ms = MetaStore::open_create(&tc.config, None, Some(())).await?;

        tracing::info!("--- feed logs and state machine");

        for l in logs.iter() {
            ms.log.insert(l).await?;
            ms.state_machine.write().await.apply(l).await?;
        }
        snap = ms.do_log_compaction().await?;
    }

    let data = snap.snapshot.into_inner();

    tracing::info!("--- reopen a new MetaStore to install snapshot");
    {
        let mut tc = new_test_context();
        tc.config.id = id;

        let ms = MetaStore::open_create(&tc.config, None, Some(())).await?;

        tracing::info!("--- rejected because old sm is not cleaned");
        {
            ms.raft_state.write_state_machine_id(&(1, 2)).await?;
            let res = ms.install_snapshot(&data).await;
            assert!(res.is_err(), "different ids disallow installing snapshot");
            assert!(res.unwrap_err().to_string().starts_with(
                "Code: 2404, displayText = another snapshot install is not finished yet: 1 2"
            ));
        }

        tracing::info!("--- install snapshot");
        {
            ms.raft_state.write_state_machine_id(&(0, 0)).await?;
            ms.install_snapshot(&data).await?;
        }

        tracing::info!("--- check installed meta");
        {
            assert_eq!((1, 1), ms.raft_state.read_state_machine_id()?);

            let mem = ms.state_machine.write().await.get_membership()?;
            assert_eq!(
                Some(MembershipConfig {
                    members: btreeset![4, 5, 6],
                    members_after_consensus: None,
                }),
                mem
            );

            let last_applied = ms.state_machine.write().await.get_last_applied()?;
            assert_eq!(LogId { term: 1, index: 9 }, last_applied);
        }

        tracing::info!("--- check snapshot");
        {
            let curr_snap = ms.do_log_compaction().await?;
            let data = curr_snap.snapshot.into_inner();

            let ser_snap: SerializableSnapshot = serde_json::from_slice(&data)?;
            let res = pretty_snapshot(&ser_snap.kvs);
            tracing::debug!("res: {:?}", res);

            assert_eq!(want, res);
        }
    }

    Ok(())
}

// TODO(xp): test finalize_snapshot_installation
