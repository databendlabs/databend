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

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_raft::raft::Entry;
use async_raft::raft::EntryNormal;
use async_raft::raft::EntryPayload;
use async_raft::raft::MembershipConfig;
use async_raft::LogId;
use common_metatypes::Database;
use common_metatypes::KVMeta;
use common_metatypes::KVValue;
use common_metatypes::MatchSeq;
use common_metatypes::Operation;
use common_metatypes::SeqValue;
use common_runtime::tokio;
use common_tracing::tracing;
use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::meta_service::testing::pretty_snapshot;
use crate::meta_service::testing::pretty_snapshot_iter;
use crate::meta_service::testing::snapshot_logs;
use crate::meta_service::Cmd;
use crate::meta_service::LogEntry;
use crate::raft::state_machine::AppliedState;
use crate::raft::state_machine::Node;
use crate::raft::state_machine::Replication;
use crate::raft::state_machine::SerializableSnapshot;
use crate::raft::state_machine::Slot;
use crate::raft::state_machine::StateMachine;
use crate::tests::service::new_test_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_assign_rand_nodes_to_slot() -> anyhow::Result<()> {
    // - Create a state machine with 3 node 1,3,5.
    // - Assert that expected number of nodes are assigned to a slot.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc = new_test_context();
    let mut sm = StateMachine::open(&tc.config.meta_config, 1).await?;
    sm.nodes()
        .append(&[
            (1, Node::default()),
            (3, Node::default()),
            (5, Node::default()),
        ])
        .await?;

    sm.slots = vec![Slot::default(), Slot::default(), Slot::default()];
    sm.replication = Replication::Mirror(3);

    // assign all node to slot 2
    sm.assign_rand_nodes_to_slot(2)?;
    assert_eq!(sm.slots[2].node_ids, vec![1, 3, 5]);

    // assign all node again to slot 2
    sm.assign_rand_nodes_to_slot(2)?;
    assert_eq!(sm.slots[2].node_ids, vec![1, 3, 5]);

    // assign 1 node again to slot 1
    sm.replication = Replication::Mirror(1);
    sm.assign_rand_nodes_to_slot(1)?;
    assert_eq!(1, sm.slots[1].node_ids.len());

    let id = sm.slots[1].node_ids[0];
    assert!(id == 1 || id == 3 || id == 5);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_init_slots() -> anyhow::Result<()> {
    // - Create a state machine with 3 node 1,3,5.
    // - Initialize all slots.
    // - Assert slot states.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc = new_test_context();
    let mut sm = StateMachine::open(&tc.config.meta_config, 1).await?;
    sm.nodes()
        .append(&[
            (1, Node::default()),
            (3, Node::default()),
            (5, Node::default()),
        ])
        .await?;

    sm.slots = vec![Slot::default(), Slot::default(), Slot::default()];
    sm.replication = Replication::Mirror(1);

    sm.init_slots()?;
    for slot in sm.slots.iter() {
        assert_eq!(1, slot.node_ids.len());

        let id = slot.node_ids[0];
        assert!(id == 1 || id == 3 || id == 5);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_builder() -> anyhow::Result<()> {
    // - Assert default state machine builder
    // - Assert customized state machine builder

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    {
        let tc = new_test_context();
        let sm = StateMachine::open(&tc.config.meta_config, 1).await?;

        assert_eq!(3, sm.slots.len());
        let Replication::Mirror(n) = sm.replication;
        assert_eq!(1, n);
    }

    {
        let tc = new_test_context();
        let sm = StateMachine::open(&tc.config.meta_config, 1).await?;

        let sm = StateMachine::initializer()
            .slots(5)
            .mirror_replication(7)
            .init(sm);

        assert_eq!(5, sm.slots.len());
        let Replication::Mirror(n) = sm.replication;
        assert_eq!(7, n);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_non_dup_incr_seq() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc = new_test_context();
    let mut sm = StateMachine::open(&tc.config.meta_config, 1).await?;

    for i in 0..3 {
        // incr "foo"

        let resp = sm
            .apply_cmd(&Cmd::IncrSeq {
                key: "foo".to_string(),
            })
            .await?;
        assert_eq!(AppliedState::Seq { seq: i + 1 }, resp);

        // incr "bar"

        let resp = sm
            .apply_cmd(&Cmd::IncrSeq {
                key: "bar".to_string(),
            })
            .await?;
        assert_eq!(AppliedState::Seq { seq: i + 1 }, resp);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_incr_seq() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc = new_test_context();
    let mut sm = StateMachine::open(&tc.config.meta_config, 1).await?;

    let cases = crate::meta_service::raftmeta_test::cases_incr_seq();

    for (name, txid, k, want) in cases.iter() {
        let resp = sm
            .apply(&Entry {
                log_id: LogId { term: 0, index: 5 },
                payload: EntryPayload::Normal(EntryNormal {
                    data: LogEntry {
                        txid: txid.clone(),
                        cmd: Cmd::IncrSeq { key: k.to_string() },
                    },
                }),
            })
            .await?;
        assert_eq!(AppliedState::Seq { seq: *want }, resp, "{}", name);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_add_database() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc = new_test_context();
    let mut m = StateMachine::open(&tc.config.meta_config, 1).await?;

    struct T {
        name: &'static str,
        prev: Option<Database>,
        result: Option<Database>,
    }

    fn case(name: &'static str, prev: Option<u64>, result: Option<u64>) -> T {
        let prev = prev.map(|id| Database {
            database_id: id,
            ..Default::default()
        });
        let result = result.map(|id| Database {
            database_id: id,
            ..Default::default()
        });
        T { name, prev, result }
    }

    let cases: Vec<T> = vec![
        case("foo", None, Some(1)),
        case("foo", Some(1), Some(1)),
        case("bar", None, Some(2)),
        case("bar", Some(2), Some(2)),
        case("wow", None, Some(3)),
    ];

    for (i, c) in cases.iter().enumerate() {
        // add

        let resp = m
            .apply_cmd(&Cmd::CreateDatabase {
                name: c.name.to_string(),
                if_not_exists: true,
                db: Default::default(),
            })
            .await?;
        assert_eq!(
            AppliedState::DataBase {
                prev: c.prev.clone(),
                result: c.result.clone(),
            },
            resp,
            "{}-th",
            i
        );

        // get

        let want = match (&c.prev, &c.result) {
            (Some(ref a), _) => a.database_id,
            (_, Some(ref b)) => b.database_id,
            _ => {
                panic!("both none");
            }
        };

        let got = m
            .get_database(c.name)
            .ok_or_else(|| anyhow::anyhow!("db not found: {}", c.name));
        assert_eq!(want, got.unwrap().database_id);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_non_dup_generic_kv_upsert_get() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc = new_test_context();
    let mut sm = StateMachine::open(&tc.config.meta_config, 1).await?;

    struct T {
        // input:
        key: String,
        seq: MatchSeq,
        value: Vec<u8>,
        value_meta: Option<KVMeta>,
        // want:
        prev: Option<SeqValue<KVValue>>,
        result: Option<SeqValue<KVValue>>,
    }

    fn case(
        name: &'static str,
        seq: MatchSeq,
        value: &'static str,
        meta: Option<u64>,
        prev: Option<(u64, &'static str)>,
        result: Option<(u64, &'static str)>,
    ) -> T {
        let m = meta.map(|x| KVMeta { expire_at: Some(x) });
        T {
            key: name.to_string(),
            seq,
            value: value.to_string().into_bytes(),
            value_meta: m.clone(),
            prev: prev.map(|(a, b)| {
                (a, KVValue {
                    meta: None,
                    value: b.as_bytes().to_vec(),
                })
            }),
            result: result.map(|(a, b)| {
                (a, KVValue {
                    meta: m,
                    value: b.as_bytes().to_vec(),
                })
            }),
        }
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let cases: Vec<T> = vec![
        case("foo", MatchSeq::Exact(5), "b", None, None, None),
        case("foo", MatchSeq::Any, "a", None, None, Some((1, "a"))),
        case(
            "foo",
            MatchSeq::Any,
            "b",
            None,
            Some((1, "a")),
            Some((2, "b")),
        ),
        case(
            "foo",
            MatchSeq::Exact(5),
            "b",
            None,
            Some((2, "b")),
            Some((2, "b")),
        ),
        case("bar", MatchSeq::Exact(0), "x", None, None, Some((3, "x"))),
        case(
            "bar",
            MatchSeq::Exact(0),
            "y",
            None,
            Some((3, "x")),
            Some((3, "x")),
        ),
        case(
            "bar",
            MatchSeq::GE(1),
            "y",
            None,
            Some((3, "x")),
            Some((4, "y")),
        ),
        // expired at once
        case("wow", MatchSeq::Any, "y", Some(0), None, Some((5, "y"))),
        // expired value does not exist
        case(
            "wow",
            MatchSeq::Any,
            "y",
            Some(now + 1000),
            None,
            Some((6, "y")),
        ),
    ];

    for (i, c) in cases.iter().enumerate() {
        let mes = format!("{}-th: {}({:?})={:?}", i, c.key, c.seq, c.value);

        // write

        let resp = sm
            .apply_cmd(&Cmd::UpsertKV {
                key: c.key.clone(),
                seq: c.seq,
                value: Some(c.value.clone()).into(),
                value_meta: c.value_meta.clone(),
            })
            .await?;
        assert_eq!(
            AppliedState::KV {
                prev: c.prev.clone(),
                result: c.result.clone(),
            },
            resp,
            "write: {}",
            mes,
        );

        // get

        let want = match (&c.prev, &c.result) {
            (_, Some(ref b)) => Some(b.clone()),
            (Some(ref a), _) => Some(a.clone()),
            _ => None,
        };
        let want = match want {
            None => None,
            Some(ref w) => {
                // trick: in this test all expired timestamps are all 0
                if w.1 < now {
                    None
                } else {
                    want
                }
            }
        };

        let got = sm.get_kv(&c.key)?;
        assert_eq!(want, got, "get: {}", mes,);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_non_dup_generic_kv_value_meta() -> anyhow::Result<()> {
    // - Update a value-meta of None does nothing.
    // - Update a value-meta of Some() only updates the value-meta.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc = new_test_context();
    let mut sm = StateMachine::open(&tc.config.meta_config, 1).await?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let key = "value_meta_foo".to_string();

    tracing::info!("--- update meta of a nonexistent record");

    let resp = sm
        .apply_cmd(&Cmd::UpsertKV {
            key: key.clone(),
            seq: MatchSeq::Any,
            value: Operation::AsIs,
            value_meta: Some(KVMeta {
                expire_at: Some(now + 10),
            }),
        })
        .await?;

    assert_eq!(
        AppliedState::KV {
            prev: None,
            result: None,
        },
        resp,
        "update meta of None does nothing",
    );

    tracing::info!("--- update meta of a existent record");

    // add a record
    let _resp = sm
        .apply_cmd(&Cmd::UpsertKV {
            key: key.clone(),
            seq: MatchSeq::Any,
            value: Operation::Update(b"value_meta_bar".to_vec()),
            value_meta: Some(KVMeta {
                expire_at: Some(now + 10),
            }),
        })
        .await?;

    // update the meta of the record
    let _resp = sm
        .apply_cmd(&Cmd::UpsertKV {
            key: key.clone(),
            seq: MatchSeq::Any,
            value: Operation::AsIs,
            value_meta: Some(KVMeta {
                expire_at: Some(now + 20),
            }),
        })
        .await?;

    tracing::info!("--- read the original value and updated meta");

    let got = sm.get_kv(&key)?;
    let got = got.unwrap();

    assert_eq!(
        KVValue {
            meta: Some(KVMeta {
                expire_at: Some(now + 20)
            }),
            value: b"value_meta_bar".to_vec()
        },
        got.1,
        "update meta of None does nothing",
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_non_dup_generic_kv_delete() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    struct T {
        // input:
        key: String,
        seq: MatchSeq,
        // want:
        prev: Option<SeqValue<KVValue>>,
        result: Option<SeqValue<KVValue>>,
    }

    fn case(
        name: &'static str,
        seq: MatchSeq,
        prev: Option<(u64, &'static str)>,
        result: Option<(u64, &'static str)>,
    ) -> T {
        T {
            key: name.to_string(),
            seq,
            prev: prev.map(|(a, b)| {
                (a, KVValue {
                    meta: None,
                    value: b.as_bytes().to_vec(),
                })
            }),
            result: result.map(|(a, b)| {
                (a, KVValue {
                    meta: None,
                    value: b.as_bytes().to_vec(),
                })
            }),
        }
    }

    let prev = Some((1u64, "x"));

    let cases: Vec<T> = vec![
        case("foo", MatchSeq::Any, prev, None),
        case("foo", MatchSeq::Exact(1), prev, None),
        case("foo", MatchSeq::Exact(0), prev, prev),
        case("foo", MatchSeq::GE(1), prev, None),
        case("foo", MatchSeq::GE(2), prev, prev),
    ];

    for (i, c) in cases.iter().enumerate() {
        let mes = format!("{}-th: {}({})", i, c.key, c.seq);

        let tc = new_test_context();
        let mut sm = StateMachine::open(&tc.config.meta_config, 1).await?;

        // prepare an record
        sm.apply_cmd(&Cmd::UpsertKV {
            key: "foo".to_string(),
            seq: MatchSeq::Any,
            value: Some(b"x".to_vec()).into(),
            value_meta: None,
        })
        .await?;

        // delete
        let resp = sm
            .apply_cmd(&Cmd::UpsertKV {
                key: c.key.clone(),
                seq: c.seq,
                value: Operation::Delete,
                value_meta: None,
            })
            .await?;
        assert_eq!(
            AppliedState::KV {
                prev: c.prev.clone(),
                result: c.result.clone(),
            },
            resp,
            "delete: {}",
            mes,
        );

        // read it to ensure the modified state.
        let want = &c.result;
        let got = sm.get_kv(&c.key)?;
        assert_eq!(want, &got, "get: {}", mes,);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_add_file() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc = new_test_context();
    let mut sm = StateMachine::open(&tc.config.meta_config, 1).await?;

    let cases = crate::meta_service::raftmeta_test::cases_add_file();

    for (name, txid, k, v, want_prev, want_result) in cases.iter() {
        let resp = sm
            .apply(&Entry {
                log_id: LogId { term: 0, index: 5 },
                payload: EntryPayload::Normal(EntryNormal {
                    data: LogEntry {
                        txid: txid.clone(),
                        cmd: Cmd::AddFile {
                            key: k.to_string(),
                            value: v.to_string(),
                        },
                    },
                }),
            })
            .await?;
        assert_eq!(
            AppliedState::String {
                prev: want_prev.clone(),
                result: want_result.clone()
            },
            resp,
            "{}",
            name
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_set_file() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc = new_test_context();
    let mut sm = StateMachine::open(&tc.config.meta_config, 1).await?;

    let cases = crate::meta_service::raftmeta_test::cases_set_file();

    for (name, txid, k, v, want_prev, want_result) in cases.iter() {
        let resp = sm
            .apply(&Entry {
                log_id: LogId { term: 0, index: 5 },
                payload: EntryPayload::Normal(EntryNormal {
                    data: LogEntry {
                        txid: txid.clone(),
                        cmd: Cmd::SetFile {
                            key: k.to_string(),
                            value: v.to_string(),
                        },
                    },
                }),
            })
            .await?;
        assert_eq!(
            AppliedState::String {
                prev: want_prev.clone(),
                result: want_result.clone()
            },
            resp,
            "{}",
            name
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_snapshot() -> anyhow::Result<()> {
    // - Feed logs into state machine.
    // - Take a snapshot and examine the data

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let tc = new_test_context();
    let mut sm = StateMachine::open(&tc.config.meta_config, 0).await?;

    let (logs, want) = snapshot_logs();
    // TODO(xp): following logs are not saving to sled yet:
    //           database
    //           table
    //           slots
    //           replication

    for l in logs.iter() {
        sm.apply(l).await?;
    }

    let (it, last, mem, id) = sm.snapshot()?;

    assert_eq!(LogId { term: 1, index: 9 }, last);
    assert!(id.starts_with(&format!("{}-{}-", 1, 9)));
    assert_eq!(
        MembershipConfig {
            members: btreeset![4, 5, 6],
            members_after_consensus: None,
        },
        mem
    );

    let res = pretty_snapshot_iter(it);
    assert_eq!(want, res);

    // test serialized snapshot

    {
        let (it, _last, _mem, _id) = sm.snapshot()?;

        let data = StateMachine::serialize_snapshot(it)?;

        let d: SerializableSnapshot = serde_json::from_slice(&data)?;
        let res = pretty_snapshot(&d.kvs);

        assert_eq!(want, res);
    }

    Ok(())
}
