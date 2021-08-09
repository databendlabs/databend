// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::LogId;
use common_metatypes::Database;
use common_metatypes::MatchSeq;
use common_metatypes::SeqValue;
use common_runtime::tokio;
use pretty_assertions::assert_eq;

use crate::meta_service::sled_key_space;
use crate::meta_service::state_machine::Replication;
use crate::meta_service::AppliedState;
use crate::meta_service::Cmd;
use crate::meta_service::LogEntry;
use crate::meta_service::Node;
use crate::meta_service::Slot;
use crate::meta_service::StateMachine;
use crate::tests::service::init_store_unittest;
use crate::tests::service::new_test_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_assign_rand_nodes_to_slot() -> anyhow::Result<()> {
    // - Create a state machine with 3 node 1,3,5.
    // - Assert that expected number of nodes are assigned to a slot.

    init_store_unittest();

    let tc = new_test_context();
    let mut sm = StateMachine::open(&tc.config).await?;
    sm.sm_tree
        .key_space::<sled_key_space::Nodes>()
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

    init_store_unittest();

    let tc = new_test_context();
    let mut sm = StateMachine::open(&tc.config).await?;
    sm.sm_tree
        .key_space::<sled_key_space::Nodes>()
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

    init_store_unittest();

    {
        let tc = new_test_context();
        let sm = StateMachine::open(&tc.config).await?;

        assert_eq!(3, sm.slots.len());
        let n = match sm.replication {
            Replication::Mirror(x) => x,
        };
        assert_eq!(1, n);
    }

    {
        let tc = new_test_context();
        let sm = StateMachine::open(&tc.config).await?;

        let sm = StateMachine::initializer()
            .slots(5)
            .mirror_replication(7)
            .init(sm);

        assert_eq!(5, sm.slots.len());
        let n = match sm.replication {
            Replication::Mirror(x) => x,
        };
        assert_eq!(7, n);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_non_dup_incr_seq() -> anyhow::Result<()> {
    init_store_unittest();

    let tc = new_test_context();
    let mut sm = StateMachine::open(&tc.config).await?;

    for i in 0..3 {
        // incr "foo"

        let resp = sm
            .apply_non_dup(&LogEntry {
                txid: None,
                cmd: Cmd::IncrSeq {
                    key: "foo".to_string(),
                },
            })
            .await?;
        assert_eq!(AppliedState::Seq { seq: i + 1 }, resp);

        // incr "bar"

        let resp = sm
            .apply_non_dup(&LogEntry {
                txid: None,
                cmd: Cmd::IncrSeq {
                    key: "bar".to_string(),
                },
            })
            .await?;
        assert_eq!(AppliedState::Seq { seq: i + 1 }, resp);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_incr_seq() -> anyhow::Result<()> {
    init_store_unittest();

    let tc = new_test_context();
    let mut sm = StateMachine::open(&tc.config).await?;

    let cases = crate::meta_service::raftmeta_test::cases_incr_seq();

    for (name, txid, k, want) in cases.iter() {
        let resp = sm
            .apply(&LogId { term: 0, index: 5 }, &LogEntry {
                txid: txid.clone(),
                cmd: Cmd::IncrSeq { key: k.to_string() },
            })
            .await?;
        assert_eq!(AppliedState::Seq { seq: *want }, resp, "{}", name);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_add_database() -> anyhow::Result<()> {
    init_store_unittest();

    let tc = new_test_context();
    let mut m = StateMachine::open(&tc.config).await?;

    struct T {
        name: &'static str,
        prev: Option<Database>,
        result: Option<Database>,
    }

    fn case(name: &'static str, prev: Option<u64>, result: Option<u64>) -> T {
        let prev = match prev {
            None => None,
            Some(id) => Some(Database {
                database_id: id,
                ..Default::default()
            }),
        };
        let result = match result {
            None => None,
            Some(id) => Some(Database {
                database_id: id,
                ..Default::default()
            }),
        };
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
            .apply_non_dup(&LogEntry {
                txid: None,
                cmd: Cmd::CreateDatabase {
                    name: c.name.to_string(),
                    if_not_exists: true,
                    db: Default::default(),
                },
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
    init_store_unittest();

    let tc = new_test_context();
    let mut sm = StateMachine::open(&tc.config).await?;

    struct T {
        // input:
        key: String,
        seq: MatchSeq,
        value: Vec<u8>,
        // want:
        prev: Option<SeqValue>,
        result: Option<SeqValue>,
    }

    fn case(
        name: &'static str,
        seq: MatchSeq,
        value: &'static str,
        prev: Option<(u64, &'static str)>,
        result: Option<(u64, &'static str)>,
    ) -> T {
        T {
            key: name.to_string(),
            seq,
            value: value.to_string().into_bytes(),
            prev: prev.map(|(a, b)| (a, b.as_bytes().to_vec())),
            result: result.map(|(a, b)| (a, b.as_bytes().to_vec())),
        }
    }

    let cases: Vec<T> = vec![
        case("foo", MatchSeq::Exact(5), "b", None, None),
        case("foo", MatchSeq::Any, "a", None, Some((1, "a"))),
        case("foo", MatchSeq::Any, "b", Some((1, "a")), Some((2, "b"))),
        case(
            "foo",
            MatchSeq::Exact(5),
            "b",
            Some((2, "b")),
            Some((2, "b")),
        ),
        case("bar", MatchSeq::Exact(0), "x", None, Some((3, "x"))),
        case(
            "bar",
            MatchSeq::Exact(0),
            "y",
            Some((3, "x")),
            Some((3, "x")),
        ),
        case("bar", MatchSeq::GE(1), "y", Some((3, "x")), Some((4, "y"))),
    ];

    for (i, c) in cases.iter().enumerate() {
        let mes = format!("{}-th: {}({:?})={:?}", i, c.key, c.seq, c.value);

        // write

        let resp = sm
            .apply_non_dup(&LogEntry {
                txid: None,
                cmd: Cmd::UpsertKV {
                    key: c.key.clone(),
                    seq: c.seq.clone(),
                    value: Some(c.value.clone()),
                },
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

        let got = sm.get_kv(&c.key);
        assert_eq!(want, got, "get: {}", mes,);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_non_dup_generic_kv_delete() -> anyhow::Result<()> {
    init_store_unittest();

    struct T {
        // input:
        key: String,
        seq: MatchSeq,
        // want:
        prev: Option<SeqValue>,
        result: Option<SeqValue>,
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
            prev: prev.map(|(a, b)| (a, b.as_bytes().to_vec())),
            result: result.map(|(a, b)| (a, b.as_bytes().to_vec())),
        }
    }

    let prev = Some((1u64, "x"));

    let cases: Vec<T> = vec![
        case("foo", MatchSeq::Any, prev.clone(), None),
        case("foo", MatchSeq::Exact(1), prev.clone(), None),
        case("foo", MatchSeq::Exact(0), prev.clone(), prev.clone()),
        case("foo", MatchSeq::GE(1), prev.clone(), None),
        case("foo", MatchSeq::GE(2), prev.clone(), prev.clone()),
    ];

    for (i, c) in cases.iter().enumerate() {
        let mes = format!("{}-th: {}({})", i, c.key, c.seq);

        let tc = new_test_context();
        let mut sm = StateMachine::open(&tc.config).await?;

        // prepare an record
        sm.apply_non_dup(&LogEntry {
            txid: None,
            cmd: Cmd::UpsertKV {
                key: "foo".to_string(),
                seq: MatchSeq::Any,
                value: Some(b"x".to_vec()),
            },
        })
        .await?;

        // delete
        let resp = sm
            .apply_non_dup(&LogEntry {
                txid: None,
                cmd: Cmd::UpsertKV {
                    key: c.key.clone(),
                    seq: c.seq.clone(),
                    value: None,
                },
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
        let got = sm.get_kv(&c.key);
        assert_eq!(want, &got, "get: {}", mes,);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_add_file() -> anyhow::Result<()> {
    init_store_unittest();

    let tc = new_test_context();
    let mut sm = StateMachine::open(&tc.config).await?;

    let cases = crate::meta_service::raftmeta_test::cases_add_file();

    for (name, txid, k, v, want_prev, want_result) in cases.iter() {
        let resp = sm
            .apply(&LogId { term: 0, index: 5 }, &LogEntry {
                txid: txid.clone(),
                cmd: Cmd::AddFile {
                    key: k.to_string(),
                    value: v.to_string(),
                },
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
    init_store_unittest();

    let tc = new_test_context();
    let mut sm = StateMachine::open(&tc.config).await?;

    let cases = crate::meta_service::raftmeta_test::cases_set_file();

    for (name, txid, k, v, want_prev, want_result) in cases.iter() {
        let resp = sm
            .apply(&LogId { term: 0, index: 5 }, &LogEntry {
                txid: txid.clone(),
                cmd: Cmd::SetFile {
                    key: k.to_string(),
                    value: v.to_string(),
                },
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
async fn test_state_machine_get_databases() -> anyhow::Result<()> {
    init_store_unittest();

    let tc = new_test_context();
    let mut m = StateMachine::open(&tc.config).await?;

    struct T {
        name: &'static str,
    }

    fn case(name: &'static str) -> T {
        T { name }
    }

    let cases: Vec<T> = vec![case("foo"), case("bar"), case("wow")];

    for ref c in cases {
        // add

        m.apply_non_dup(&LogEntry {
            txid: None,
            cmd: Cmd::CreateDatabase {
                name: c.name.to_string(),
                if_not_exists: true,
                db: Default::default(),
            },
        })
        .await?;
    }
    let dbs = m.get_databases();
    let mut db_names: Vec<String> = dbs.iter().map(|v| v.0.clone()).collect();
    db_names.sort();
    assert_eq!(
        vec!["bar".to_string(), "foo".to_string(), "wow".to_string()],
        db_names,
        "get: {}",
        db_names.join(",")
    );
    Ok(())
}
