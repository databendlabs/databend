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

use std::collections::HashMap;
use std::convert::TryInto;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_raft::raft::Entry;
use async_raft::raft::EntryNormal;
use async_raft::raft::EntryPayload;
use async_raft::raft::MembershipConfig;
use async_raft::LogId;
use common_base::tokio;
use common_exception::ErrorCode;
use common_meta_types::Change;
use common_meta_types::Cmd;
use common_meta_types::KVMeta;
use common_meta_types::LogEntry;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::TableMeta;
use common_tracing::tracing;
use maplit::btreeset;
use maplit::hashmap;
use pretty_assertions::assert_eq;

use crate::init_raft_store_ut;
use crate::state_machine::testing::pretty_snapshot;
use crate::state_machine::testing::pretty_snapshot_iter;
use crate::state_machine::testing::snapshot_logs;
use crate::state_machine::AppliedState;
use crate::state_machine::SerializableSnapshot;
use crate::state_machine::StateMachine;
use crate::testing::new_raft_test_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_non_dup_incr_seq() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_raft_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_raft_test_context();
    let mut sm = StateMachine::open(&tc.raft_config, 1).await?;

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
    let (_log_guards, ut_span) = init_raft_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_raft_test_context();
    let mut sm = StateMachine::open(&tc.raft_config, 1).await?;

    let cases = crate::state_machine::testing::cases_incr_seq();

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
    let (_log_guards, ut_span) = init_raft_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_raft_test_context();
    let mut m = StateMachine::open(&tc.raft_config, 1).await?;

    struct T {
        name: &'static str,
        prev: Option<u64>,
        result: Option<u64>,
    }

    fn case(name: &'static str, prev: Option<u64>, result: Option<u64>) -> T {
        T { name, prev, result }
    }

    let cases: Vec<T> = vec![
        case("foo", None, Some(1)),
        case("foo", Some(1), Some(1)),
        case("bar", None, Some(3)),
        case("bar", Some(3), Some(3)),
        case("wow", None, Some(5)),
    ];

    for (i, c) in cases.iter().enumerate() {
        // add

        let resp = m
            .apply_cmd(&Cmd::CreateDatabase {
                name: c.name.to_string(),
            })
            .await?;

        let (prev, result) = match resp {
            AppliedState::DatabaseId(ch) => ch.map(|x| x.data),
            _ => {
                panic!("expect AppliedState::Database")
            }
        };
        assert_eq!(c.prev, prev, "{}-th", i);
        assert_eq!(c.result, result, "{}-th", i);

        // get

        let want = match (&prev, &result) {
            (Some(ref a), _) => a,
            (_, Some(ref b)) => b,
            _ => {
                panic!("both none");
            }
        };

        let got = m
            .get_database(c.name)?
            .ok_or_else(|| anyhow::anyhow!("db not found: {}", c.name))?;
        assert_eq!(*want, got.data);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_upsert_table_option() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_raft_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_raft_test_context();
    let mut m = StateMachine::open(&tc.raft_config, 1).await?;

    tracing::info!("--- prepare a table");
    m.apply_cmd(&Cmd::CreateDatabase {
        name: "db1".to_string(),
    })
    .await?;

    let resp = m
        .apply_cmd(&Cmd::CreateTable {
            db_name: "db1".to_string(),
            table_name: "tb1".to_string(),
            table_meta: Default::default(),
        })
        .await?;

    let (table_id, mut version) = match resp {
        AppliedState::TableIdent { result, .. } => {
            let r = result.unwrap();
            (r.table_id, r.version)
        }
        _ => {
            panic!("expect AppliedState::TableIdent")
        }
    };

    tracing::info!("--- upsert options on empty table options");
    {
        let resp = m
            .apply_cmd(&Cmd::UpsertTableOptions {
                table_id,
                seq: MatchSeq::Exact(version),
                table_options: hashmap! {
                    "a".to_string() => Some("A".to_string()),
                    "b".to_string() => None,
                },
            })
            .await?;

        let ch: Change<TableMeta> = resp.try_into().unwrap();
        let (prev, result) = ch.unwrap();

        tracing::info!("--- check prev state is returned");
        {
            assert_eq!(version, prev.seq);
            assert_eq!(HashMap::new(), prev.data.options);
        }

        tracing::info!("--- check result state, deleting b has no effect");
        {
            assert!(result.seq > version);
            assert_eq!(
                hashmap! {
                    "a".to_string() => "A".to_string()
                },
                result.data.options
            );
        }
    }

    tracing::info!("--- check table is updated");
    {
        let got = m.get_table_by_id(&table_id)?.unwrap();
        assert!(got.seq > version);
        assert_eq!(
            hashmap! {
                "a".to_string() => "A".to_string()
            },
            got.data.options
        );

        // update version to the latest
        version = got.seq;
    }

    tracing::info!("--- update with invalid table_id");
    {
        let resp = m
            .apply_cmd(&Cmd::UpsertTableOptions {
                table_id: 0,
                seq: MatchSeq::Exact(version - 1),
                table_options: hashmap! {},
            })
            .await;

        let err = resp.unwrap_err();

        assert_eq!(ErrorCode::UnknownTableIdCode(), err.code());
    }

    tracing::info!("--- update with mismatched seq wont update anything");
    {
        let resp = m
            .apply_cmd(&Cmd::UpsertTableOptions {
                table_id,
                seq: MatchSeq::Exact(version - 1),
                table_options: hashmap! {},
            })
            .await?;

        let ch: Change<TableMeta> = resp.try_into().unwrap();
        let (prev, result) = ch.unwrap();

        assert_eq!(prev, result);
    }

    tracing::info!("--- update OK");
    {
        let resp = m
            .apply_cmd(&Cmd::UpsertTableOptions {
                table_id,
                seq: MatchSeq::Exact(version),
                table_options: hashmap! {
                    "a".to_string() => None,
                    "c".to_string() => Some("C".to_string()),
                },
            })
            .await?;

        let ch: Change<TableMeta> = resp.try_into().unwrap();
        let (prev, result) = ch.unwrap();

        tracing::info!("--- check prev state is returned");
        assert_eq!(version, prev.seq);
        assert_eq!(
            hashmap! {
                "a".to_string() => "A".to_string()
            },
            prev.data.options
        );

        tracing::info!("--- check result state, delete a add c");
        assert!(result.seq > version);
        assert_eq!(
            hashmap! {
                "c".to_string() => "C".to_string()
            },
            result.data.options
        );

        tracing::info!("--- check table is updated");
        {
            let got = m.get_table_by_id(&table_id)?.unwrap();
            assert!(got.seq > version);
            assert_eq!(
                hashmap! {
                    "c".to_string() => "C".to_string()
                },
                got.data.options
            );
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_non_dup_generic_kv_upsert_get() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_raft_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_raft_test_context();
    let mut sm = StateMachine::open(&tc.raft_config, 1).await?;

    struct T {
        // input:
        key: String,
        seq: MatchSeq,
        value: Vec<u8>,
        value_meta: Option<KVMeta>,
        // want:
        prev: Option<SeqV<Vec<u8>>>,
        result: Option<SeqV<Vec<u8>>>,
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
            prev: prev.map(|(a, b)| SeqV::new(a, b.into())),
            result: result.map(|(a, b)| SeqV::with_meta(a, m, b.into())),
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
            AppliedState::KV(Change::new(c.prev.clone(), c.result.clone())),
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
                if w.get_expire_at() < now {
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

    let (_log_guards, ut_span) = init_raft_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_raft_test_context();
    let mut sm = StateMachine::open(&tc.raft_config, 1).await?;

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
        AppliedState::KV(Change::new(None, None)),
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
        SeqV {
            seq: got.seq,
            meta: Some(KVMeta {
                expire_at: Some(now + 20)
            }),
            data: b"value_meta_bar".to_vec()
        },
        got,
        "update meta of None does nothing",
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_state_machine_apply_non_dup_generic_kv_delete() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_raft_store_ut!();
    let _ent = ut_span.enter();

    struct T {
        // input:
        key: String,
        seq: MatchSeq,
        // want:
        prev: Option<SeqV<Vec<u8>>>,
        result: Option<SeqV<Vec<u8>>>,
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
            prev: prev.map(|(a, b)| SeqV::new(a, b.into())),
            result: result.map(|(a, b)| SeqV::new(a, b.into())),
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

        let tc = new_raft_test_context();
        let mut sm = StateMachine::open(&tc.raft_config, 1).await?;

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
            AppliedState::KV(Change::new(c.prev.clone(), c.result.clone())),
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
async fn test_state_machine_snapshot() -> anyhow::Result<()> {
    // - Feed logs into state machine.
    // - Take a snapshot and examine the data

    let (_log_guards, ut_span) = init_raft_store_ut!();
    let _ent = ut_span.enter();

    let tc = new_raft_test_context();
    let mut sm = StateMachine::open(&tc.raft_config, 0).await?;

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
