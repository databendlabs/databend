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

use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_raft_store::state_machine::StateMachine;
use databend_common_meta_types::new_log_id;
use databend_common_meta_types::seq_value::KVMeta;
use databend_common_meta_types::seq_value::SeqV;
use databend_common_meta_types::seq_value::SeqValue;
use databend_common_meta_types::AppliedState;
use databend_common_meta_types::Change;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::CmdContext;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::Entry;
use databend_common_meta_types::EntryPayload;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaSpec;
use databend_common_meta_types::Node;
use databend_common_meta_types::Operation;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::With;
use log::info;
use pretty_assertions::assert_eq;
use test_harness::test;

use crate::testing::new_raft_test_context;
use crate::testing::raft_store_test_harness;

mod expire;
mod schema_api_impl;

#[test(harness = raft_store_test_harness)]
#[fastrace::trace]
async fn test_state_machine_apply_add_node() -> anyhow::Result<()> {
    let tc = new_raft_test_context();
    let sm = StateMachine::open(&tc.raft_config, 1).await?;

    let apply = |index: u64, n: Node, overriding| {
        //
        let ss = &sm;
        async move {
            ss.apply(&Entry {
                log_id: new_log_id(1, 0, index),
                payload: EntryPayload::Normal(LogEntry {
                    txid: None,
                    time_ms: None,
                    cmd: Cmd::AddNode {
                        node_id: 1,
                        node: n,
                        overriding,
                    },
                }),
            })
            .await
        }
    };

    let n1 = || Node::new("a", Endpoint::new("1", 1));
    let n2 = || Node::new("b", Endpoint::new("1", 2));

    // Add node without overriding
    {
        let resp = apply(5, n1(), false).await?;
        assert_eq!(
            AppliedState::Node {
                prev: None,
                result: Some(n1())
            },
            resp
        );

        assert_eq!(Some(n1()), sm.get_node(&1)?);
    }

    // Add node without overriding, no update
    {
        let resp = apply(6, n2(), false).await?;
        assert_eq!(
            AppliedState::Node {
                prev: Some(n1()),
                result: Some(n1())
            },
            resp
        );
        assert_eq!(Some(n1()), sm.get_node(&1)?);
    }

    // Add node with overriding, updated
    {
        let resp = apply(7, n2(), true).await?;
        assert_eq!(
            AppliedState::Node {
                prev: Some(n1()),
                result: Some(n2())
            },
            resp
        );
        assert_eq!(Some(n2()), sm.get_node(&1)?);
    }

    Ok(())
}

#[test(harness = raft_store_test_harness)]
#[fastrace::trace]
async fn test_state_machine_apply_non_dup_generic_kv_upsert_get() -> anyhow::Result<()> {
    let tc = new_raft_test_context();
    let sm = StateMachine::open(&tc.raft_config, 1).await?;

    #[derive(Debug)]
    struct T {
        // input:
        key: String,
        seq: MatchSeq,
        value: Vec<u8>,
        value_meta: Option<MetaSpec>,
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
        // The time when it is applied to sm.
        now_ms: u64,
    ) -> T {
        let m = meta.map(|x| MetaSpec::new_ttl(Duration::from_secs(x)));
        T {
            key: name.to_string(),
            seq,
            value: value.to_string().into_bytes(),
            value_meta: m.clone(),
            prev: prev.map(|(a, b)| SeqV::new(a, b.into())),
            result: result.map(|(a, b)| {
                SeqV::with_meta(
                    a,
                    m.map(|m| m.to_kv_meta(&CmdContext::from_millis(now_ms))),
                    b.into(),
                )
            }),
        }
    }

    let now_ms = SeqV::<()>::now_ms();

    let cases: Vec<T> = vec![
        case("foo", MatchSeq::Exact(5), "b", None, None, None, now_ms),
        case(
            "foo",
            MatchSeq::GE(0),
            "a",
            None,
            None,
            Some((1, "a")),
            now_ms,
        ),
        case(
            "foo",
            MatchSeq::GE(0),
            "b",
            None,
            Some((1, "a")),
            Some((2, "b")),
            now_ms,
        ),
        case(
            "foo",
            MatchSeq::Exact(5),
            "b",
            None,
            Some((2, "b")),
            Some((2, "b")),
            now_ms,
        ),
        case(
            "bar",
            MatchSeq::Exact(0),
            "x",
            None,
            None,
            Some((3, "x")),
            now_ms,
        ),
        case(
            "bar",
            MatchSeq::Exact(0),
            "y",
            None,
            Some((3, "x")),
            Some((3, "x")),
            now_ms,
        ),
        case(
            "bar",
            MatchSeq::GE(1),
            "y",
            None,
            Some((3, "x")),
            Some((4, "y")),
            now_ms,
        ),
        // expired at once
        case(
            "wow",
            MatchSeq::GE(0),
            "y",
            Some(0),
            None,
            Some((5, "y")),
            now_ms,
        ),
        // expired value does not exist
        case(
            "wow",
            MatchSeq::GE(0),
            "y",
            Some(5_000),
            None,
            Some((6, "y")),
            now_ms,
        ),
    ];

    for (i, c) in cases.iter().enumerate() {
        let mes = format!("{}-th: {}({:?})={:?}", i, c.key, c.seq, c.value);

        // write
        let resp = sm.sm_tree.txn(true, |mut t| {
            Ok(sm
                .apply_cmd(
                    &Cmd::UpsertKV(UpsertKV {
                        key: c.key.clone(),
                        seq: c.seq,
                        value: Operation::Update(c.value.clone()),
                        value_meta: c.value_meta.clone(),
                    }),
                    &mut t,
                    None,
                    now_ms,
                )
                .unwrap())
        })?;
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
                if w.eval_expire_at_ms() < now_ms {
                    None
                } else {
                    want
                }
            }
        };

        let got = sm.get_kv(&c.key).await?;
        assert_eq!(want, got, "get: {}", mes,);
    }

    Ok(())
}

#[test(harness = raft_store_test_harness)]
#[fastrace::trace]
async fn test_state_machine_apply_non_dup_generic_kv_value_meta() -> anyhow::Result<()> {
    // - Update a value-meta of None does nothing.
    // - Update a value-meta of Some() only updates the value-meta.

    let tc = new_raft_test_context();
    let sm = StateMachine::open(&tc.raft_config, 1).await?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let now_ms = SeqV::<()>::now_ms();

    let key = "value_meta_foo".to_string();

    info!("--- update meta of a nonexistent record");

    let resp = sm.sm_tree.txn(true, |mut t| {
        Ok(sm
            .apply_cmd(
                &Cmd::UpsertKV(UpsertKV {
                    key: key.clone(),
                    seq: MatchSeq::GE(0),
                    value: Operation::AsIs,
                    value_meta: Some(MetaSpec::new_ttl(Duration::from_secs(10))),
                }),
                &mut t,
                None,
                now_ms,
            )
            .unwrap())
    })?;

    assert_eq!(
        AppliedState::KV(Change::new(None, None)),
        resp,
        "update meta of None does nothing",
    );

    info!("--- update meta of a existent record");

    // add a record
    sm.sm_tree.txn(true, |mut t| {
        Ok(sm
            .apply_cmd(
                &Cmd::UpsertKV(UpsertKV {
                    key: key.clone(),
                    seq: MatchSeq::GE(0),
                    value: Operation::Update(b"value_meta_bar".to_vec()),
                    value_meta: Some(MetaSpec::new_ttl(Duration::from_secs(10))),
                }),
                &mut t,
                None,
                now_ms,
            )
            .unwrap())
    })?;

    let got = sm.get_kv(&key).await?;
    println!("after first upsert: got: {:?}", got);

    // update the meta of the record
    sm.sm_tree.txn(true, |mut t| {
        Ok(sm
            .apply_cmd(
                &Cmd::UpsertKV(UpsertKV {
                    key: key.clone(),
                    seq: MatchSeq::GE(0),
                    value: Operation::AsIs,
                    value_meta: Some(MetaSpec::new_ttl(Duration::from_secs(20))),
                }),
                &mut t,
                None,
                now_ms,
            )
            .unwrap())
    })?;

    info!("--- read the original value and updated meta");

    let got = sm.get_kv(&key).await?;
    let got = got.unwrap();

    assert_eq!(
        SeqV {
            seq: got.seq,
            meta: Some(KVMeta::new_expire(now + 20)),
            data: b"value_meta_bar".to_vec()
        },
        got,
        "update meta of None does nothing",
    );

    Ok(())
}

#[test(harness = raft_store_test_harness)]
#[fastrace::trace]
async fn test_state_machine_apply_non_dup_generic_kv_delete() -> anyhow::Result<()> {
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
        case("foo", MatchSeq::GE(0), prev, None),
        case("foo", MatchSeq::Exact(1), prev, None),
        case("foo", MatchSeq::Exact(0), prev, prev),
        case("foo", MatchSeq::GE(1), prev, None),
        case("foo", MatchSeq::GE(2), prev, prev),
    ];

    for (i, c) in cases.iter().enumerate() {
        let mes = format!("{}-th: {}({})", i, c.key, c.seq);

        let tc = new_raft_test_context();
        let sm = StateMachine::open(&tc.raft_config, 1).await?;

        // prepare an record
        sm.sm_tree.txn(true, |mut t| {
            Ok(sm
                .apply_cmd(
                    &Cmd::UpsertKV(UpsertKV::update("foo", b"x")),
                    &mut t,
                    None,
                    0,
                )
                .unwrap())
        })?;

        // delete
        let resp = sm.sm_tree.txn(true, |mut t| {
            Ok(sm
                .apply_cmd(
                    &Cmd::UpsertKV(UpsertKV::delete(&c.key).with(c.seq)),
                    &mut t,
                    None,
                    0,
                )
                .unwrap())
        })?;
        assert_eq!(
            AppliedState::KV(Change::new(c.prev.clone(), c.result.clone())),
            resp,
            "delete: {}",
            mes,
        );

        // read it to ensure the modified state.
        let want = &c.result;
        let got = sm.get_kv(&c.key).await?;
        assert_eq!(want, &got, "get: {}", mes,);
    }

    Ok(())
}
