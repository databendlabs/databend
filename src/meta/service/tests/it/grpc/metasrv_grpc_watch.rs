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

use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_base::base::tokio;
use databend_common_base::base::tokio::time::sleep;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::protobuf::watch_request::FilterType;
use databend_common_meta_types::protobuf::Event;
use databend_common_meta_types::protobuf::KvMeta;
use databend_common_meta_types::protobuf::SeqV;
use databend_common_meta_types::protobuf::TxnRequest;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::txn_condition;
use databend_common_meta_types::txn_op;
use databend_common_meta_types::ConditionResult;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::Operation;
use databend_common_meta_types::TxnCondition;
use databend_common_meta_types::TxnDeleteByPrefixRequest;
use databend_common_meta_types::TxnOp;
use databend_meta::meta_service::MetaNode;
use log::info;
use test_harness::test;

use crate::testing::meta_service_test_harness;

async fn test_watch_main(
    addr: String,
    watch: WatchRequest,
    mut watch_events: Vec<Event>,
    updates: Vec<UpsertKVReq>,
) -> anyhow::Result<()> {
    let client = make_client(&addr)?;
    let mut watch_stream = client.request(watch).await?;

    {
        let client = make_client(&addr)?;
        let _h = databend_common_base::runtime::spawn(async move {
            for update in updates.iter() {
                client.upsert_kv(update.clone()).await.unwrap();
            }
        });
    }

    loop {
        if let Ok(Some(resp)) = watch_stream.message().await {
            if let Some(event) = resp.event {
                assert!(!watch_events.is_empty());

                assert_eq!(watch_events.first(), Some(&event));
                watch_events.remove(0);

                if watch_events.is_empty() {
                    break;
                }
            }
        }
    }

    Ok(())
}

async fn test_watch_txn_main(
    addr: String,
    watch: WatchRequest,
    mut watch_events: Vec<Event>,
    txn: TxnRequest,
) -> anyhow::Result<()> {
    let client = make_client(&addr)?;
    let mut watch_stream = client.request(watch).await?;

    {
        let client = make_client(&addr)?;
        let _h = databend_common_base::runtime::spawn(async move {
            client.transaction(txn).await.unwrap();
        });
    }

    loop {
        if let Ok(Some(resp)) = watch_stream.message().await {
            if let Some(event) = resp.event {
                assert!(!watch_events.is_empty());

                assert_eq!(watch_events.first(), Some(&event));
                watch_events.remove(0);

                if watch_events.is_empty() {
                    break;
                }
            }
        }
    }

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_watch() -> anyhow::Result<()> {
    // - Start a metasrv server.
    // - Watch some key.
    // - Write some data.
    // - Assert watcher get all the update.

    let (_tc, addr) = crate::tests::start_metasrv().await?;

    let mut seq: u64 = 1;
    // 1.update some events
    {
        let watch = WatchRequest {
            key: "a".to_string(),
            key_end: Some("z".to_string()),
            filter_type: FilterType::All.into(),
        };

        let key_a = s("a");
        let key_b = s("b");

        let val_a = b("a");
        let val_b = b("b");
        let val_new = b("new");
        let val_z = b("z");

        let watch_events = vec![
            // set a->a
            Event {
                key: key_a.clone(),
                current: Some(SeqV::new(seq, val_a.clone())),
                prev: None,
            },
            // set b->b
            Event {
                key: key_b.clone(),
                current: Some(SeqV::new(seq + 2, val_b.clone())),
                prev: None,
            },
            // update b->new
            Event {
                key: key_b.clone(),
                current: Some(SeqV::new(seq + 3, val_new.clone())),
                prev: Some(SeqV::new(seq + 2, val_b.clone())),
            },
            // delete b
            Event {
                key: key_b.clone(),
                prev: Some(SeqV::new(seq + 3, val_new.clone())),
                current: None,
            },
        ];

        seq = 4;
        // update kv
        let updates = vec![
            UpsertKVReq::new("a", MatchSeq::GE(0), Operation::Update(val_a), None),
            // upsert key z, because z in key_end and the range is [key_start, key_end), so key z MUST not be notified in watch events.
            UpsertKVReq::new("z", MatchSeq::GE(0), Operation::Update(val_z), None),
            UpsertKVReq::new("b", MatchSeq::GE(0), Operation::Update(val_b), None),
            UpsertKVReq::new("b", MatchSeq::GE(0), Operation::Update(val_new), None),
            UpsertKVReq::new("b", MatchSeq::GE(0), Operation::Delete, None),
        ];
        test_watch_main(addr.clone(), watch, watch_events, updates).await?;
    }

    // 2. test filter
    {
        let key_str = "1";
        let watch = WatchRequest {
            key: s(key_str),
            key_end: None,
            // filter only delete events
            filter_type: FilterType::Delete.into(),
        };

        let key = s(key_str);
        let val = b("old");
        let val_new = b("new");

        // has only delete events
        let watch_events = vec![
            // delete 1 first time
            Event {
                key: key.clone(),
                prev: Some(SeqV::new(seq + 1, val.clone())),
                current: None,
            },
            // delete 1 second time
            Event {
                key: key.clone(),
                prev: Some(SeqV::new(seq + 2, val_new.clone())),
                current: None,
            },
        ];

        // update and delete twice
        let updates = vec![
            UpsertKVReq::new(key_str, MatchSeq::GE(0), Operation::Update(val), None),
            UpsertKVReq::new(key_str, MatchSeq::GE(0), Operation::Delete, None),
            UpsertKVReq::new(key_str, MatchSeq::GE(0), Operation::Update(val_new), None),
            UpsertKVReq::new(key_str, MatchSeq::GE(0), Operation::Delete, None),
        ];
        test_watch_main(addr.clone(), watch, watch_events, updates).await?;
    }
    // 3. test watch transaction
    {
        // first construct test kv
        let delete_key = "watch_delete_key";
        let watch_delete_by_prefix_key = "watch_delete_by_prefix_key";

        {
            let client = make_client(&addr)?;

            let updates = vec![
                UpsertKVReq::update(delete_key, &b(delete_key)),
                UpsertKVReq::update(watch_delete_by_prefix_key, &b(watch_delete_by_prefix_key)),
            ];

            for update in updates {
                let _ = client.upsert_kv(update.clone()).await;
            }
        }

        let watch_prefix = "watch";

        let k1 = "watch_txn_key";

        let txn_key = s(k1);
        let txn_val = b("txn_val");

        let (start, end) = kvapi::prefix_to_range(watch_prefix)?;

        let watch = WatchRequest {
            key: start,
            key_end: Some(end),
            filter_type: FilterType::All.into(),
        };

        let conditions = vec![TxnCondition {
            key: txn_key.clone(),
            expected: ConditionResult::Eq as i32,
            target: Some(txn_condition::Target::Seq(0)),
        }];

        let if_then: Vec<TxnOp> = vec![
            TxnOp::put(txn_key.clone(), txn_val.clone()),
            TxnOp::delete(delete_key),
            TxnOp {
                request: Some(txn_op::Request::DeleteByPrefix(TxnDeleteByPrefixRequest {
                    prefix: watch_delete_by_prefix_key.to_string(),
                })),
            },
        ];

        let else_then: Vec<TxnOp> = vec![];

        let txn = TxnRequest {
            condition: conditions,
            if_then,
            else_then,
        };

        seq = 7;

        let watch_events = vec![
            Event {
                key: txn_key.clone(),
                current: Some(SeqV::with_meta(seq + 2, Some(KvMeta::default()), txn_val)),
                prev: None,
            },
            Event {
                key: s(delete_key),
                prev: Some(SeqV::new(seq, b(delete_key))),
                current: None,
            },
            Event {
                key: s(watch_delete_by_prefix_key),
                prev: Some(SeqV::new(seq + 1, b(watch_delete_by_prefix_key))),
                current: None,
            },
        ];

        test_watch_txn_main(addr.clone(), watch, watch_events, txn).await?;
    }

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_watch_expired_events() -> anyhow::Result<()> {
    // Test events emitted when cleaning expired key:
    // - Before applying, 32 expired keys will be cleaned.
    // - When applying, touched expired keys will be cleaned.

    let (_tc, addr) = crate::tests::start_metasrv().await?;

    let watch_prefix = "w_";
    let now_sec = now();
    let expire = now_sec + 11;
    // dbg!(now_sec, expire);

    info!("--- prepare data that are gonna expire");
    {
        let client = make_client(&addr)?;

        let mut txn = TxnRequest {
            condition: vec![],
            if_then: vec![],
            else_then: vec![],
        };

        // Every apply() will clean all expired keys.
        for i in 0..(32 + 1) {
            let k = format!("w_auto_gc_{}", i);
            txn.if_then
                .push(TxnOp::put_with_expire(&k, b(&k), Some(expire - 10)));
        }

        // Expired key wont be cleaned when they are read, although read returns None.

        txn.if_then
            .push(TxnOp::put_with_expire("w_b1", b("w_b1"), Some(expire - 5)));
        txn.if_then
            .push(TxnOp::put_with_expire("w_b2", b("w_b2"), Some(expire - 5)));
        txn.if_then.push(TxnOp::put_with_expire(
            "w_b3a",
            b("w_b3a"),
            Some(expire - 5),
        ));
        txn.if_then.push(TxnOp::put_with_expire(
            "w_b3b",
            b("w_b3b"),
            Some(expire + 5),
        ));

        client.transaction(txn).await?;
    }

    info!("--- start watching");
    let watch_client = make_client(&addr)?;
    let mut client_stream = {
        let (start, end) = kvapi::prefix_to_range(watch_prefix)?;
        let watch = WatchRequest {
            key: start,
            key_end: Some(end),
            filter_type: FilterType::All.into(),
        };
        watch_client.request(watch).await?
    };

    info!("--- sleep {} for expiration", expire - now_sec);
    tokio::time::sleep(Duration::from_secs(expire - now_sec)).await;

    info!("--- apply another txn in another thread to override keys");
    {
        let txn = TxnRequest {
            condition: vec![],
            if_then: vec![
                TxnOp::put("w_b1", b("w_b1_override")),
                TxnOp::delete("w_b2"),
                TxnOp {
                    request: Some(txn_op::Request::DeleteByPrefix(TxnDeleteByPrefixRequest {
                        prefix: s("w_b3"),
                    })),
                },
            ],
            else_then: vec![],
        };

        let client = make_client(&addr)?;
        let _h = databend_common_base::runtime::spawn(async move {
            let _res = client.transaction(txn).await;
        });
    }

    info!("--- check emitted events");
    {
        // 32 expired keys are auto cleaned.
        for i in 0..(32 + 1) {
            let k = format!("w_auto_gc_{}", i);
            let want = del_event(&k, 1 + i, &k, Some(KvMeta::new_expire(expire - 10)));
            let msg = client_stream.message().await?.unwrap();
            assert_eq!(Some(want), msg.event);
        }

        // Check event generated when applying the txn

        let seq = 34;
        let watch_events = vec![
            del_event("w_b1", seq, "w_b1", Some(KvMeta::new_expire(expire - 5))), // expired
            del_event(
                "w_b2",
                seq + 1,
                "w_b2",
                Some(KvMeta::new_expire(expire - 5)),
            ), // expired
            del_event(
                "w_b3a",
                seq + 2,
                "w_b3a",
                Some(KvMeta::new_expire(expire - 5)),
            ), // expired
            add_event("w_b1", seq + 4, "w_b1_override", Some(KvMeta::default())), // override
            del_event(
                "w_b3b",
                seq + 3,
                "w_b3b",
                Some(KvMeta::new_expire(expire + 5)),
            ), // expired
        ];

        for ev in watch_events {
            let msg = client_stream.message().await?.unwrap();
            assert_eq!(Some(ev), msg.event);
        }
    }

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_watch_stream_count() -> anyhow::Result<()> {
    // When the client drops the stream, databend-meta should reclaim the resources.

    let (tc, addr) = crate::tests::start_metasrv().await?;

    let watch_req = || WatchRequest {
        key: "a".to_string(),
        key_end: Some("z".to_string()),
        filter_type: FilterType::All.into(),
    };

    let client1 = make_client(&addr)?;
    let _watch_stream1 = client1.request(watch_req()).await?;

    let mn: Arc<MetaNode> = tc.grpc_srv.as_ref().map(|x| x.get_meta_node()).unwrap();

    let watcher_count = Arc::new(std::sync::Mutex::new(0usize));

    info!("one watcher");
    {
        let cnt = watcher_count.clone();

        mn.dispatcher_handle
            .request_blocking(move |d| {
                *cnt.lock().unwrap() = d.watchers().count();
            })
            .await;

        assert_eq!(1, *watcher_count.lock().unwrap());
    }

    info!("second watcher");
    {
        let client2 = make_client(&addr)?;
        let _watch_stream2 = client2.request(watch_req()).await?;

        let cnt = watcher_count.clone();

        mn.dispatcher_handle
            .request_blocking(move |d| {
                *cnt.lock().unwrap() = d.watchers().count();
            })
            .await;

        assert_eq!(2, *watcher_count.lock().unwrap());
    }

    info!("wait a while for MetaNode to process stream cleanup");
    sleep(Duration::from_millis(2_000)).await;

    info!("second watcher is removed");
    {
        let cnt = watcher_count.clone();

        mn.dispatcher_handle
            .request_blocking(move |d| {
                *cnt.lock().unwrap() = d.watchers().count();
            })
            .await;

        assert_eq!(1, *watcher_count.lock().unwrap());
    }

    Ok(())
}

fn s(x: &str) -> String {
    x.to_string()
}

fn b(x: &str) -> Vec<u8> {
    x.as_bytes().to_vec()
}

/// Build a protobuf defined `SeqV`.
fn pb_seqv(seq: u64, data: &str, meta: Option<KvMeta>) -> Option<SeqV> {
    Some(SeqV::with_meta(seq, meta, b(data)))
}

/// Build an event represent a delete operation: i.e., prev is Some, result is None
fn del_event(key: &str, prev_seq: u64, prev_val: &str, prev_meta: Option<KvMeta>) -> Event {
    Event {
        key: s(key),
        prev: pb_seqv(prev_seq, prev_val, prev_meta),
        current: None,
    }
}

/// Build an event represent an add operation: i.e., prev is None, result is Some
fn add_event(key: &str, res_seq: u64, res_val: &str, meta: Option<KvMeta>) -> Event {
    Event {
        key: s(key),
        prev: None,
        current: pb_seqv(res_seq, res_val, meta),
    }
}

fn make_client(addr: impl ToString) -> anyhow::Result<Arc<ClientHandle>> {
    let client = MetaGrpcClient::try_create(
        vec![addr.to_string()],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
    )?;

    Ok(client)
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
