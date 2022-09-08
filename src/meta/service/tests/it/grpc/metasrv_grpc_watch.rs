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

use std::time::Duration;

use common_base::base::tokio;
use common_meta_api::get_start_and_end_of_prefix;
use common_meta_api::prefix_of_string;
use common_meta_api::KVApi;
use common_meta_grpc::MetaGrpcClient;
use common_meta_types::protobuf::watch_request::FilterType;
use common_meta_types::protobuf::Event;
use common_meta_types::protobuf::SeqV;
use common_meta_types::protobuf::TxnRequest;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::txn_condition;
use common_meta_types::txn_op;
use common_meta_types::ConditionResult;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::TxnCondition;
use common_meta_types::TxnDeleteByPrefixRequest;
use common_meta_types::TxnDeleteRequest;
use common_meta_types::TxnOp;
use common_meta_types::TxnPutRequest;
use common_meta_types::UpsertKVReq;

use crate::init_meta_ut;

async fn upsert_kv_client_main(addr: String, updates: Vec<UpsertKVReq>) -> anyhow::Result<()> {
    let client = MetaGrpcClient::try_create(
        vec![addr],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
    )?;

    // update some kv
    for update in updates.iter() {
        let _ = client.upsert_kv(update.clone()).await;
    }

    Ok(())
}

async fn txn_client_main(addr: String, txn: TxnRequest) -> anyhow::Result<()> {
    let client = MetaGrpcClient::try_create(
        vec![addr],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
    )?;

    let _ = client.transaction(txn).await;

    Ok(())
}

async fn test_watch_main(
    addr: String,
    watch: WatchRequest,
    mut watch_events: Vec<Event>,
    updates: Vec<UpsertKVReq>,
) -> anyhow::Result<()> {
    let client = MetaGrpcClient::try_create(
        vec![addr.clone()],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
    )?;

    // let mut grpc_client = client.make_conn().await?;

    let mut client_stream = client.request(watch).await?;

    let _h = tokio::spawn(upsert_kv_client_main(addr, updates));

    loop {
        if let Ok(Some(resp)) = client_stream.message().await {
            if let Some(event) = resp.event {
                assert!(!watch_events.is_empty());

                assert_eq!(watch_events.get(0), Some(&event));
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
    let client = MetaGrpcClient::try_create(
        vec![addr.clone()],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
    )?;

    let mut client_stream = client.request(watch).await?;

    let _h = tokio::spawn(txn_client_main(addr, txn));

    loop {
        if let Ok(Some(resp)) = client_stream.message().await {
            if let Some(event) = resp.event {
                assert!(!watch_events.is_empty());

                assert_eq!(watch_events.get(0), Some(&event));
                watch_events.remove(0);

                if watch_events.is_empty() {
                    break;
                }
            }
        }
    }

    Ok(())
}

#[async_entry::test(worker_threads = 3, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_watch() -> common_exception::Result<()> {
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

        let key_a = "a".to_string();
        let key_b = "b".to_string();

        let val_a = "a".as_bytes().to_vec();
        let val_b = "b".as_bytes().to_vec();
        let val_new = "new".as_bytes().to_vec();
        let val_z = "z".as_bytes().to_vec();

        let watch_events = vec![
            // set a->a
            Event {
                key: key_a.clone(),
                current: Some(SeqV {
                    seq,
                    data: val_a.clone(),
                }),
                prev: None,
            },
            // set b->b
            Event {
                key: key_b.clone(),
                current: Some(SeqV {
                    seq: seq + 2,
                    data: val_b.clone(),
                }),
                prev: None,
            },
            // update b->new
            Event {
                key: key_b.clone(),
                current: Some(SeqV {
                    seq: seq + 3,
                    data: val_new.clone(),
                }),
                prev: Some(SeqV {
                    seq: seq + 2,
                    data: val_b.clone(),
                }),
            },
            // delete b
            Event {
                key: key_b.clone(),
                prev: Some(SeqV {
                    seq: seq + 3,
                    data: val_new.clone(),
                }),
                current: None,
            },
        ];

        seq += 3;
        // update kv
        let updates = vec![
            UpsertKVReq::new("a", MatchSeq::Any, Operation::Update(val_a), None),
            // upsert key z, because z in key_end and the range is [key_start, key_end), so key z MUST not be notified in watche events.
            UpsertKVReq::new("z", MatchSeq::Any, Operation::Update(val_z), None),
            UpsertKVReq::new("b", MatchSeq::Any, Operation::Update(val_b), None),
            UpsertKVReq::new("b", MatchSeq::Any, Operation::Update(val_new), None),
            UpsertKVReq::new("b", MatchSeq::Any, Operation::Delete, None),
        ];
        test_watch_main(addr.clone(), watch, watch_events, updates).await?;
    }

    // 2. test filter
    {
        let key_str = "1";
        let watch = WatchRequest {
            key: key_str.to_string(),
            key_end: None,
            // filter only delete events
            filter_type: FilterType::Delete.into(),
        };

        let key = key_str.to_string();
        let val = "old".as_bytes().to_vec();
        let val_new = "new".as_bytes().to_vec();

        // has only delete events
        let watch_events = vec![
            // delete 1 first time
            Event {
                key: key.clone(),
                prev: Some(SeqV {
                    seq: seq + 1,
                    data: val.clone(),
                }),
                current: None,
            },
            // delete 1 second time
            Event {
                key: key.clone(),
                prev: Some(SeqV {
                    seq: seq + 2,
                    data: val_new.clone(),
                }),
                current: None,
            },
        ];

        // update and delete twice
        let updates = vec![
            UpsertKVReq::new(key_str, MatchSeq::Any, Operation::Update(val), None),
            UpsertKVReq::new(key_str, MatchSeq::Any, Operation::Delete, None),
            UpsertKVReq::new(key_str, MatchSeq::Any, Operation::Update(val_new), None),
            UpsertKVReq::new(key_str, MatchSeq::Any, Operation::Delete, None),
        ];
        test_watch_main(addr.clone(), watch, watch_events, updates).await?;
    }
    // 3. test watch transaction
    {
        // first construct test kv
        let delete_key = "watch_delete_key";
        let watch_delete_by_prefix_key = "watch_delete_by_prefix_key";

        {
            let client = MetaGrpcClient::try_create(
                vec![addr.clone()],
                "root",
                "xxx",
                None,
                Some(Duration::from_secs(10)),
                None,
            )?;

            let updates = vec![
                UpsertKVReq::new(
                    delete_key,
                    MatchSeq::Any,
                    Operation::Update(delete_key.as_bytes().to_vec()),
                    None,
                ),
                UpsertKVReq::new(
                    watch_delete_by_prefix_key,
                    MatchSeq::Any,
                    Operation::Update(watch_delete_by_prefix_key.as_bytes().to_vec()),
                    None,
                ),
            ];

            for update in updates {
                let _ = client.upsert_kv(update.clone()).await;
            }
        }
        seq += 2;

        let watch_prefix = "watch";

        let k1 = "watch_txn_key";

        let txn_key = k1.to_string();
        let txn_val = "txn_val".as_bytes().to_vec();

        let (start, end) = get_start_and_end_of_prefix(watch_prefix)?;

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
            TxnOp {
                request: Some(txn_op::Request::Put(TxnPutRequest {
                    key: txn_key.clone(),
                    value: txn_val.clone(),
                    prev_value: true,
                    expire_at: None,
                })),
            },
            TxnOp {
                request: Some(txn_op::Request::Delete(TxnDeleteRequest {
                    key: delete_key.to_string(),
                    prev_value: true,
                })),
            },
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

        seq += 1;

        let watch_events = vec![
            Event {
                key: txn_key.clone(),
                current: Some(SeqV {
                    seq: seq + 2,
                    data: txn_val,
                }),
                prev: None,
            },
            Event {
                key: delete_key.to_string(),
                prev: Some(SeqV {
                    seq,
                    data: delete_key.as_bytes().to_vec(),
                }),
                current: None,
            },
            Event {
                key: watch_delete_by_prefix_key.to_string(),
                prev: Some(SeqV {
                    seq: seq + 1,
                    data: watch_delete_by_prefix_key.as_bytes().to_vec(),
                }),
                current: None,
            },
        ];

        test_watch_txn_main(addr.clone(), watch, watch_events, txn).await?;
    }

    Ok(())
}

#[test]
fn prefix_of_string_test() -> common_exception::Result<()> {
    assert_eq!("b".to_string(), prefix_of_string("a")?);
    assert_eq!("2".to_string(), prefix_of_string("1")?);
    assert_eq!(
        "__fd_table_by_ie".to_string(),
        prefix_of_string("__fd_table_by_id")?
    );
    {
        let str = 127 as char;
        let s = str.to_string();
        let ret = prefix_of_string(&s)?;
        for byte in ret.as_bytes() {
            assert_eq!(*byte, 127_u8);
        }
    }
    {
        let s = format!("ab{}", 127 as char);
        let ret = prefix_of_string(&s)?;
        assert_eq!(ret, format!("ac{}", 127 as char));
    }
    {
        let s = "我".to_string();
        let ret = prefix_of_string(&s);
        match ret {
            Err(e) => {
                assert_eq!(
                    e.to_string(),
                    common_exception::ErrorCode::OnlySupportAsciiChars(format!(
                        "Only support ASCII characters: {}",
                        "我"
                    ))
                    .to_string()
                );
            }
            Ok(_) => panic!("MUST return error "),
        }
    }

    Ok(())
}

#[test]
fn test_get_start_and_end_of_prefix() -> common_exception::Result<()> {
    assert_eq!(
        ("aa".to_string(), "ab".to_string()),
        get_start_and_end_of_prefix("aa")?
    );
    assert_eq!(
        ("a1".to_string(), "a2".to_string()),
        get_start_and_end_of_prefix("a1")?
    );
    {
        let str = 127 as char;
        let s = str.to_string();
        let (_, end) = get_start_and_end_of_prefix(&s)?;
        for byte in end.as_bytes() {
            assert_eq!(*byte, 127_u8);
        }
    }
    {
        let ret = get_start_and_end_of_prefix("我");
        match ret {
            Err(e) => {
                assert_eq!(
                    e.to_string(),
                    common_exception::ErrorCode::OnlySupportAsciiChars(format!(
                        "Only support ASCII characters: {}",
                        "我"
                    ))
                    .to_string()
                );
            }
            Ok(_) => panic!("MUST return error "),
        }
    }
    Ok(())
}
