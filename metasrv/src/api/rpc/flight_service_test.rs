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

use common_metatypes::KVMeta;
use common_metatypes::KVValue;
use common_metatypes::MatchSeq;
use common_runtime::tokio;
use common_store_api_sdk::kv_api_impl::UpsertKVActionResult;
use common_store_api_sdk::KVApi;
use common_store_api_sdk::StoreClient;
use common_tracing::tracing;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_restart() -> anyhow::Result<()> {
    // Issue 1134  https://github.com/datafuselabs/databend/issues/1134
    // - Start a metasrv server.
    // - create db and create table
    // - restart
    // - Test read the db and read the table.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (mut tc, addr) = crate::tests::start_metasrv().await?;

    let client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

    tracing::info!("--- upsert kv");
    {
        let res = client
            .upsert_kv("foo", MatchSeq::Any, Some(b"bar".to_vec()), None)
            .await;

        tracing::debug!("set kv res: {:?}", res);
        let res = res?;
        assert_eq!(
            UpsertKVActionResult {
                prev: None,
                result: Some((1, KVValue {
                    meta: None,
                    value: b"bar".to_vec()
                }))
            },
            res,
            "upsert kv"
        );
    }

    tracing::info!("--- get kv");
    {
        let res = client.get_kv("foo").await;
        tracing::debug!("get kv res: {:?}", res);
        let res = res?;
        assert_eq!(
            Some((1, KVValue {
                meta: None,
                value: b"bar".to_vec()
            })),
            res.result,
            "get kv"
        );
    }

    tracing::info!("--- stop Metasrv");
    {
        let (stop_tx, fin_rx) = tc.channels.take().unwrap();
        stop_tx
            .send(())
            .map_err(|_| anyhow::anyhow!("fail to send"))?;

        fin_rx.await?;

        drop(client);

        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // restart by opening existent meta db
        tc.config.meta_config.boot = false;
        crate::tests::start_metasrv_with_context(&mut tc).await?;
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(10_000)).await;

    // try to reconnect the restarted server.
    let client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

    tracing::info!("--- get kv");
    {
        let res = client.get_kv("foo").await;
        tracing::debug!("get kv res: {:?}", res);
        let res = res?;
        assert_eq!(
            Some((1, KVValue {
                meta: None,
                value: b"bar".to_vec()
            })),
            res.result,
            "get kv"
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_generic_kv_mget() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();
    {
        let span = tracing::span!(tracing::Level::INFO, "test_flight_generic_kv_list");
        let _ent = span.enter();

        let (_tc, addr) = crate::tests::start_metasrv().await?;

        let client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

        client
            .upsert_kv("k1", MatchSeq::Any, Some(b"v1".to_vec()), None)
            .await?;
        client
            .upsert_kv("k2", MatchSeq::Any, Some(b"v2".to_vec()), None)
            .await?;

        let res = client
            .mget_kv(&["k1".to_string(), "k2".to_string()])
            .await?;
        assert_eq!(res.result, vec![
            Some((1, KVValue {
                meta: None,
                value: b"v1".to_vec()
            })),
            // NOTE, the sequence number is increased globally (inside the namespace of generic kv)
            Some((2, KVValue {
                meta: None,
                value: b"v2".to_vec()
            })),
        ]);

        let res = client
            .mget_kv(&["k1".to_string(), "key_no exist".to_string()])
            .await?;
        assert_eq!(res.result, vec![
            Some((1, KVValue {
                meta: None,
                value: b"v1".to_vec()
            })),
            None
        ]);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_generic_kv_list() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();
    {
        let span = tracing::span!(tracing::Level::INFO, "test_flight_generic_kv_list");
        let _ent = span.enter();

        let (_tc, addr) = crate::tests::start_metasrv().await?;

        let client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

        let mut values = vec![];
        {
            client
                .upsert_kv("t", MatchSeq::Any, Some("".as_bytes().to_vec()), None)
                .await?;

            for i in 0..9 {
                let key = format!("__users/{}", i);
                let val = format!("val_{}", i);
                values.push(val.clone());
                client
                    .upsert_kv(&key, MatchSeq::Any, Some(val.as_bytes().to_vec()), None)
                    .await?;
            }
            client
                .upsert_kv("v", MatchSeq::Any, Some(b"".to_vec()), None)
                .await?;
        }

        let res = client.prefix_list_kv("__users/").await?;
        assert_eq!(
            res.iter()
                .map(|(_key, (_s, val))| val.clone())
                .collect::<Vec<_>>(),
            values
                .iter()
                .map(|v| KVValue {
                    meta: None,
                    value: v.as_bytes().to_vec()
                })
                .collect::<Vec<_>>()
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_generic_kv_delete() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();
    {
        let span = tracing::span!(tracing::Level::INFO, "test_flight_generic_kv_list");
        let _ent = span.enter();

        let (_tc, addr) = crate::tests::start_metasrv().await?;

        let client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

        let test_key = "test_key";
        client
            .upsert_kv(test_key, MatchSeq::Any, Some(b"v1".to_vec()), None)
            .await?;

        let current = client.get_kv(test_key).await?;
        if let Some((seq, _val)) = current.result {
            // seq mismatch
            let wrong_seq = Some(seq + 1);
            let res = client
                .upsert_kv(test_key, wrong_seq.into(), None, None)
                .await?;
            assert_eq!(res.prev, res.result);

            // seq match
            let res = client
                .upsert_kv(test_key, MatchSeq::Exact(seq), None, None)
                .await?;
            assert!(res.result.is_none());

            // read nothing
            let r = client.get_kv(test_key).await?;
            assert!(r.result.is_none());
        } else {
            panic!("expecting a value, but got nothing");
        }

        // key not exist
        let res = client
            .upsert_kv("not exists", MatchSeq::Any, None, None)
            .await?;
        assert_eq!(None, res.prev);
        assert_eq!(None, res.result);

        // do not care seq
        client
            .upsert_kv(test_key, MatchSeq::Any, Some(b"v2".to_vec()), None)
            .await?;

        let res = client
            .upsert_kv(test_key, MatchSeq::Any, None, None)
            .await?;
        assert_eq!(
            (
                Some((2, KVValue {
                    meta: None,
                    value: b"v2".to_vec()
                })),
                None
            ),
            (res.prev, res.result)
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_generic_kv_update() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();
    {
        let span = tracing::span!(tracing::Level::INFO, "test_flight_generic_kv_list");
        let _ent = span.enter();

        let (_tc, addr) = crate::tests::start_metasrv().await?;

        let client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

        let test_key = "test_key_for_update";

        let r = client
            .upsert_kv(test_key, MatchSeq::GE(1), Some(b"v1".to_vec()), None)
            .await?;
        assert_eq!((None, None), (r.prev, r.result), "not changed");

        let r = client
            .upsert_kv(test_key, MatchSeq::Any, Some(b"v1".to_vec()), None)
            .await?;
        assert_eq!(
            Some((1, KVValue {
                meta: None,
                value: b"v1".to_vec()
            })),
            r.result
        );
        let seq = r.result.unwrap().0;

        // unmatched seq
        let r = client
            .upsert_kv(
                test_key,
                MatchSeq::Exact(seq + 1),
                Some(b"v2".to_vec()),
                None,
            )
            .await?;
        assert_eq!(
            Some((1, KVValue {
                meta: None,
                value: b"v1".to_vec()
            })),
            r.prev
        );
        assert_eq!(
            Some((1, KVValue {
                meta: None,
                value: b"v1".to_vec()
            })),
            r.result
        );

        // matched seq
        let r = client
            .upsert_kv(test_key, MatchSeq::Exact(seq), Some(b"v2".to_vec()), None)
            .await?;
        assert_eq!(
            Some((1, KVValue {
                meta: None,
                value: b"v1".to_vec()
            })),
            r.prev
        );
        assert_eq!(
            Some((2, KVValue {
                meta: None,
                value: b"v2".to_vec()
            })),
            r.result
        );

        // blind update
        let r = client
            .upsert_kv(test_key, MatchSeq::GE(1), Some(b"v3".to_vec()), None)
            .await?;
        assert_eq!(
            Some((2, KVValue {
                meta: None,
                value: b"v2".to_vec()
            })),
            r.prev
        );
        assert_eq!(
            Some((3, KVValue {
                meta: None,
                value: b"v3".to_vec()
            })),
            r.result
        );

        // value updated
        let kv = client.get_kv(test_key).await?;
        assert!(kv.result.is_some());
        assert_eq!(kv.result.unwrap().1, KVValue {
            meta: None,
            value: b"v3".to_vec()
        });
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_generic_kv_update_meta() -> anyhow::Result<()> {
    // Only update meta, do not touch the value part.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();
    {
        let span = tracing::span!(tracing::Level::INFO, "test_flight_generic_kv_update_meta");
        let _ent = span.enter();

        let (_tc, addr) = crate::tests::start_metasrv().await?;

        let client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

        let test_key = "test_key_for_update_meta";

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let r = client
            .upsert_kv(test_key, MatchSeq::Any, Some(b"v1".to_vec()), None)
            .await?;
        assert_eq!(
            Some((1, KVValue {
                meta: None,
                value: b"v1".to_vec()
            })),
            r.result
        );
        let seq = r.result.unwrap().0;

        tracing::info!("--- mismatching seq does nothing");

        let r = client
            .update_kv_meta(
                test_key,
                MatchSeq::Exact(seq + 1),
                Some(KVMeta {
                    expire_at: Some(now + 20),
                }),
            )
            .await?;
        assert_eq!(
            Some((1, KVValue {
                meta: None,
                value: b"v1".to_vec()
            })),
            r.prev
        );
        assert_eq!(
            Some((1, KVValue {
                meta: None,
                value: b"v1".to_vec()
            })),
            r.result
        );

        tracing::info!("--- matching seq only update meta");

        let r = client
            .update_kv_meta(
                test_key,
                MatchSeq::Exact(seq),
                Some(KVMeta {
                    expire_at: Some(now + 20),
                }),
            )
            .await?;
        assert_eq!(
            Some((1, KVValue {
                meta: None,
                value: b"v1".to_vec()
            })),
            r.prev
        );
        assert_eq!(
            Some((2, KVValue {
                meta: Some(KVMeta {
                    expire_at: Some(now + 20)
                }),
                value: b"v1".to_vec()
            })),
            r.result
        );

        tracing::info!("--- get returns the value with meta and seq updated");
        let kv = client.get_kv(test_key).await?;
        assert!(kv.result.is_some());
        assert_eq!(
            (seq + 1, KVValue {
                meta: Some(KVMeta {
                    expire_at: Some(now + 20)
                }),
                value: b"v1".to_vec()
            }),
            kv.result.unwrap(),
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_generic_kv_timeout() -> anyhow::Result<()> {
    // - Test get  expired and non-expired.
    // - Test mget expired and non-expired.
    // - Test list expired and non-expired.
    // - Test update with a new expire value.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();
    {
        let span = tracing::span!(tracing::Level::INFO, "test_flight_generic_kv_timeout");
        let _ent = span.enter();

        let (_tc, addr) = crate::tests::start_metasrv().await?;

        let client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        client
            .upsert_kv(
                "k1",
                MatchSeq::Any,
                Some(b"v1".to_vec()),
                Some(KVMeta {
                    expire_at: Some(now + 1),
                }),
            )
            .await?;

        tracing::info!("---get unexpired");
        {
            let res = client.get_kv(&"k1".to_string()).await?;
            assert!(res.result.is_some(), "got unexpired");
        }

        tracing::info!("---get expired");
        {
            tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;
            let res = client.get_kv(&"k1".to_string()).await?;
            tracing::debug!("got k1:{:?}", res);
            assert!(res.result.is_none(), "got expired");
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        tracing::info!("--- expired entry act as if it does not exist, an ADD op should apply");
        {
            client
                .upsert_kv(
                    "k1",
                    MatchSeq::Exact(0),
                    Some(b"v1".to_vec()),
                    Some(KVMeta {
                        expire_at: Some(now - 1),
                    }),
                )
                .await?;
            client
                .upsert_kv(
                    "k2",
                    MatchSeq::Exact(0),
                    Some(b"v2".to_vec()),
                    Some(KVMeta {
                        expire_at: Some(now + 2),
                    }),
                )
                .await?;

            tracing::info!("--- mget should not return expired");
            let res = client
                .mget_kv(&["k1".to_string(), "k2".to_string()])
                .await?;
            assert_eq!(res.result, vec![
                None,
                Some((3, KVValue {
                    meta: Some(KVMeta {
                        expire_at: Some(now + 2)
                    }),
                    value: b"v2".to_vec()
                })),
            ]);
        }

        tracing::info!("--- list should not return expired");
        {
            let res = client.prefix_list_kv("k").await?;
            let res_vec = res.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();

            assert_eq!(res_vec, vec!["k2".to_string(),]);
        }

        tracing::info!("--- update expire");
        {
            client
                .upsert_kv(
                    "k2",
                    MatchSeq::Exact(3),
                    Some(b"v2".to_vec()),
                    Some(KVMeta {
                        expire_at: Some(now - 1),
                    }),
                )
                .await?;

            let res = client.get_kv(&"k2".to_string()).await?;
            assert!(res.result.is_none(), "k2 expired");
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_generic_kv() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    {
        let span = tracing::span!(tracing::Level::INFO, "test_flight_generic_kv");
        let _ent = span.enter();

        let (_tc, addr) = crate::tests::start_metasrv().await?;

        let client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

        {
            // write
            let res = client
                .upsert_kv("foo", MatchSeq::Any, Some(b"bar".to_vec()), None)
                .await?;
            assert_eq!(None, res.prev);
            assert_eq!(
                Some((1, KVValue {
                    meta: None,
                    value: b"bar".to_vec()
                })),
                res.result
            );
        }

        {
            // write fails with unmatched seq
            let res = client
                .upsert_kv("foo", MatchSeq::Exact(2), Some(b"bar".to_vec()), None)
                .await?;
            assert_eq!(
                (
                    Some((1, KVValue {
                        meta: None,
                        value: b"bar".to_vec()
                    })),
                    Some((1, KVValue {
                        meta: None,
                        value: b"bar".to_vec(),
                    })),
                ),
                (res.prev, res.result),
                "nothing changed"
            );
        }

        {
            // write done with matching seq
            let res = client
                .upsert_kv("foo", MatchSeq::Exact(1), Some(b"wow".to_vec()), None)
                .await?;
            assert_eq!(
                Some((1, KVValue {
                    meta: None,
                    value: b"bar".to_vec()
                })),
                res.prev,
                "old value"
            );
            assert_eq!(
                Some((2, KVValue {
                    meta: None,
                    value: b"wow".to_vec()
                })),
                res.result,
                "new value"
            );
        }
    }

    Ok(())
}
