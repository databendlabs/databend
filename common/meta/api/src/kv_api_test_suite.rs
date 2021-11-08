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

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use common_base::tokio;
use common_meta_types::KVMeta;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_tracing::tracing;

use crate::KVApi;

pub struct KVApiTestSuite {}
impl KVApiTestSuite {
    pub async fn kv_write_read<KV: KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        {
            // write
            let res = kv
                .upsert_kv(
                    "foo",
                    MatchSeq::Any,
                    Operation::Update(b"bar".to_vec()),
                    None,
                )
                .await?;
            assert_eq!(None, res.prev);
            assert_eq!(Some(SeqV::with_meta(1, None, b"bar".to_vec())), res.result);
        }

        {
            // write fails with unmatched seq
            let res = kv
                .upsert_kv(
                    "foo",
                    MatchSeq::Exact(2),
                    Operation::Update(b"bar".to_vec()),
                    None,
                )
                .await?;
            assert_eq!(
                (
                    Some(SeqV::with_meta(1, None, b"bar".to_vec())),
                    Some(SeqV::with_meta(1, None, b"bar".to_vec())),
                ),
                (res.prev, res.result),
                "nothing changed"
            );
        }

        {
            // write done with matching seq
            let res = kv
                .upsert_kv(
                    "foo",
                    MatchSeq::Exact(1),
                    Operation::Update(b"wow".to_vec()),
                    None,
                )
                .await?;
            assert_eq!(
                Some(SeqV::with_meta(1, None, b"bar".to_vec())),
                res.prev,
                "old value"
            );
            assert_eq!(
                Some(SeqV::with_meta(2, None, b"wow".to_vec())),
                res.result,
                "new value"
            );
        }

        Ok(())
    }

    pub async fn kv_delete<KV: KVApi>(&self, client: &KV) -> anyhow::Result<()> {
        let test_key = "test_key";
        client
            .upsert_kv(
                test_key,
                MatchSeq::Any,
                Operation::Update(b"v1".to_vec()),
                None,
            )
            .await?;

        let current = client.get_kv(test_key).await?;
        if let Some(SeqV { seq, .. }) = current.result {
            // seq mismatch
            let wrong_seq = Some(seq + 1);
            let res = client
                .upsert_kv(test_key, wrong_seq.into(), Operation::Delete, None)
                .await?;

            assert_eq!(res.prev, res.result);

            // seq match
            let res = client
                .upsert_kv(test_key, MatchSeq::Exact(seq), Operation::Delete, None)
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
            .upsert_kv("not exists", MatchSeq::Any, Operation::Delete, None)
            .await?;
        assert_eq!(None, res.prev);
        assert_eq!(None, res.result);

        // do not care seq
        client
            .upsert_kv(
                test_key,
                MatchSeq::Any,
                Operation::Update(b"v2".to_vec()),
                None,
            )
            .await?;

        let res = client
            .upsert_kv(test_key, MatchSeq::Any, Operation::Delete, None)
            .await?;
        assert_eq!(
            (Some(SeqV::with_meta(2, None, b"v2".to_vec())), None),
            (res.prev, res.result)
        );

        Ok(())
    }

    pub async fn kv_update<KV: KVApi>(&self, client: &KV) -> anyhow::Result<()> {
        let test_key = "test_key_for_update";

        let r = client
            .upsert_kv(
                test_key,
                MatchSeq::GE(1),
                Operation::Update(b"v1".to_vec()),
                None,
            )
            .await?;
        assert_eq!((None, None), (r.prev, r.result), "not changed");

        let r = client
            .upsert_kv(
                test_key,
                MatchSeq::Any,
                Operation::Update(b"v1".to_vec()),
                None,
            )
            .await?;
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.result);
        let seq = r.result.unwrap().seq;

        // unmatched seq
        let r = client
            .upsert_kv(
                test_key,
                MatchSeq::Exact(seq + 1),
                Operation::Update(b"v2".to_vec()),
                None,
            )
            .await?;
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.prev);
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.result);

        // matched seq
        let r = client
            .upsert_kv(
                test_key,
                MatchSeq::Exact(seq),
                Operation::Update(b"v2".to_vec()),
                None,
            )
            .await?;
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.prev);
        assert_eq!(Some(SeqV::with_meta(2, None, b"v2".to_vec())), r.result);

        // blind update
        let r = client
            .upsert_kv(
                test_key,
                MatchSeq::GE(1),
                Operation::Update(b"v3".to_vec()),
                None,
            )
            .await?;
        assert_eq!(Some(SeqV::with_meta(2, None, b"v2".to_vec())), r.prev);
        assert_eq!(Some(SeqV::with_meta(3, None, b"v3".to_vec())), r.result);

        // value updated
        let kv = client.get_kv(test_key).await?;
        assert!(kv.result.is_some());
        let kv = kv.result.unwrap();
        assert_eq!(kv, SeqV::with_meta(kv.seq, None, b"v3".to_vec()));
        Ok(())
    }

    pub async fn kv_timeout<KV: KVApi>(&self, client: &KV) -> anyhow::Result<()> {
        // - Test get  expired and non-expired.
        // - Test mget expired and non-expired.
        // - Test list expired and non-expired.
        // - Test update with a new expire value.

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        client
            .upsert_kv(
                "k1",
                MatchSeq::Any,
                Operation::Update(b"v1".to_vec()),
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
                    Operation::Update(b"v1".to_vec()),
                    Some(KVMeta {
                        expire_at: Some(now - 1),
                    }),
                )
                .await?;
            client
                .upsert_kv(
                    "k2",
                    MatchSeq::Exact(0),
                    Operation::Update(b"v2".to_vec()),
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
                Some(SeqV::with_meta(
                    3,
                    Some(KVMeta {
                        expire_at: Some(now + 2)
                    }),
                    b"v2".to_vec()
                ))
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
                    Operation::Update(b"v2".to_vec()),
                    Some(KVMeta {
                        expire_at: Some(now - 1),
                    }),
                )
                .await?;

            let res = client.get_kv(&"k2".to_string()).await?;
            assert!(res.result.is_none(), "k2 expired");
        }

        Ok(())
    }

    pub async fn kv_meta<KV: KVApi>(&self, client: &KV) -> anyhow::Result<()> {
        let test_key = "test_key_for_update_meta";

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let r = client
            .upsert_kv(
                test_key,
                MatchSeq::Any,
                Operation::Update(b"v1".to_vec()),
                None,
            )
            .await?;
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.result);
        let seq = r.result.unwrap().seq;

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
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.prev);
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.result);

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
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.prev);
        assert_eq!(
            Some(SeqV::with_meta(
                2,
                Some(KVMeta {
                    expire_at: Some(now + 20)
                }),
                b"v1".to_vec()
            )),
            r.result
        );

        tracing::info!("--- get returns the value with meta and seq updated");
        let kv = client.get_kv(test_key).await?;
        assert!(kv.result.is_some());
        assert_eq!(
            SeqV::with_meta(
                seq + 1,
                Some(KVMeta {
                    expire_at: Some(now + 20)
                }),
                b"v1".to_vec()
            ),
            kv.result.unwrap(),
        );

        Ok(())
    }

    pub async fn kv_list<KV: KVApi>(&self, client: &KV) -> anyhow::Result<()> {
        let mut values = vec![];
        {
            client
                .upsert_kv(
                    "t",
                    MatchSeq::Any,
                    Operation::Update("".as_bytes().to_vec()),
                    None,
                )
                .await?;

            for i in 0..9 {
                let key = format!("__users/{}", i);
                let val = format!("val_{}", i);
                values.push(val.clone());
                client
                    .upsert_kv(
                        &key,
                        MatchSeq::Any,
                        Operation::Update(val.as_bytes().to_vec()),
                        None,
                    )
                    .await?;
            }
            client
                .upsert_kv("v", MatchSeq::Any, Operation::Update(b"".to_vec()), None)
                .await?;
        }

        let res = client.prefix_list_kv("__users/").await?;
        assert_eq!(
            res.iter()
                .map(|(_key, val)| val.data.clone())
                .collect::<Vec<_>>(),
            values
                .iter()
                .map(|v| v.as_bytes().to_vec())
                .collect::<Vec<_>>()
        );
        Ok(())
    }

    pub async fn kv_mget<KV: KVApi>(&self, client: &KV) -> anyhow::Result<()> {
        client
            .upsert_kv("k1", MatchSeq::Any, Operation::Update(b"v1".to_vec()), None)
            .await?;
        client
            .upsert_kv("k2", MatchSeq::Any, Operation::Update(b"v2".to_vec()), None)
            .await?;

        let res = client
            .mget_kv(&["k1".to_string(), "k2".to_string()])
            .await?;
        assert_eq!(res.result, vec![
            Some(SeqV::with_meta(1, None, b"v1".to_vec(),)),
            // NOTE, the sequence number is increased globally (inside the namespace of generic kv)
            Some(SeqV::with_meta(2, None, b"v2".to_vec(),)),
        ]);

        let res = client
            .mget_kv(&["k1".to_string(), "key_no exist".to_string()])
            .await?;
        assert_eq!(res.result, vec![Some(SeqV::new(1, b"v1".to_vec())), None]);

        Ok(())
    }
}
