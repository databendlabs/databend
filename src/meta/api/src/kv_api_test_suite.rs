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

use common_base::base::tokio;
use common_meta_types::txn_condition;
use common_meta_types::txn_op;
use common_meta_types::txn_op_response;
use common_meta_types::ConditionResult;
use common_meta_types::KVMeta;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::PbSeqV;
use common_meta_types::SeqV;
use common_meta_types::TxnCondition;
use common_meta_types::TxnDeleteByPrefixRequest;
use common_meta_types::TxnDeleteByPrefixResponse;
use common_meta_types::TxnDeleteRequest;
use common_meta_types::TxnDeleteResponse;
use common_meta_types::TxnGetRequest;
use common_meta_types::TxnGetResponse;
use common_meta_types::TxnOp;
use common_meta_types::TxnOpResponse;
use common_meta_types::TxnPutRequest;
use common_meta_types::TxnPutResponse;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use common_meta_types::UpsertKVReq;
use common_meta_types::With;
use tracing::debug;
use tracing::info;

use crate::ApiBuilder;
use crate::KVApi;

pub struct KVApiTestSuite {}

impl KVApiTestSuite {
    #[tracing::instrument(level = "info", skip(self, builder))]
    pub async fn test_all<KV, B>(&self, builder: B) -> anyhow::Result<()>
    where
        KV: KVApi,
        B: ApiBuilder<KV>,
    {
        self.kv_write_read(&builder.build().await).await?;
        self.kv_delete(&builder.build().await).await?;
        self.kv_update(&builder.build().await).await?;
        self.kv_timeout(&builder.build().await).await?;
        self.kv_meta(&builder.build().await).await?;
        self.kv_list(&builder.build().await).await?;
        self.kv_mget(&builder.build().await).await?;
        self.kv_txn_absent_seq_0(&builder.build().await).await?;
        self.kv_transaction(&builder.build().await).await?;
        self.kv_delete_by_prefix_transaction(&builder.build().await)
            .await?;

        // Run cross node test on every 2 adjacent nodes
        let mut i = 0;
        loop {
            let cluster = builder.build_cluster().await;
            self.kv_write_read_across_nodes(&cluster[i], &cluster[i + 1])
                .await?;

            if i + 1 == cluster.len() - 1 {
                break;
            }
            i += 1;
        }

        Ok(())
    }
}

impl KVApiTestSuite {
    #[tracing::instrument(level = "info", skip(self, kv))]
    pub async fn kv_write_read<KV: KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- KVApiTestSuite::kv_write_read() start");
        {
            // write
            let res = kv.upsert_kv(UpsertKVReq::update("foo", b"bar")).await?;
            assert_eq!(None, res.prev);
            assert_eq!(Some(SeqV::with_meta(1, None, b"bar".to_vec())), res.result);
        }

        {
            // write fails with unmatched seq
            let res = kv
                .upsert_kv(UpsertKVReq::update("foo", b"bar").with(MatchSeq::Exact(2)))
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
                .upsert_kv(UpsertKVReq::update("foo", b"wow").with(MatchSeq::Exact(1)))
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

    #[tracing::instrument(level = "info", skip(self, kv))]
    pub async fn kv_delete<KV: KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- KVApiTestSuite::kv_delete() start");
        let test_key = "test_key";
        kv.upsert_kv(UpsertKVReq::update(test_key, b"v1")).await?;

        let current = kv.get_kv(test_key).await?;
        if let Some(SeqV { seq, .. }) = current {
            // seq mismatch
            let wrong_seq = Some(seq + 1);
            let res = kv
                .upsert_kv(UpsertKVReq::new(
                    test_key,
                    wrong_seq.into(),
                    Operation::Delete,
                    None,
                ))
                .await?;

            assert_eq!(res.prev, res.result);

            // seq match
            let res = kv
                .upsert_kv(UpsertKVReq::delete(test_key).with(MatchSeq::Exact(seq)))
                .await?;
            assert!(res.result.is_none());

            // read nothing
            let r = kv.get_kv(test_key).await?;
            assert!(r.is_none());
        } else {
            panic!("expecting a value, but got nothing");
        }

        // key not exist
        let res = kv.upsert_kv(UpsertKVReq::delete("not exists")).await?;
        assert_eq!(None, res.prev);
        assert_eq!(None, res.result);

        // do not care seq
        kv.upsert_kv(UpsertKVReq::update(test_key, b"v2")).await?;

        let res = kv.upsert_kv(UpsertKVReq::delete(test_key)).await?;
        assert_eq!(
            (Some(SeqV::with_meta(2, None, b"v2".to_vec())), None),
            (res.prev, res.result)
        );

        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self, kv))]
    pub async fn kv_update<KV: KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- KVApiTestSuite::kv_update() start");
        let test_key = "test_key_for_update";

        let r = kv
            .upsert_kv(UpsertKVReq::update(test_key, b"v1").with(MatchSeq::GE(1)))
            .await?;
        assert_eq!((None, None), (r.prev, r.result), "not changed");

        let r = kv.upsert_kv(UpsertKVReq::update(test_key, b"v1")).await?;
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.result);
        let seq = r.result.unwrap().seq;

        // unmatched seq
        let r = kv
            .upsert_kv(UpsertKVReq::update(test_key, b"v2").with(MatchSeq::Exact(seq + 1)))
            .await?;
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.prev);
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.result);

        // matched seq
        let r = kv
            .upsert_kv(UpsertKVReq::update(test_key, b"v2").with(MatchSeq::Exact(seq)))
            .await?;
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.prev);
        assert_eq!(Some(SeqV::with_meta(2, None, b"v2".to_vec())), r.result);

        // blind update
        let r = kv
            .upsert_kv(UpsertKVReq::update(test_key, b"v3").with(MatchSeq::GE(1)))
            .await?;
        assert_eq!(Some(SeqV::with_meta(2, None, b"v2".to_vec())), r.prev);
        assert_eq!(Some(SeqV::with_meta(3, None, b"v3".to_vec())), r.result);

        // value updated
        let key_value = kv.get_kv(test_key).await?;
        assert!(key_value.is_some());
        let key_value = key_value.unwrap();
        assert_eq!(
            key_value,
            SeqV::with_meta(key_value.seq, None, b"v3".to_vec())
        );
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self, kv))]
    pub async fn kv_timeout<KV: KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- KVApiTestSuite::kv_timeout() start");

        // - Test get  expired and non-expired.
        // - Test mget expired and non-expired.
        // - Test list expired and non-expired.
        // - Test update with a new expire value.

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        kv.upsert_kv(UpsertKVReq::update("k1", b"v1").with(KVMeta {
            expire_at: Some(now + 2),
        }))
        .await?;

        info!("---get unexpired");
        {
            let res = kv.get_kv("k1").await?;
            assert!(res.is_some(), "got unexpired");
        }

        info!("---get expired");
        {
            tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;
            let res = kv.get_kv("k1").await?;
            debug!("got k1:{:?}", res);
            assert!(res.is_none(), "got expired");
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        info!("--- expired entry act as if it does not exist, an ADD op should apply");
        {
            kv.upsert_kv(
                UpsertKVReq::update("k1", b"v1")
                    .with(MatchSeq::Exact(0))
                    .with(KVMeta {
                        expire_at: Some(now - 1),
                    }),
            )
            .await?;
            kv.upsert_kv(
                UpsertKVReq::update("k2", b"v2")
                    .with(MatchSeq::Exact(0))
                    .with(KVMeta {
                        expire_at: Some(now + 2),
                    }),
            )
            .await?;

            info!("--- mget should not return expired");
            let res = kv.mget_kv(&["k1".to_string(), "k2".to_string()]).await?;
            assert_eq!(res, vec![
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

        info!("--- list should not return expired");
        {
            let res = kv.prefix_list_kv("k").await?;
            let res_vec = res.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();

            assert_eq!(res_vec, vec!["k2".to_string(),]);
        }

        info!("--- update expire");
        {
            kv.upsert_kv(
                UpsertKVReq::update("k2", b"v2")
                    .with(MatchSeq::Exact(3))
                    .with(KVMeta {
                        expire_at: Some(now - 1),
                    }),
            )
            .await?;

            let res = kv.get_kv("k2").await?;
            assert!(res.is_none(), "k2 expired");
        }

        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self, kv))]
    pub async fn kv_meta<KV: KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- KVApiTestSuite::kv_meta() start");

        let test_key = "test_key_for_update_meta";

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let r = kv.upsert_kv(UpsertKVReq::update(test_key, b"v1")).await?;
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.result);
        let seq = r.result.unwrap().seq;

        info!("--- mismatching seq does nothing");

        let r = kv
            .upsert_kv(UpsertKVReq::new(
                test_key,
                MatchSeq::Exact(seq + 1),
                Operation::AsIs,
                Some(KVMeta {
                    expire_at: Some(now + 20),
                }),
            ))
            .await?;
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.prev);
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.result);

        info!("--- matching seq only update meta");

        let r = kv
            .upsert_kv(UpsertKVReq::new(
                test_key,
                MatchSeq::Exact(seq),
                Operation::AsIs,
                Some(KVMeta {
                    expire_at: Some(now + 20),
                }),
            ))
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

        info!("--- get returns the value with meta and seq updated");
        let key_value = kv.get_kv(test_key).await?;
        assert!(key_value.is_some());
        assert_eq!(
            SeqV::with_meta(
                seq + 1,
                Some(KVMeta {
                    expire_at: Some(now + 20)
                }),
                b"v1".to_vec()
            ),
            key_value.unwrap(),
        );

        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self, kv))]
    pub async fn kv_list<KV: KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- KVApiTestSuite::kv_list() start");

        let mut values = vec![];
        {
            kv.upsert_kv(UpsertKVReq::update("t", b"")).await?;

            for i in 0..9 {
                let key = format!("__users/{}", i);
                let val = format!("val_{}", i);
                values.push(val.clone());
                kv.upsert_kv(UpsertKVReq::update(&key, val.as_bytes()))
                    .await?;
            }
            kv.upsert_kv(UpsertKVReq::update("v", b"")).await?;
        }

        let res = kv.prefix_list_kv("__users/").await?;
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

    #[tracing::instrument(level = "info", skip(self, kv))]
    pub async fn kv_mget<KV: KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- KVApiTestSuite::kv_mget() start");

        kv.upsert_kv(UpsertKVReq::update("k1", b"v1")).await?;
        kv.upsert_kv(UpsertKVReq::update("k2", b"v2")).await?;

        let res = kv.mget_kv(&["k1".to_string(), "k2".to_string()]).await?;
        assert_eq!(res, vec![
            Some(SeqV::with_meta(1, None, b"v1".to_vec(),)),
            // NOTE, the sequence number is increased globally (inside the namespace of generic kv)
            Some(SeqV::with_meta(2, None, b"v2".to_vec(),)),
        ]);

        let res = kv
            .mget_kv(&["k1".to_string(), "key_no exist".to_string()])
            .await?;
        assert_eq!(res, vec![Some(SeqV::new(1, b"v1".to_vec())), None]);

        Ok(())
    }

    fn check_transaction_responses(
        &self,
        reply: &TxnReply,
        expected: &Vec<TxnOpResponse>,
        success: bool,
    ) {
        assert_eq!(reply.success, success);
        let responses = &reply.responses;
        assert_eq!(responses.len(), expected.len());

        for i in 0..responses.len() {
            let resp = &responses[i];
            let expect_resp = &expected[i];

            assert_eq!(resp, expect_resp);
        }
    }

    pub async fn kv_txn_absent_seq_0<KV: KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- Absent record should has seq as 0");

        let k1 = "txn_0_absent";

        let txn_key = k1.to_string();
        let conditions = vec![TxnCondition {
            key: txn_key.clone(),
            expected: ConditionResult::Eq as i32,
            target: Some(txn_condition::Target::Seq(0)),
        }];

        let if_then: Vec<TxnOp> = vec![TxnOp {
            request: Some(txn_op::Request::Put(TxnPutRequest {
                key: txn_key.clone(),
                value: b"new_v1".to_vec(),
                prev_value: true,
            })),
        }];

        let else_then: Vec<TxnOp> = vec![];

        let txn = TxnRequest {
            condition: conditions,
            if_then,
            else_then,
        };

        let resp = kv.transaction(txn).await?;

        let expected: Vec<TxnOpResponse> = vec![TxnOpResponse {
            response: Some(txn_op_response::Response::Put(TxnPutResponse {
                key: txn_key.clone(),
                prev_value: None,
            })),
        }];

        self.check_transaction_responses(&resp, &expected, true);

        Ok(())
    }

    pub async fn kv_delete_by_prefix_transaction<KV: KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- KVApiTestSuite::kv_delete_by_prefix_transaction() start");
        let test_prefix = "test";

        let match_keys = vec![
            format!("{}_key1", test_prefix),
            format!("{}/key2", test_prefix),
            format!("{}key3", test_prefix),
        ];

        let unmatch_keys = vec!["teskey4".to_string()];

        for key in [match_keys.clone(), unmatch_keys.clone()].concat().iter() {
            kv.upsert_kv(UpsertKVReq::update(key, b"v1")).await?;

            let current = kv.get_kv(key).await?;
            assert!(current.is_some());
        }

        // test again with if condition
        {
            let txn_key = unmatch_keys.get(0).unwrap().to_string();
            let condition = vec![TxnCondition {
                key: txn_key.clone(),
                expected: ConditionResult::Gt as i32,
                target: Some(txn_condition::Target::Seq(0)),
            }];

            let if_then: Vec<TxnOp> = vec![TxnOp {
                request: Some(txn_op::Request::DeleteByPrefix(TxnDeleteByPrefixRequest {
                    prefix: test_prefix.to_string(),
                })),
            }];

            let else_then: Vec<TxnOp> = vec![];
            let txn = TxnRequest {
                condition,
                if_then,
                else_then,
            };

            let resp = kv.transaction(txn).await?;

            let expected: Vec<TxnOpResponse> = vec![TxnOpResponse {
                response: Some(txn_op_response::Response::DeleteByPrefix(
                    TxnDeleteByPrefixResponse {
                        prefix: test_prefix.to_string(),
                        count: match_keys.len() as u32,
                    },
                )),
            }];

            self.check_transaction_responses(&resp, &expected, true);

            for key in match_keys.iter() {
                let current = kv.get_kv(key).await?;
                assert!(current.is_none());
            }
            for key in unmatch_keys.iter() {
                let current = kv.get_kv(key).await?;
                assert!(current.is_some());
            }
        }

        // test again with else condition
        {
            let txn_key = "unmatch_keys".to_string();
            let unmatch_prefix = unmatch_keys.get(0).unwrap().to_string();
            let condition = vec![TxnCondition {
                key: txn_key.clone(),
                expected: ConditionResult::Gt as i32,
                target: Some(txn_condition::Target::Seq(0)),
            }];

            let if_then: Vec<TxnOp> = vec![];

            let else_then: Vec<TxnOp> = vec![TxnOp {
                request: Some(txn_op::Request::DeleteByPrefix(TxnDeleteByPrefixRequest {
                    prefix: unmatch_prefix.clone(),
                })),
            }];
            let txn = TxnRequest {
                condition,
                if_then,
                else_then,
            };

            let resp = kv.transaction(txn).await?;

            let expected: Vec<TxnOpResponse> = vec![TxnOpResponse {
                response: Some(txn_op_response::Response::DeleteByPrefix(
                    TxnDeleteByPrefixResponse {
                        prefix: unmatch_prefix.to_string(),
                        count: unmatch_keys.len() as u32,
                    },
                )),
            }];

            self.check_transaction_responses(&resp, &expected, false);

            for key in unmatch_keys.iter() {
                let current = kv.get_kv(key).await?;
                assert!(current.is_none());
            }
        }

        Ok(())
    }

    pub async fn kv_transaction<KV: KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- KVApiTestSuite::kv_transaction() start");
        // first case: get and set one key transaction
        {
            let k1 = "txn_1_K1";
            let val1 = b"v1".to_vec();

            // first insert k1 value
            kv.upsert_kv(UpsertKVReq::update(k1, &val1)).await?;

            // transaction by k1 condition
            let txn_key = k1.to_string();
            let condition = vec![TxnCondition {
                key: txn_key.clone(),
                expected: ConditionResult::Gt as i32,
                target: Some(txn_condition::Target::Seq(0)),
            }];

            let if_then: Vec<TxnOp> = vec![TxnOp {
                request: Some(txn_op::Request::Put(TxnPutRequest {
                    key: txn_key.clone(),
                    value: b"new_v1".to_vec(),
                    prev_value: true,
                })),
            }];

            let else_then: Vec<TxnOp> = vec![];
            let txn = TxnRequest {
                condition,
                if_then,
                else_then,
            };

            let resp = kv.transaction(txn).await?;

            let expected: Vec<TxnOpResponse> = vec![TxnOpResponse {
                response: Some(txn_op_response::Response::Put(TxnPutResponse {
                    key: txn_key.clone(),
                    prev_value: Some(PbSeqV::from(SeqV::new(1, val1.clone()))),
                })),
            }];

            self.check_transaction_responses(&resp, &expected, true);
        }
        // second case: get two key(one not exist) and set one key transaction
        {
            // first insert k1 value
            let k1 = "txn_2_K1";
            let k2 = "txn_2_K2";

            kv.upsert_kv(UpsertKVReq::update(k1, b"v1")).await?;

            // transaction by k1 and k2 condition
            let txn_key1 = k1.to_string();
            let txn_key2 = k2.to_string();

            let condition = vec![
                TxnCondition {
                    key: txn_key1.clone(),
                    expected: ConditionResult::Gt as i32,
                    target: Some(txn_condition::Target::Seq(0)),
                },
                TxnCondition {
                    key: txn_key2.clone(),
                    expected: ConditionResult::Gt as i32,
                    target: Some(txn_condition::Target::Seq(0)),
                },
            ];

            let if_then: Vec<TxnOp> = vec![TxnOp {
                request: Some(txn_op::Request::Put(TxnPutRequest {
                    key: txn_key1.clone(),
                    value: b"new_v1".to_vec(),
                    prev_value: true,
                })),
            }];

            let else_then: Vec<TxnOp> = vec![];
            let txn = TxnRequest {
                condition,
                if_then,
                else_then,
            };

            let resp = kv.transaction(txn).await?;

            // k2 not exists, so the resp MUST return false
            assert!(!resp.success);
            assert_eq!(resp.responses.len(), 0);
        }

        // 3rd case: get two key and set both key transaction
        {
            let k1 = "txn_3_K1";
            let val1 = b"v1".to_vec();
            let val1_new = b"v1_new".to_vec();

            let k2 = "txn_3_K2";
            let val2 = b"v1".to_vec();

            // first insert k1 and k2 value
            kv.upsert_kv(UpsertKVReq::update(k1, &val1)).await?;
            kv.upsert_kv(UpsertKVReq::update(k2, &val2)).await?;

            // transaction by k1 and k2 condition
            let txn_key1 = k1.to_string();
            let txn_key2 = k2.to_string();

            let condition = vec![
                TxnCondition {
                    key: txn_key1.clone(),
                    expected: ConditionResult::Gt as i32,
                    target: Some(txn_condition::Target::Seq(0)),
                },
                TxnCondition {
                    key: txn_key2.clone(),
                    expected: ConditionResult::Gt as i32,
                    target: Some(txn_condition::Target::Seq(0)),
                },
            ];

            let if_then: Vec<TxnOp> = vec![
                // change k1
                TxnOp {
                    request: Some(txn_op::Request::Put(TxnPutRequest {
                        key: txn_key1.clone(),
                        value: val1_new.to_vec(),
                        prev_value: true,
                    })),
                },
                // change k2
                TxnOp {
                    request: Some(txn_op::Request::Put(TxnPutRequest {
                        key: txn_key2.clone(),
                        value: b"new_v2".to_vec(),
                        prev_value: true,
                    })),
                },
                // get k1
                TxnOp {
                    request: Some(txn_op::Request::Get(TxnGetRequest {
                        key: txn_key1.clone(),
                    })),
                },
                // delete k1
                TxnOp {
                    request: Some(txn_op::Request::Delete(TxnDeleteRequest {
                        key: txn_key1.clone(),
                        prev_value: true,
                    })),
                },
                // get k1
                TxnOp {
                    request: Some(txn_op::Request::Get(TxnGetRequest {
                        key: txn_key1.clone(),
                    })),
                },
            ];

            let else_then: Vec<TxnOp> = vec![];
            let txn = TxnRequest {
                condition,
                if_then,
                else_then,
            };

            let resp = kv.transaction(txn).await?;

            let expected: Vec<TxnOpResponse> = vec![
                // change k1
                TxnOpResponse {
                    response: Some(txn_op_response::Response::Put(TxnPutResponse {
                        key: txn_key1.clone(),
                        prev_value: Some(PbSeqV::from(SeqV::new(4, val1.clone()))),
                    })),
                },
                // change k2
                TxnOpResponse {
                    response: Some(txn_op_response::Response::Put(TxnPutResponse {
                        key: txn_key2.clone(),
                        prev_value: Some(PbSeqV::from(SeqV::new(5, val2.clone()))),
                    })),
                },
                // get k1
                TxnOpResponse {
                    response: Some(txn_op_response::Response::Get(TxnGetResponse {
                        key: txn_key1.clone(),
                        value: Some(PbSeqV::from(SeqV::new(6, val1_new.clone()))),
                    })),
                },
                // delete k1
                TxnOpResponse {
                    response: Some(txn_op_response::Response::Delete(TxnDeleteResponse {
                        key: txn_key1.clone(),
                        success: true,
                        prev_value: Some(PbSeqV::from(SeqV::new(6, val1_new.clone()))),
                    })),
                },
                // get k1
                TxnOpResponse {
                    response: Some(txn_op_response::Response::Get(TxnGetResponse {
                        key: txn_key1.clone(),
                        value: None,
                    })),
                },
            ];

            self.check_transaction_responses(&resp, &expected, true);
        }

        // 4th case: get one key by value and set key transaction
        {
            let k1 = "txn_4_K1";
            let val1 = b"v1".to_vec();
            let val1_new = b"v1_new".to_vec();

            // first insert k1 value
            kv.upsert_kv(UpsertKVReq::update(k1, &val1)).await?;

            // transaction by k1 condition
            let txn_key1 = k1.to_string();

            let condition = vec![TxnCondition {
                key: txn_key1.clone(),
                expected: ConditionResult::Gt as i32,
                target: Some(txn_condition::Target::Value(b"v".to_vec())),
            }];

            let if_then: Vec<TxnOp> = vec![
                // change k1
                TxnOp {
                    request: Some(txn_op::Request::Put(TxnPutRequest {
                        key: txn_key1.clone(),
                        value: val1_new.to_vec(),
                        prev_value: true,
                    })),
                },
                // get k1
                TxnOp {
                    request: Some(txn_op::Request::Get(TxnGetRequest {
                        key: txn_key1.clone(),
                    })),
                },
            ];

            let else_then: Vec<TxnOp> = vec![];
            let txn = TxnRequest {
                condition,
                if_then,
                else_then,
            };

            let resp = kv.transaction(txn).await?;

            let expected: Vec<TxnOpResponse> = vec![
                // change k1
                TxnOpResponse {
                    response: Some(txn_op_response::Response::Put(TxnPutResponse {
                        key: txn_key1.clone(),
                        prev_value: Some(PbSeqV::from(SeqV::new(8, val1.clone()))),
                    })),
                },
                // get k1
                TxnOpResponse {
                    response: Some(txn_op_response::Response::Get(TxnGetResponse {
                        key: txn_key1.clone(),
                        value: Some(PbSeqV::from(SeqV::new(9, val1_new.clone()))),
                    })),
                },
            ];

            self.check_transaction_responses(&resp, &expected, true);
        }
        Ok(())
    }
}

/// Test that write and read should be forwarded to leader
impl KVApiTestSuite {
    #[tracing::instrument(level = "info", skip(self, kv1, kv2))]
    pub async fn kv_write_read_across_nodes<KV: KVApi>(
        &self,
        kv1: &KV,
        kv2: &KV,
    ) -> anyhow::Result<()> {
        let mut values = vec![];
        {
            kv1.upsert_kv(UpsertKVReq::update("t", b"t")).await?;

            for i in 0..9 {
                let key = format!("__users/{}", i);
                let val = format!("val_{}", i);
                values.push(val.clone());
                info!("--- Start upsert-kv: {}", key);
                kv1.upsert_kv(UpsertKVReq::update(&key, val.as_bytes()))
                    .await?;
                info!("--- Done upsert-kv: {}", key);
            }

            kv1.upsert_kv(UpsertKVReq::update("v", b"v")).await?;
        }

        info!("--- test get on other node");
        {
            let res = kv2.get_kv("t").await?;
            let res = res.unwrap();
            assert_eq!(b"t".to_vec(), res.data);
        }

        info!("--- test mget on other node");
        {
            let res = kv2.mget_kv(&["u".to_string(), "v".to_string()]).await?;
            assert_eq!(
                vec![
                    None,
                    Some(SeqV {
                        seq: 11,
                        meta: None,
                        data: b"v".to_vec()
                    })
                ],
                res
            );
        }

        info!("--- test list on other node");
        {
            let res = kv2.prefix_list_kv("__users/").await?;
            assert_eq!(
                res.iter()
                    .map(|(_key, val)| val.data.clone())
                    .collect::<Vec<_>>(),
                values
                    .iter()
                    .map(|v| v.as_bytes().to_vec())
                    .collect::<Vec<_>>()
            );
        }
        Ok(())
    }
}
