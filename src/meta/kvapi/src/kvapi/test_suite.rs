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

use databend_common_meta_types::protobuf as pb;
use databend_common_meta_types::txn_condition;
use databend_common_meta_types::txn_op;
use databend_common_meta_types::txn_op_response;
use databend_common_meta_types::ConditionResult;
use databend_common_meta_types::KVMeta;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaSpec;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnCondition;
use databend_common_meta_types::TxnDeleteByPrefixRequest;
use databend_common_meta_types::TxnDeleteByPrefixResponse;
use databend_common_meta_types::TxnDeleteRequest;
use databend_common_meta_types::TxnDeleteResponse;
use databend_common_meta_types::TxnGetRequest;
use databend_common_meta_types::TxnGetResponse;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnOpResponse;
use databend_common_meta_types::TxnPutResponse;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::With;
use fastrace::full_name;
use fastrace::func_name;
use log::debug;
use log::info;

use crate::kvapi;
use crate::kvapi::UpsertKVReq;

pub struct TestSuite {}

impl kvapi::TestSuite {
    #[fastrace::trace]
    pub async fn test_all<KV, B>(&self, builder: B) -> anyhow::Result<()>
    where
        KV: kvapi::KVApi,
        B: kvapi::ApiBuilder<KV>,
    {
        self.kv_write_read(&builder.build().await).await?;
        self.kv_delete(&builder.build().await).await?;
        self.kv_update(&builder.build().await).await?;
        self.kv_timeout(&builder.build().await).await?;
        self.kv_upsert_with_ttl(&builder.build().await).await?;
        self.kv_meta(&builder.build().await).await?;
        self.kv_list(&builder.build().await).await?;
        self.kv_mget(&builder.build().await).await?;
        self.kv_txn_absent_seq_0(&builder.build().await).await?;
        self.kv_transaction(&builder.build().await).await?;
        self.kv_transaction_with_ttl(&builder.build().await).await?;
        self.kv_transaction_delete_match_seq_none(&builder.build().await)
            .await?;
        self.kv_transaction_delete_match_seq_some_not_match(&builder.build().await)
            .await?;
        self.kv_transaction_delete_match_seq_some_match(&builder.build().await)
            .await?;
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

impl kvapi::TestSuite {
    #[fastrace::trace]
    pub async fn kv_write_read<KV: kvapi::KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- kvapi::KVApiTestSuite::kv_write_read() start");
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

    #[fastrace::trace]
    pub async fn kv_delete<KV: kvapi::KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- kvapi::KVApiTestSuite::kv_delete() start");
        let test_key = "test_key";
        kv.upsert_kv(UpsertKVReq::update(test_key, b"v1")).await?;

        let current = kv.get_kv(test_key).await?;
        if let Some(SeqV { seq, .. }) = current {
            // seq mismatch
            let wrong_seq = MatchSeq::Exact(seq + 1);
            let res = kv
                .upsert_kv(UpsertKVReq::new(
                    test_key,
                    wrong_seq,
                    Operation::Delete,
                    None,
                ))
                .await?;

            assert_eq!(res.prev, res.result);

            // dbg!("delete with wrong seq", &res);

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
        // dbg!("delete non-exist key", &res);

        assert_eq!(None, res.prev);
        assert_eq!(None, res.result);

        // do not care seq
        let _res = kv.upsert_kv(UpsertKVReq::update(test_key, b"v2")).await?;
        // dbg!("update with v2", &res);

        let res = kv.upsert_kv(UpsertKVReq::delete(test_key)).await?;
        // dbg!("delete", &res);

        assert_eq!(
            (Some(SeqV::with_meta(2, None, b"v2".to_vec())), None),
            (res.prev, res.result)
        );

        Ok(())
    }

    #[fastrace::trace]
    pub async fn kv_update<KV: kvapi::KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- kvapi::KVApiTestSuite::kv_update() start");
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

    #[fastrace::trace]
    pub async fn kv_timeout<KV: kvapi::KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- kvapi::KVApiTestSuite::kv_timeout() start");

        // - Test get  expired and non-expired.
        // - Test mget expired and non-expired.
        // - Test list expired and non-expired.
        // - Test update with a new expire value.

        let _res = kv
            .upsert_kv(
                UpsertKVReq::update("k1", b"v1").with(MetaSpec::new_ttl(Duration::from_secs(2))),
            )
            .await?;
        // dbg!("upsert non expired k1", _res);

        info!("---get unexpired");
        {
            let res = kv.get_kv("k1").await?;
            // dbg!("got non expired k1", &res);
            assert!(res.is_some(), "got unexpired");
        }

        info!("---get expired");
        {
            tokio::time::sleep(Duration::from_millis(3000)).await;
            let res = kv.get_kv("k1").await?;
            // dbg!("k1 expired", &res);
            debug!("got k1:{:?}", res);
            assert!(res.is_none(), "got expired");
        }

        let now_sec = SeqV::<()>::now_sec();

        info!("--- expired entry act as if it does not exist, an ADD op should apply");
        {
            // dbg!("start upsert expired k1");
            let _res = kv
                .upsert_kv(
                    UpsertKVReq::update("k1", b"v1")
                        .with(MatchSeq::Exact(0))
                        .with(MetaSpec::new_expire(now_sec - 1)),
                )
                .await?;
            // dbg!("update expired k1", _res);

            let _res = kv
                .upsert_kv(
                    UpsertKVReq::update("k2", b"v2")
                        .with(MatchSeq::Exact(0))
                        .with(MetaSpec::new_ttl(Duration::from_secs(10))),
                )
                .await?;
            // dbg!("update non expired k2", _res);

            info!("--- mget should not return expired");
            let mut res = kv.mget_kv(&["k1".to_string(), "k2".to_string()]).await?;
            {
                assert_eq!(res[0], None);

                let v2 = res.remove(1).unwrap();
                assert_eq!(v2.seq, 3);
                assert_eq!(v2.data, b("v2"));
                let v2_meta = v2.meta.unwrap();
                let expire_at_sec = v2_meta.get_expire_at_ms().unwrap() / 1000;
                let want = now_sec + 10;
                assert!((want..want + 2).contains(&expire_at_sec));
            }
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
                    .with(MetaSpec::new_expire(now_sec - 1)),
            )
            .await?;

            let res = kv.get_kv("k2").await?;
            assert!(res.is_none(), "k2 expired");
        }

        Ok(())
    }

    #[fastrace::trace]
    pub async fn kv_upsert_with_ttl<KV: kvapi::KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        // - Add with ttl

        info!("--- {}", full_name!());

        let _res = kv
            .upsert_kv(
                UpsertKVReq::update("k1", b"v1")
                    .with(MetaSpec::new_ttl(Duration::from_millis(2_000))),
            )
            .await?;

        info!("---get unexpired");
        {
            let res = kv.get_kv("k1").await?;
            assert!(res.is_some(), "got unexpired");
        }

        info!("---get expired");
        {
            tokio::time::sleep(Duration::from_millis(2_100)).await;
            let res = kv.get_kv("k1").await?;
            assert!(res.is_none(), "got expired");
        }

        Ok(())
    }

    #[fastrace::trace]
    pub async fn kv_meta<KV: kvapi::KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- kvapi::KVApiTestSuite::kv_meta() start");

        let test_key = "test_key_for_update_meta";

        let now_sec = SeqV::<()>::now_sec();

        let r = kv.upsert_kv(UpsertKVReq::update(test_key, b"v1")).await?;
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.result);
        let seq = r.result.unwrap().seq;

        info!("--- mismatching seq does nothing");

        let r = kv
            .upsert_kv(UpsertKVReq::new(
                test_key,
                MatchSeq::Exact(seq + 1),
                Operation::AsIs,
                Some(MetaSpec::new_ttl(Duration::from_secs(20))),
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
                Some(MetaSpec::new_ttl(Duration::from_secs(20))),
            ))
            .await?;
        assert_eq!(Some(SeqV::with_meta(1, None, b"v1".to_vec())), r.prev);

        {
            let res = r.result.unwrap();

            assert_eq!(res.seq, 2);
            assert_eq!(res.data, b("v1"));

            let meta = res.meta.unwrap();
            let expire_at_sec = meta.get_expire_at_ms().unwrap() / 1000;
            let want = now_sec + 20;
            assert!((want..want + 2).contains(&expire_at_sec));
        }

        info!("--- get returns the value with meta and seq updated");
        let key_value = kv.get_kv(test_key).await?;
        assert!(key_value.is_some());

        {
            let res = key_value.unwrap();

            assert_eq!(res.seq, seq + 1);
            assert_eq!(res.data, b("v1"));

            let meta = res.meta.unwrap();
            let expire_at_sec = meta.get_expire_at_ms().unwrap() / 1000;
            let want = now_sec + 20;
            assert!((want..want + 2).contains(&expire_at_sec));
        }

        Ok(())
    }

    #[fastrace::trace]
    pub async fn kv_list<KV: kvapi::KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- kvapi::KVApiTestSuite::kv_list() start");

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

    #[fastrace::trace]
    pub async fn kv_mget<KV: kvapi::KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- kvapi::KVApiTestSuite::kv_mget() start");

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
        expected: &[TxnOpResponse],
        success: bool,
    ) {
        assert_eq!(reply.success, success);
        let responses = &reply.responses;
        assert_eq!(responses.len(), expected.len());

        for i in 0..responses.len() {
            let resp = &responses[i];
            let expect_resp = &expected[i];

            assert_eq!(resp, expect_resp, "{}-th response", i);
        }
    }

    pub async fn kv_txn_absent_seq_0<KV: kvapi::KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- Absent record should has seq as 0");

        let k1 = "txn_0_absent";

        let txn_key = k1.to_string();
        let conditions = vec![TxnCondition {
            key: txn_key.clone(),
            expected: ConditionResult::Eq as i32,
            target: Some(txn_condition::Target::Seq(0)),
        }];

        let if_then: Vec<TxnOp> = vec![TxnOp::put(txn_key.clone(), b("new_v1"))];

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

    pub async fn kv_delete_by_prefix_transaction<KV: kvapi::KVApi>(
        &self,
        kv: &KV,
    ) -> anyhow::Result<()> {
        info!("--- kvapi::KVApiTestSuite::kv_delete_by_prefix_transaction() start");
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
            let txn_key = unmatch_keys.first().unwrap().to_string();
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
            let unmatch_prefix = unmatch_keys.first().unwrap().to_string();
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

    pub async fn kv_transaction<KV: kvapi::KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- kvapi::KVApiTestSuite::kv_transaction() start");
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

            let if_then: Vec<TxnOp> = vec![TxnOp::put(txn_key.clone(), b("new_v1"))];

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
                    prev_value: Some(pb::SeqV::from(SeqV::new(1, val1.clone()))),
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

            let if_then: Vec<TxnOp> = vec![TxnOp::put(txn_key1.clone(), b("new_v1"))];

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
                TxnOp::put(txn_key1.clone(), val1_new.clone()),
                // change k2
                TxnOp::put(txn_key2.clone(), b("new_v2")),
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
                        match_seq: None,
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
                        prev_value: Some(pb::SeqV::from(SeqV::new(4, val1.clone()))),
                    })),
                },
                // change k2
                TxnOpResponse {
                    response: Some(txn_op_response::Response::Put(TxnPutResponse {
                        key: txn_key2.clone(),
                        prev_value: Some(pb::SeqV::from(SeqV::new(5, val2.clone()))),
                    })),
                },
                // get k1
                TxnOpResponse {
                    response: Some(txn_op_response::Response::Get(TxnGetResponse {
                        key: txn_key1.clone(),
                        value: Some(pb::SeqV::from(SeqV::with_meta(
                            6,
                            Some(KVMeta::default()),
                            val1_new.clone(),
                        ))),
                    })),
                },
                // delete k1
                TxnOpResponse {
                    response: Some(txn_op_response::Response::Delete(TxnDeleteResponse {
                        key: txn_key1.clone(),
                        success: true,
                        prev_value: Some(pb::SeqV::from(SeqV::with_meta(
                            6,
                            Some(KVMeta::default()),
                            val1_new.clone(),
                        ))),
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
                TxnOp::put(txn_key1.clone(), val1_new.clone()),
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
                        prev_value: Some(pb::SeqV::from(SeqV::new(8, val1.clone()))),
                    })),
                },
                // get k1
                TxnOpResponse {
                    response: Some(txn_op_response::Response::Get(TxnGetResponse {
                        key: txn_key1.clone(),
                        value: Some(pb::SeqV::from(SeqV::with_meta(
                            9,
                            Some(KVMeta::default()),
                            val1_new.clone(),
                        ))),
                    })),
                },
            ];

            self.check_transaction_responses(&resp, &expected, true);
        }
        Ok(())
    }

    #[fastrace::trace]
    pub async fn kv_transaction_with_ttl<KV: kvapi::KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        // - Add a record via transaction with ttl

        info!("--- {}", full_name!());

        let txn = TxnRequest {
            condition: vec![],
            if_then: vec![TxnOp::put_with_ttl("k1", b("v1"), Some(2_000))],
            else_then: vec![],
        };

        let _resp = kv.transaction(txn).await?;

        info!("---get unexpired");
        {
            let res = kv.get_kv("k1").await?;
            assert!(res.is_some(), "got unexpired");
        }

        info!("---get expired");
        {
            tokio::time::sleep(Duration::from_millis(2_100)).await;
            let res = kv.get_kv("k1").await?;
            assert!(res.is_none(), "got expired");
        }

        Ok(())
    }

    /// If `TxnDeleteRequest.match_seq` is not set,
    /// the delete operation will always be executed.
    pub async fn kv_transaction_delete_match_seq_none<KV: kvapi::KVApi>(
        &self,
        kv: &KV,
    ) -> anyhow::Result<()> {
        info!("--- {}", func_name!());
        let key = || "txn_1_K1".to_string();
        let val = || b"v1".to_vec();

        kv.upsert_kv(UpsertKVReq::update(key(), &val())).await?;

        let txn = TxnRequest {
            condition: vec![],
            if_then: vec![TxnOp::delete(key())],
            else_then: vec![],
        };

        let resp = kv.transaction(txn).await?;

        let expected = vec![TxnOpResponse::delete(
            key(),
            true,
            Some(pb::SeqV::from(SeqV::new(1, val()))),
        )];

        self.check_transaction_responses(&resp, &expected, true);

        let got = kv.get_kv(&key()).await?;
        assert_eq!(None, got, "successful delete");

        Ok(())
    }

    /// If `TxnDeleteRequest.match_seq` is Some,
    /// no delete because the current seq is not equal to `match_seq`.
    pub async fn kv_transaction_delete_match_seq_some_not_match<KV: kvapi::KVApi>(
        &self,
        kv: &KV,
    ) -> anyhow::Result<()> {
        info!("--- {}", func_name!());
        let key = || "txn_1_K1".to_string();
        let val = || b"v1".to_vec();

        kv.upsert_kv(UpsertKVReq::update(key(), &val())).await?;

        let txn = TxnRequest {
            condition: vec![],
            if_then: vec![TxnOp::delete_exact(key(), Some(100))],
            else_then: vec![],
        };

        let resp = kv.transaction(txn).await?;

        let expected = vec![TxnOpResponse::delete(
            key(),
            false,
            Some(pb::SeqV::from(SeqV::new(1, val()))),
        )];

        self.check_transaction_responses(&resp, &expected, true);

        let got = kv.get_kv(&key()).await?;
        assert_eq!(
            Some(SeqV::new(1, val())),
            got,
            "not deleted due to non-matching seq"
        );

        Ok(())
    }

    /// If `TxnDeleteRequest.match_seq` is Some,
    /// delete because the current seq equal to `match_seq`.
    pub async fn kv_transaction_delete_match_seq_some_match<KV: kvapi::KVApi>(
        &self,
        kv: &KV,
    ) -> anyhow::Result<()> {
        info!("--- {}", func_name!());
        let key = || "txn_1_K1".to_string();
        let val = || b"v1".to_vec();

        kv.upsert_kv(UpsertKVReq::update(key(), &val())).await?;

        let txn = TxnRequest {
            condition: vec![],
            if_then: vec![TxnOp::delete_exact(key(), Some(1))],
            else_then: vec![],
        };

        let resp = kv.transaction(txn).await?;

        let expected = vec![TxnOpResponse::delete(
            key(),
            true,
            Some(pb::SeqV::from(SeqV::new(1, val()))),
        )];

        self.check_transaction_responses(&resp, &expected, true);

        let got = kv.get_kv(&key()).await?;
        assert_eq!(None, got, "successful delete");

        Ok(())
    }
}

/// Test that write and read should be forwarded to leader
impl kvapi::TestSuite {
    #[fastrace::trace]
    pub async fn kv_write_read_across_nodes<KV: kvapi::KVApi>(
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

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}
