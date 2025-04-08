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
use databend_common_meta_types::protobuf::BooleanExpression;
use databend_common_meta_types::seq_value::KVMeta;
use databend_common_meta_types::seq_value::SeqV;
use databend_common_meta_types::txn_condition;
use databend_common_meta_types::txn_op;
use databend_common_meta_types::txn_op_response;
use databend_common_meta_types::txn_op_response::Response;
use databend_common_meta_types::ConditionResult;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaSpec;
use databend_common_meta_types::Operation;
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
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::With;
use display_more::DisplayOptionExt;
use display_more::DisplaySliceExt;
use fastrace::func_name;
use fastrace::func_path;
use log::debug;
use log::info;

use crate::kvapi;

pub struct TestSuite {}

impl kvapi::TestSuite {
    #[fastrace::trace]
    pub async fn test_all<KV, B>(&self, builder: B) -> anyhow::Result<()>
    where
        KV: kvapi::KVApi,
        B: kvapi::ApiBuilder<KV>,
    {
        self.test_single_node(&builder).await?;

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

    #[fastrace::trace]
    pub async fn test_single_node<KV, B>(&self, builder: &B) -> anyhow::Result<()>
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
        self.kv_transaction_condition_keys_with_prefix(&builder.build().await)
            .await?;
        self.kv_transaction_complex_conditions(&builder.build().await)
            .await?;
        self.kv_transaction_delete_match_seq_some_not_match(&builder.build().await)
            .await?;
        self.kv_transaction_delete_match_seq_some_match(&builder.build().await)
            .await?;
        self.kv_delete_by_prefix_transaction(&builder.build().await)
            .await?;

        Ok(())
    }
}

impl kvapi::TestSuite {
    #[fastrace::trace]
    pub async fn kv_write_read<KV: kvapi::KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- kvapi::KVApiTestSuite::kv_write_read() start");
        {
            // write
            let res = kv.upsert_kv(UpsertKV::update("foo", b"bar")).await?;
            assert_eq!(None, res.prev);
            assert_eq!(Some(SeqV::new(1, b("bar"))), res.result);
        }

        {
            // write fails with unmatched seq
            let res = kv
                .upsert_kv(UpsertKV::update("foo", b"bar").with(MatchSeq::Exact(2)))
                .await?;
            assert_eq!(
                (Some(SeqV::new(1, b("bar"))), Some(SeqV::new(1, b("bar"))),),
                (res.prev, res.result),
                "nothing changed"
            );
        }

        {
            // write done with matching seq
            let res = kv
                .upsert_kv(UpsertKV::update("foo", b"wow").with(MatchSeq::Exact(1)))
                .await?;
            assert_eq!(Some(SeqV::new(1, b("bar"))), res.prev, "old value");
            assert_eq!(Some(SeqV::new(2, b("wow"))), res.result, "new value");
        }

        Ok(())
    }

    #[fastrace::trace]
    pub async fn kv_delete<KV: kvapi::KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- kvapi::KVApiTestSuite::kv_delete() start");
        let test_key = "test_key";
        kv.upsert_kv(UpsertKV::update(test_key, b"v1")).await?;

        let current = kv.get_kv(test_key).await?;
        if let Some(SeqV { seq, .. }) = current {
            // seq mismatch
            let wrong_seq = MatchSeq::Exact(seq + 1);
            let res = kv
                .upsert_kv(UpsertKV::new(test_key, wrong_seq, Operation::Delete, None))
                .await?;

            assert_eq!(res.prev, res.result);

            // dbg!("delete with wrong seq", &res);

            // seq match
            let res = kv
                .upsert_kv(UpsertKV::delete(test_key).with(MatchSeq::Exact(seq)))
                .await?;
            assert!(res.result.is_none());

            // read nothing
            let r = kv.get_kv(test_key).await?;
            assert!(r.is_none());
        } else {
            panic!("expecting a value, but got nothing");
        }

        // key not exist
        let res = kv.upsert_kv(UpsertKV::delete("not exists")).await?;
        // dbg!("delete non-exist key", &res);

        assert_eq!(None, res.prev);
        assert_eq!(None, res.result);

        // do not care seq
        let _res = kv.upsert_kv(UpsertKV::update(test_key, b"v2")).await?;
        // dbg!("update with v2", &res);

        let res = kv.upsert_kv(UpsertKV::delete(test_key)).await?;
        // dbg!("delete", &res);

        assert_eq!((Some(SeqV::new(2, b("v2"))), None), (res.prev, res.result));

        Ok(())
    }

    #[fastrace::trace]
    pub async fn kv_update<KV: kvapi::KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- kvapi::KVApiTestSuite::kv_update() start");
        let test_key = "test_key_for_update";

        let r = kv
            .upsert_kv(UpsertKV::update(test_key, b"v1").with(MatchSeq::GE(1)))
            .await?;
        assert_eq!((None, None), (r.prev, r.result), "not changed");

        let r = kv.upsert_kv(UpsertKV::update(test_key, b"v1")).await?;
        assert_eq!(Some(SeqV::new(1, b("v1"))), r.result);
        let seq = r.result.unwrap().seq;

        // unmatched seq
        let r = kv
            .upsert_kv(UpsertKV::update(test_key, b"v2").with(MatchSeq::Exact(seq + 1)))
            .await?;
        assert_eq!(Some(SeqV::new(1, b("v1"))), r.prev);
        assert_eq!(Some(SeqV::new(1, b("v1"))), r.result);

        // matched seq
        let r = kv
            .upsert_kv(UpsertKV::update(test_key, b"v2").with(MatchSeq::Exact(seq)))
            .await?;
        assert_eq!(Some(SeqV::new(1, b("v1"))), r.prev);
        assert_eq!(Some(SeqV::new(2, b("v2"))), r.result);

        // blind update
        let r = kv
            .upsert_kv(UpsertKV::update(test_key, b"v3").with(MatchSeq::GE(1)))
            .await?;
        assert_eq!(Some(SeqV::new(2, b("v2"))), r.prev);
        assert_eq!(Some(SeqV::new(3, b("v3"))), r.result);

        // value updated
        let key_value = kv.get_kv(test_key).await?;
        assert!(key_value.is_some());
        let key_value = key_value.unwrap();
        assert_eq!(key_value, SeqV::new(key_value.seq, b("v3")));
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
                UpsertKV::update("k1", b"v1").with(MetaSpec::new_ttl(Duration::from_secs(2))),
            )
            .await?;
        // dbg!("upsert non expired k1", _res);

        info!("---get unexpired");
        {
            let res = kv.get_kv("k1").await?;
            // dbg!("got k1:{:?}", &res);
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
                    UpsertKV::update("k1", b"v1")
                        .with(MatchSeq::Exact(0))
                        .with(MetaSpec::new_expire(now_sec - 1)),
                )
                .await?;
            // dbg!("update expired k1", _res);

            let _res = kv
                .upsert_kv(
                    UpsertKV::update("k2", b"v2")
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
                UpsertKV::update("k2", b"v2")
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

        info!("--- {}", func_path!());

        let _res = kv
            .upsert_kv(
                UpsertKV::update("k1", b"v1").with(MetaSpec::new_ttl(Duration::from_millis(2_000))),
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

        let r = kv.upsert_kv(UpsertKV::update(test_key, b"v1")).await?;
        assert_eq!(Some(SeqV::new(1, b("v1"))), r.result);
        let seq = r.result.unwrap().seq;

        info!("--- mismatching seq does nothing");

        let r = kv
            .upsert_kv(UpsertKV::new(
                test_key,
                MatchSeq::Exact(seq + 1),
                Operation::Update(b("v1")),
                Some(MetaSpec::new_ttl(Duration::from_secs(20))),
            ))
            .await?;
        assert_eq!(Some(SeqV::new(1, b("v1"))), r.prev);
        assert_eq!(Some(SeqV::new(1, b("v1"))), r.result);

        info!("--- matching seq only update meta");

        let r = kv
            .upsert_kv(UpsertKV::new(
                test_key,
                MatchSeq::Exact(seq),
                Operation::Update(b("v1")),
                Some(MetaSpec::new_ttl(Duration::from_secs(20))),
            ))
            .await?;
        assert_eq!(Some(SeqV::new(1, b("v1"))), r.prev);

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
            kv.upsert_kv(UpsertKV::update("t", b"")).await?;

            for i in 0..9 {
                let key = format!("__users/{}", i);
                let val = format!("val_{}", i);
                values.push(val.clone());
                kv.upsert_kv(UpsertKV::update(&key, val.as_bytes())).await?;
            }
            kv.upsert_kv(UpsertKV::update("v", b"")).await?;
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

        kv.upsert_kv(UpsertKV::update("k1", b"v1")).await?;
        kv.upsert_kv(UpsertKV::update("k2", b"v2")).await?;

        let res = kv.mget_kv(&["k1".to_string(), "k2".to_string()]).await?;
        assert_eq!(res, vec![
            Some(SeqV::new(1, b("v1"),)),
            // NOTE, the sequence number is increased globally (inside the namespace of generic kv)
            Some(SeqV::new(2, b("v2"),)),
        ]);

        let res = kv
            .mget_kv(&["k1".to_string(), "key_no exist".to_string()])
            .await?;
        assert_eq!(res, vec![Some(SeqV::new(1, b("v1"))), None]);

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

        let txn = TxnRequest::new(conditions, if_then);

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
            kv.upsert_kv(UpsertKV::update(key, b"v1")).await?;

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

            let txn = TxnRequest::new(condition, if_then);

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

            let else_then: Vec<TxnOp> = vec![TxnOp {
                request: Some(txn_op::Request::DeleteByPrefix(TxnDeleteByPrefixRequest {
                    prefix: unmatch_prefix.clone(),
                })),
            }];
            let txn = TxnRequest::new(condition, vec![]).with_else(else_then);

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

    #[allow(clippy::bool_assert_comparison)]
    pub async fn kv_transaction<KV: kvapi::KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        info!("--- kvapi::KVApiTestSuite::kv_transaction() start");
        // first case: get and set one key transaction
        {
            let k1 = "txn_1_K1";
            let val1 = b("v1");

            // first insert k1 value
            kv.upsert_kv(UpsertKV::update(k1, &val1)).await?;

            // transaction by k1 condition
            let txn_key = k1.to_string();
            let condition = vec![TxnCondition {
                key: txn_key.clone(),
                expected: ConditionResult::Gt as i32,
                target: Some(txn_condition::Target::Seq(0)),
            }];

            let if_then: Vec<TxnOp> = vec![TxnOp::put(txn_key.clone(), b("new_v1"))];

            let txn = TxnRequest::new(condition, if_then);

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

            kv.upsert_kv(UpsertKV::update(k1, b"v1")).await?;

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

            let txn = TxnRequest::new(condition, if_then);

            let resp = kv.transaction(txn).await?;

            // k2 not exists, so the resp MUST return false
            assert!(!resp.success);
            assert_eq!(resp.responses.len(), 0);
        }

        // 3rd case: get two key and set both key transaction
        {
            let k1 = "txn_3_K1";
            let val1 = b("v1");
            let val1_new = b("v1_new");

            let k2 = "txn_3_K2";
            let val2 = b("v1");

            // first insert k1 and k2 value
            kv.upsert_kv(UpsertKV::update(k1, &val1)).await?;
            kv.upsert_kv(UpsertKV::update(k2, &val2)).await?;

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

            let txn = TxnRequest::new(condition, if_then);

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
            /// Convert Some(KvMeta{ expire: None }) to None to simplify the comparison
            fn norm(vs: Vec<TxnOpResponse>) -> Vec<TxnOpResponse> {
                vs.into_iter()
                    .map(|mut v| {
                        //
                        match &mut v.response {
                            Some(Response::Get(TxnGetResponse {
                                value: Some(pb::SeqV { meta, .. }),
                                ..
                            })) => {
                                if *meta == Some(pb::KvMeta { expire_at: None }) {
                                    *meta = None;
                                }
                            }
                            Some(Response::Put(TxnPutResponse {
                                prev_value: Some(pb::SeqV { meta, .. }),
                                ..
                            })) => {
                                if *meta == Some(pb::KvMeta { expire_at: None }) {
                                    *meta = None;
                                }
                            }
                            _ => {}
                        }
                        v
                    })
                    .collect()
            }

            let k1 = "txn_4_K1";

            kv.upsert_kv(UpsertKV::update(k1, b"v1")).await?;

            // Test eq value: success = false

            let txn = TxnRequest::new(vec![TxnCondition::eq_value(k1, b("v10"))], vec![
                TxnOp::put(k1, b("v2")),
                TxnOp::get(k1),
            ])
            .with_else(vec![TxnOp::get(k1)]);

            let resp = kv.transaction(txn).await?;

            let expected: Vec<TxnOpResponse> =
                vec![TxnOpResponse::get(k1, Some(SeqV::new(8, b("v1"))))];

            assert_eq!(resp.success, false);
            assert_eq!(resp.responses, expected);

            // Test eq value: success = true

            let txn = TxnRequest::new(vec![TxnCondition::eq_value(k1, b("v1"))], vec![
                TxnOp::put(k1, b("v2")),
                TxnOp::get(k1),
            ]);

            let resp = kv.transaction(txn).await?;

            let expected: Vec<TxnOpResponse> = vec![
                TxnOpResponse::put(k1, Some(pb::SeqV::new(8, b("v1")))),
                TxnOpResponse::get(k1, Some(SeqV::new(9, b("v2")))),
            ];

            assert_eq!(resp.success, true);
            assert_eq!(norm(resp.responses), norm(expected));

            // Test less than value: success = false

            let txn = TxnRequest::new(
                vec![TxnCondition::match_value(k1, ConditionResult::Lt, b("v2"))],
                vec![TxnOp::put(k1, b("v3")), TxnOp::get(k1)],
            )
            .with_else(vec![TxnOp::get(k1)]);

            let resp = kv.transaction(txn).await?;

            let expected: Vec<TxnOpResponse> =
                vec![TxnOpResponse::get(k1, Some(SeqV::new(9, b("v2"))))];

            assert_eq!(resp.success, false);
            assert_eq!(norm(resp.responses), norm(expected));

            // Test less than value: success = true

            let txn = TxnRequest::new(
                vec![TxnCondition::match_value(k1, ConditionResult::Lt, b("v3"))],
                vec![TxnOp::put(k1, b("v3")), TxnOp::get(k1)],
            )
            .with_else(vec![TxnOp::get(k1)]);

            let resp = kv.transaction(txn).await?;

            let expected: Vec<TxnOpResponse> = vec![
                TxnOpResponse::put(k1, Some(pb::SeqV::new(9, b("v2")))),
                TxnOpResponse::get(k1, Some(SeqV::new(10, b("v3")))),
            ];

            assert_eq!(resp.success, true);
            assert_eq!(norm(resp.responses), norm(expected));

            // Test less equal value: success = false

            let txn = TxnRequest::new(
                vec![TxnCondition::match_value(k1, ConditionResult::Le, b("v0"))],
                vec![TxnOp::put(k1, b("v4")), TxnOp::get(k1)],
            )
            .with_else(vec![TxnOp::get(k1)]);

            let resp = kv.transaction(txn).await?;

            let expected: Vec<TxnOpResponse> =
                vec![TxnOpResponse::get(k1, Some(SeqV::new(10, b("v3"))))];

            assert_eq!(resp.success, false);
            assert_eq!(norm(resp.responses), norm(expected));

            // Test less equal value: success = true

            let txn = TxnRequest::new(
                vec![TxnCondition::match_value(k1, ConditionResult::Le, b("v3"))],
                vec![TxnOp::put(k1, b("v4")), TxnOp::get(k1)],
            )
            .with_else(vec![TxnOp::get(k1)]);

            let resp = kv.transaction(txn).await?;

            let expected: Vec<TxnOpResponse> = vec![
                TxnOpResponse::put(k1, Some(pb::SeqV::new(10, b("v3")))),
                TxnOpResponse::get(k1, Some(SeqV::new(11, b("v4")))),
            ];

            assert_eq!(resp.success, true);
            assert_eq!(norm(resp.responses), norm(expected));

            // Test greater than value: success = false

            let txn = TxnRequest::new(
                vec![TxnCondition::match_value(k1, ConditionResult::Gt, b("v5"))],
                vec![TxnOp::put(k1, b("v5")), TxnOp::get(k1)],
            )
            .with_else(vec![TxnOp::get(k1)]);

            let resp = kv.transaction(txn).await?;

            let expected: Vec<TxnOpResponse> =
                vec![TxnOpResponse::get(k1, Some(SeqV::new(11, b("v4"))))];

            assert_eq!(resp.success, false);
            assert_eq!(norm(resp.responses), norm(expected));

            // Test greater than value: success = true

            let txn = TxnRequest::new(
                vec![TxnCondition::match_value(k1, ConditionResult::Gt, b("v3"))],
                vec![TxnOp::put(k1, b("v5")), TxnOp::get(k1)],
            )
            .with_else(vec![TxnOp::get(k1)]);

            let resp = kv.transaction(txn).await?;

            let expected: Vec<TxnOpResponse> = vec![
                TxnOpResponse::put(k1, Some(pb::SeqV::new(11, b("v4")))),
                TxnOpResponse::get(k1, Some(SeqV::new(12, b("v5")))),
            ];

            assert_eq!(resp.success, true);
            assert_eq!(norm(resp.responses), norm(expected));

            // Test greater equal value: success = false

            let txn = TxnRequest::new(
                vec![TxnCondition::match_value(k1, ConditionResult::Ge, b("v6"))],
                vec![TxnOp::put(k1, b("v6")), TxnOp::get(k1)],
            )
            .with_else(vec![TxnOp::get(k1)]);

            let resp = kv.transaction(txn).await?;

            let expected: Vec<TxnOpResponse> =
                vec![TxnOpResponse::get(k1, Some(SeqV::new(12, b("v5"))))];

            assert_eq!(resp.success, false);
            assert_eq!(norm(resp.responses), norm(expected));

            // Test greater equal value: success = true

            let txn = TxnRequest::new(
                vec![TxnCondition::match_value(k1, ConditionResult::Ge, b("v5"))],
                vec![TxnOp::put(k1, b("v6")), TxnOp::get(k1)],
            )
            .with_else(vec![TxnOp::get(k1)]);

            let resp = kv.transaction(txn).await?;

            let expected: Vec<TxnOpResponse> = vec![
                TxnOpResponse::put(k1, Some(pb::SeqV::new(12, b("v5")))),
                TxnOpResponse::get(k1, Some(SeqV::new(13, b("v6")))),
            ];

            assert_eq!(resp.success, true);
            assert_eq!(norm(resp.responses), norm(expected));
        }
        Ok(())
    }

    #[fastrace::trace]
    pub async fn kv_transaction_with_ttl<KV: kvapi::KVApi>(&self, kv: &KV) -> anyhow::Result<()> {
        // - Add a record via transaction with ttl

        info!("--- {}", func_path!());

        let txn = TxnRequest::new(vec![], vec![TxnOp::put_with_ttl(
            "k1",
            b("v1"),
            Some(Duration::from_millis(2_000)),
        )]);

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

    /// A transaction that checks the number of keys with given prefix.
    pub async fn kv_transaction_condition_keys_with_prefix<KV: kvapi::KVApi>(
        &self,
        kv: &KV,
    ) -> anyhow::Result<()> {
        let prefix = func_name!();

        let sample_keys_prefix = format!("{}/xxx", prefix);

        let sample = |suffix| format!("{}/{}", sample_keys_prefix, suffix);
        let positive = format!("{prefix}/positive");
        let negative = format!("{prefix}/negative");

        kv.upsert_kv(UpsertKV::update(sample("a"), &b("a"))).await?;
        kv.upsert_kv(UpsertKV::update(sample("b"), &b("b"))).await?;
        kv.upsert_kv(UpsertKV::update(sample("c"), &b("c"))).await?;

        use ConditionResult::*;

        // A transaction that set positive key if succeeded,
        // otherwise set the negative key.
        let txn = |op: ConditionResult, n: u64| {
            TxnRequest::new(
                vec![TxnCondition::match_keys_with_prefix(
                    &sample_keys_prefix,
                    op,
                    n,
                )],
                vec![TxnOp::put(&positive, b(format!("{op:?}")))],
            )
            .with_else(vec![TxnOp::put(&negative, b(format!("{op:?}")))])
        };

        for (op, n, expected) in [
            (Eq, 2, false),
            (Eq, 3, true),
            (Eq, 4, false),
            (Ne, 2, true),
            (Ne, 3, false),
            (Ne, 4, true),
            (Lt, 3, false),
            (Lt, 4, true),
            (Lt, 5, true),
            (Le, 2, false),
            (Le, 3, true),
            (Le, 4, true),
            (Gt, 2, true),
            (Gt, 3, false),
            (Gt, 4, false),
            (Ge, 2, true),
            (Ge, 3, true),
            (Ge, 4, false),
        ] {
            kv.upsert_kv(UpsertKV::update(&positive, &b(""))).await?;
            kv.upsert_kv(UpsertKV::update(&negative, &b(""))).await?;

            let resp = kv.transaction(txn(op, n)).await?;
            assert_eq!(
                resp.success, expected,
                "case: {op:?} {n}, expected: {expected}"
            );

            let expected_key = if expected { &positive } else { &negative };
            let got = kv.get_kv(expected_key).await?.unwrap().data;
            assert_eq!(
                got,
                b(format!("{op:?}")),
                "case: {op:?} {n}, expected: {expected}"
            );
        }

        Ok(())
    }

    pub async fn kv_transaction_complex_conditions<KV: kvapi::KVApi>(
        &self,
        kv: &KV,
    ) -> anyhow::Result<()> {
        let prefix = func_name!();

        let sample = |suffix: &str| format!("{}/{}", prefix, suffix);
        let result = format!("{prefix}/result");

        kv.upsert_kv(UpsertKV::update(sample("a"), &b("a"))).await?;
        kv.upsert_kv(UpsertKV::update(sample("b"), &b("b"))).await?;
        kv.upsert_kv(UpsertKV::update(sample("c"), &b("c"))).await?;

        // Build a simple equal-value condition
        let eq = |key: &str, val: &str| TxnCondition::eq_value(sample(key), b(val));

        let txn = |bools: Vec<Option<BooleanExpression>>, conditions: Vec<TxnCondition>| {
            let mut txn = TxnRequest::default();
            for (i, cond) in bools.into_iter().enumerate() {
                txn = txn.push_branch(cond, vec![pb::TxnOp::put(
                    &result,
                    b(format!("operation:{}", i)),
                )]);
            }

            txn.push_if_then(conditions, vec![pb::TxnOp::put(&result, b("then"))])
                .with_else(vec![pb::TxnOp::put(&result, b("else"))])
        };

        for (bools, condition, expected, index) in [
            (
                vec![],
                // empty condition is always true
                vec![],
                "then",
                None,
            ),
            (vec![], vec![eq("a", "a")], "then", None),
            (vec![], vec![eq("a", "b")], "else", None),
            (vec![], vec![eq("a", "a"), eq("b", "b")], "then", None),
            (vec![], vec![eq("a", "a"), eq("b", "c")], "else", None),
            (
                vec![
                    Some(eq("a", "a").and(eq("b", "b"))),
                    Some(eq("b", "b").or(eq("c", "c"))),
                ],
                vec![eq("a", "a")],
                "operation:0",
                Some(0),
            ),
            (
                vec![
                    Some(eq("a", "a").and(eq("b", "c"))),
                    Some(eq("b", "b").and(eq("c", "c"))),
                ],
                vec![eq("a", "a")],
                "operation:1",
                Some(1),
            ),
            (
                vec![
                    Some(eq("a", "a").and(eq("b", "c"))),
                    Some(eq("b", "b").and(eq("x", "x"))),
                ],
                vec![eq("a", "a")],
                "then",
                None,
            ),
            (
                vec![
                    Some(eq("a", "a").and(eq("b", "c"))),
                    Some(eq("b", "b").and(eq("x", "x"))),
                ],
                // empty condition is always true
                vec![],
                "then",
                None,
            ),
            (
                vec![
                    Some(eq("a", "a").and(eq("b", "c"))),
                    // None condition is always true
                    None,
                ],
                vec![eq("a", "a")],
                "operation:1",
                Some(1),
            ),
            (
                vec![Some(
                    eq("a", "a")
                        .or(eq("x", "x"))
                        .and(eq("b", "b").or(eq("y", "y"))),
                )],
                vec![eq("a", "b")],
                "operation:0",
                Some(0),
            ),
        ] {
            kv.upsert_kv(UpsertKV::update(&result, &b(""))).await?;

            let resp = kv
                .transaction(txn(bools.clone(), condition.clone()))
                .await?;

            let message = format!(
                "case: {} {}, expected: {expected}",
                bools
                    .into_iter()
                    .map(|b| b.display().to_string())
                    .collect::<Vec<_>>()
                    .display(),
                condition.display()
            );

            let want_success = expected != "else";

            assert_eq!(resp.success, want_success, "{}", message);
            assert_eq!(resp.execution_path, expected, "{}", message);
            assert_eq!(resp.executed_branch_index().unwrap(), index, "{}", message);

            let got = kv.get_kv(&result).await?.unwrap().data;
            assert_eq!(got, b(expected), "{}", message);
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
        let val = || b("v1");

        kv.upsert_kv(UpsertKV::update(key(), &val())).await?;

        let txn = TxnRequest::new(vec![], vec![TxnOp::delete(key())]);

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
        let val = || b("v1");

        kv.upsert_kv(UpsertKV::update(key(), &val())).await?;

        let txn = TxnRequest::new(vec![], vec![TxnOp::delete_exact(key(), Some(100))]);

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
        let val = || b("v1");

        kv.upsert_kv(UpsertKV::update(key(), &val())).await?;

        let txn = TxnRequest::new(vec![], vec![TxnOp::delete_exact(key(), Some(1))]);

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
            kv1.upsert_kv(UpsertKV::update("t", b"t")).await?;

            for i in 0..9 {
                let key = format!("__users/{}", i);
                let val = format!("val_{}", i);
                values.push(val.clone());
                info!("--- Start upsert-kv: {}", key);
                kv1.upsert_kv(UpsertKV::update(&key, val.as_bytes()))
                    .await?;
                info!("--- Done upsert-kv: {}", key);
            }

            kv1.upsert_kv(UpsertKV::update("v", b"v")).await?;
        }

        info!("--- test get on other node");
        {
            let res = kv2.get_kv("t").await?;
            let res = res.unwrap();
            assert_eq!(b("t"), res.data);
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
                        data: b("v")
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

fn b(x: impl ToString) -> Vec<u8> {
    x.to_string().as_bytes().to_vec()
}
