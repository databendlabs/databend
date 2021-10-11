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
use common_exception::Result;
use common_meta_api::KVApi;
use common_meta_sled_store::init_temp_sled_db;
use common_meta_types::GetKVActionReply;
use common_meta_types::KVMeta;
use common_meta_types::KVValue;
use common_meta_types::MGetKVActionReply;
use common_meta_types::MatchSeq;
use common_meta_types::UpsertKVActionReply;
use common_tracing::tracing;

use crate::kv::KV;

#[tokio::test]
async fn test_kv_async_api() -> Result<()> {
    init_testing_sled_db();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let api = KV::new_temp().await?;

    tracing::info!("--- upsert");

    let res = api
        .upsert_kv(
            "upsert-key",
            MatchSeq::Any,
            Some(b"upsert-value".to_vec()),
            None,
        )
        .await?;

    assert_eq!(
        UpsertKVActionReply {
            prev: None,
            result: Some((1, KVValue {
                meta: None,
                value: b"upsert-value".to_vec(),
            }))
        },
        res
    );

    tracing::info!("--- update meta with mismatching seq");

    let res = api
        .update_kv_meta(
            "upsert-key",
            MatchSeq::Exact(10),
            Some(KVMeta {
                expire_at: Some(now + 20),
            }),
        )
        .await?;

    assert_eq!(
        UpsertKVActionReply {
            prev: Some((1, KVValue {
                meta: None,
                value: b"upsert-value".to_vec(),
            })),
            result: Some((1, KVValue {
                meta: None,
                value: b"upsert-value".to_vec(),
            }))
        },
        res,
        "unchanged with mismatching seq"
    );

    tracing::info!("--- update meta with matching seq");

    let res = api
        .update_kv_meta(
            "upsert-key",
            MatchSeq::Exact(1),
            Some(KVMeta {
                expire_at: Some(now + 20),
            }),
        )
        .await?;

    assert_eq!(
        UpsertKVActionReply {
            prev: Some((1, KVValue {
                meta: None,
                value: b"upsert-value".to_vec(),
            })),
            result: Some((2, KVValue {
                meta: Some(KVMeta {
                    expire_at: Some(now + 20)
                }),
                value: b"upsert-value".to_vec(),
            })),
        },
        res
    );

    tracing::info!("--- get_kv");

    let res = api.get_kv("upsert-key").await?;
    assert_eq!(
        GetKVActionReply {
            result: Some((2, KVValue {
                meta: Some(KVMeta {
                    expire_at: Some(now + 20)
                }),
                value: b"upsert-value".to_vec(),
            })),
        },
        res
    );

    tracing::info!("--- mget_kv");

    let _res = api
        .upsert_kv(
            "upsert-key-2",
            MatchSeq::Any,
            Some(b"upsert-value-2".to_vec()),
            None,
        )
        .await?;

    let res = api
        .mget_kv(&[
            "upsert-key".to_string(),
            "upsert-key-2".to_string(),
            "nonexistent".to_string(),
        ])
        .await?;

    assert_eq!(
        MGetKVActionReply {
            result: vec![
                Some((2, KVValue {
                    meta: Some(KVMeta {
                        expire_at: Some(now + 20)
                    }),
                    value: b"upsert-value".to_vec(),
                })),
                Some((3, KVValue {
                    meta: None,
                    value: b"upsert-value-2".to_vec(),
                })),
                None
            ]
        },
        res
    );

    tracing::info!("--- prefix_list_kv");

    let res = api.prefix_list_kv("upsert-key-").await?;
    assert_eq!(
        vec![(
            "upsert-key-2".to_string(),
            (3, KVValue {
                meta: None,
                value: b"upsert-value-2".to_vec(),
            })
        )],
        res
    );

    Ok(())
}

fn init_testing_sled_db() {
    let t = tempfile::tempdir().expect("create temp dir to sled db");
    init_temp_sled_db(t);
}
