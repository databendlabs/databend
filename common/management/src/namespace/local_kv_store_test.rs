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

use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use common_exception::Result;
use common_metatypes::KVMeta;
use common_metatypes::KVValue;
use common_metatypes::MatchSeq;
use common_runtime::tokio;
use common_store_api::kv_apis::kv_api::MGetKVActionResult;
use common_store_api::GetKVActionResult;
use common_store_api::KVApi;
use common_store_api::UpsertKVActionResult;
use common_store_api_sdk::StoreApiProvider;
use common_store_api_sdk::StoreClientConf;
use common_tracing::tracing;
use metasrv::meta_service::raft_db::init_temp_sled_db;

use crate::namespace::local_kv_store::LocalKVStore;
use crate::namespace::namespace_mgr::NamespaceMgr;
use crate::namespace::NamespaceApi;
use crate::namespace::NodeInfo;
//
// #[allow(dead_code)]
// async fn new_kv_api_with_store_api_provider() -> Result<Arc<dyn KVApi>> {
//     // this is for Api(compilation) testing only
//     //
//
//     // StoreAiProvider::new accepts a arg which can be converted
//     // into StoreClientConf, which query::configs::Conf implemented
//     // like this:
//     //
//     // - the constructor of StoreApiProvider
//     //
//     // ```
//     // pub fn new(conf: impl Into<StoreClientConf>) -> Self
//     // ```
//     // - the converter in crate `query`
//     //
//     // ```
//     //
//     // impl From<&Config> for StoreClientConf {
//     //  ...
//     // }
//     // ```
//     //
//     // since this crate is not supposed to be depended on crate `query`
//     // we can not demo it. instead we passes in a default StoreClientConf
//     //
//     // please DO NOT use the bare default config, which will lead to runtime error
//
//     let conf = StoreClientConf::default();
//     let api_provider = StoreApiProvider::new(conf);
//     api_provider.try_get_kv_client().await
// }
//
// #[tokio::test]
// async fn test_mgr_backed_with_local_kv_store() -> Result<()> {
//     init_testing_sled_db();
//
//     let tenant_id = "tenant1";
//     let namespace_id = "cluster1";
//     let node_id = "node1";
//     let node = NodeInfo {
//         id: node_id.to_string(),
//         cpu_nums: 0,
//         version: 0,
//         flight_address: "".to_string(),
//     };
//
//     let api = LocalKVStore::new_temp().await?;
//
//     let mgr = NamespaceMgr::new(Arc::new(api));
//     let res = mgr
//         .add_node(
//             tenant_id.to_string(),
//             namespace_id.to_string(),
//             node.clone(),
//         )
//         .await?;
//
//     assert_eq!(1, res, "the seq of the first added node");
//
//     let got = mgr
//         .get_nodes(tenant_id.to_string(), namespace_id.to_string(), None)
//         .await?;
//
//     assert_eq!(vec![(1, node.clone())], got, "fetch added nodes");
//
//     Ok(())
// }
//
// #[tokio::test]
// async fn test_local_kv_store() -> Result<()> {
//     init_testing_sled_db();
//
//     let now = SystemTime::now()
//         .duration_since(UNIX_EPOCH)
//         .unwrap()
//         .as_secs();
//
//     let api = LocalKVStore::new_temp().await?;
//
//     tracing::info!("--- upsert");
//
//     let res = api
//         .upsert_kv(
//             "upsert-key",
//             MatchSeq::Any,
//             Some(b"upsert-value".to_vec()),
//             None,
//         )
//         .await?;
//
//     assert_eq!(
//         UpsertKVActionResult {
//             prev: None,
//             result: Some((1, KVValue {
//                 meta: None,
//                 value: b"upsert-value".to_vec(),
//             }))
//         },
//         res
//     );
//
//     tracing::info!("--- update meta with mismatching seq");
//
//     let res = api
//         .update_kv_meta(
//             "upsert-key",
//             MatchSeq::Exact(10),
//             Some(KVMeta {
//                 expire_at: Some(now + 20),
//             }),
//         )
//         .await?;
//
//     assert_eq!(
//         UpsertKVActionResult {
//             prev: Some((1, KVValue {
//                 meta: None,
//                 value: b"upsert-value".to_vec(),
//             })),
//             result: Some((1, KVValue {
//                 meta: None,
//                 value: b"upsert-value".to_vec(),
//             }))
//         },
//         res,
//         "unchanged with mismatching seq"
//     );
//
//     tracing::info!("--- update meta with matching seq");
//
//     let res = api
//         .update_kv_meta(
//             "upsert-key",
//             MatchSeq::Exact(1),
//             Some(KVMeta {
//                 expire_at: Some(now + 20),
//             }),
//         )
//         .await?;
//
//     assert_eq!(
//         UpsertKVActionResult {
//             prev: Some((1, KVValue {
//                 meta: None,
//                 value: b"upsert-value".to_vec(),
//             })),
//             result: Some((2, KVValue {
//                 meta: Some(KVMeta {
//                     expire_at: Some(now + 20)
//                 }),
//                 value: b"upsert-value".to_vec(),
//             })),
//         },
//         res
//     );
//
//     tracing::info!("--- get_kv");
//
//     let res = api.get_kv("upsert-key").await?;
//     assert_eq!(
//         GetKVActionResult {
//             result: Some((2, KVValue {
//                 meta: Some(KVMeta {
//                     expire_at: Some(now + 20)
//                 }),
//                 value: b"upsert-value".to_vec(),
//             })),
//         },
//         res
//     );
//
//     tracing::info!("--- mget_kv");
//
//     let _res = api
//         .upsert_kv(
//             "upsert-key-2",
//             MatchSeq::Any,
//             Some(b"upsert-value-2".to_vec()),
//             None,
//         )
//         .await?;
//
//     let res = api
//         .mget_kv(&[
//             "upsert-key".to_string(),
//             "upsert-key-2".to_string(),
//             "nonexistent".to_string(),
//         ])
//         .await?;
//
//     assert_eq!(
//         MGetKVActionResult {
//             result: vec![
//                 Some((2, KVValue {
//                     meta: Some(KVMeta {
//                         expire_at: Some(now + 20)
//                     }),
//                     value: b"upsert-value".to_vec(),
//                 })),
//                 Some((3, KVValue {
//                     meta: None,
//                     value: b"upsert-value-2".to_vec(),
//                 })),
//                 None
//             ]
//         },
//         res
//     );
//
//     tracing::info!("--- prefix_list_kv");
//
//     let res = api.prefix_list_kv("upsert-key-").await?;
//     assert_eq!(
//         vec![(
//             "upsert-key-2".to_string(),
//             (3, KVValue {
//                 meta: None,
//                 value: b"upsert-value-2".to_vec(),
//             })
//         )],
//         res
//     );
//
//     Ok(())
// }
//
// fn init_testing_sled_db() {
//     let t = tempfile::tempdir().expect("create temp dir to sled db");
//     init_temp_sled_db(t);
// }
