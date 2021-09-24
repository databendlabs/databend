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
//

use std::sync::Arc;

use async_trait::async_trait;
use common_exception::ErrorCode;
use common_exception::Result;
use common_metatypes::KVMeta;
use common_metatypes::KVValue;
use common_metatypes::MatchSeq;
use common_metatypes::SeqValue;
use common_store_api::kv_apis::kv_api::MGetKVActionResult;
use common_store_api::kv_apis::kv_api::PrefixListReply;
use common_store_api::GetKVActionResult;
use common_store_api::KVApi;
use common_store_api::UpsertKVActionResult;
use mockall::predicate::*;
use mockall::*;

use super::*;
use crate::namespace::namespace_mgr::NamespaceMgr;
use crate::namespace::namespace_mgr::NAMESPACE_API_KEY_PREFIX;
// and mock!
// mock! {
//     pub KV {}
//     #[async_trait]
//     impl KVApi for KV {
//         async fn upsert_kv(
//             &self,
//             key: &str,
//             seq: MatchSeq,
//             value: Option<Vec<u8>>,
//             value_meta: Option<KVMeta>
//         ) -> Result<UpsertKVActionResult>;
//
//         async fn update_kv_meta(
//             &self,
//             key: &str,
//             seq: MatchSeq,
//             value_meta: Option<KVMeta>
//         ) -> Result<UpsertKVActionResult>;
//
//         async fn get_kv(&self, key: &str) -> Result<GetKVActionResult>;
//
//         async fn mget_kv(&self,key: &[String],) -> Result<MGetKVActionResult>;
//
//         async fn prefix_list_kv(&self, prefix: &str) -> Result<PrefixListReply>;
//     }
// }
//
// type NodeInfos = Vec<(u64, NodeInfo)>;
// fn prepare() -> common_exception::Result<(Vec<(String, SeqValue<KVValue>)>, NodeInfos)> {
//     let tenant_id = "tenant_1";
//     let namespace_id = "namespace_1";
//
//     let mut res = vec![];
//     let mut node_infos = vec![];
//     for i in 0..9 {
//         let node_id = format!("test_node_{}", i);
//         let key = format!(
//             "{}/{}/{}",
//             NAMESPACE_API_KEY_PREFIX, tenant_id, namespace_id
//         );
//         let node_info = NodeInfo {
//             id: node_id,
//             cpu_nums: 0,
//             version: 0,
//             ip: "".to_string(),
//             port: 0,
//         };
//         res.push((
//             key,
//             (i, KVValue {
//                 meta: None,
//                 value: serde_json::to_vec(&node_info)?,
//             }),
//         ));
//         node_infos.push((i, node_info));
//     }
//     Ok((res, node_infos))
// }
//
// #[test]
// fn test_add_node() -> Result<()> {
//     let tenant_id = "tenant1";
//     let namespace_id = "cluster1";
//     let node_id = "node1";
//     let key = format!(
//         "{}/{}/{}/{}",
//         NAMESPACE_API_KEY_PREFIX, tenant_id, namespace_id, node_id
//     );
//     let node = NodeInfo {
//         id: node_id.to_string(),
//         cpu_nums: 0,
//         version: 0,
//         ip: "".to_string(),
//         port: 0,
//     };
//     let value = Some(serde_json::to_vec(&node)?);
//     let seq = MatchSeq::Exact(0);
//
//     // normal
//     {
//         let test_key = key.clone();
//         let mut api = MockKV::new();
//         api.expect_upsert_kv()
//             .with(
//                 predicate::function(move |v| v == test_key.as_str()),
//                 predicate::eq(seq),
//                 predicate::eq(value.clone()),
//                 predicate::eq(None),
//             )
//             .times(1)
//             .return_once(|_, _, _, _| {
//                 Ok(UpsertKVActionResult {
//                     prev: None,
//                     result: None,
//                 })
//             });
//
//         let api = Arc::new(api);
//         let mgr = NamespaceMgr::new(api);
//         let res = mgr.add_node(
//             tenant_id.to_string(),
//             namespace_id.to_string(),
//             node.clone(),
//         );
//
//         assert_eq!(
//             res.unwrap_err().code(),
//             ErrorCode::UnknownException("").code()
//         );
//     }
//
//     // already exists
//     {
//         let test_key = key.clone();
//         let mut api = MockKV::new();
//         api.expect_upsert_kv()
//             .with(
//                 predicate::function(move |v| v == test_key.as_str()),
//                 predicate::eq(seq),
//                 predicate::eq(value.clone()),
//                 predicate::eq(None),
//             )
//             .times(1)
//             .returning(|_, _, _, _| {
//                 Ok(UpsertKVActionResult {
//                     prev: Some((1, KVValue {
//                         meta: None,
//                         value: vec![],
//                     })),
//                     result: None,
//                 })
//             });
//
//         let api = Arc::new(api);
//         let mgr = NamespaceMgr::new(api);
//         let res = mgr.add_node(
//             tenant_id.to_string(),
//             namespace_id.to_string(),
//             node.clone(),
//         );
//
//         assert_eq!(
//             res.unwrap_err().code(),
//             ErrorCode::NamespaceNodeAlreadyExists("").code()
//         );
//     }
//
//     // unknown exception
//     {
//         let test_key = key.clone();
//         let mut api = MockKV::new();
//         api.expect_upsert_kv()
//             .with(
//                 predicate::function(move |v| v == test_key.as_str()),
//                 predicate::eq(seq),
//                 predicate::eq(value.clone()),
//                 predicate::eq(None),
//             )
//             .times(1)
//             .returning(|_u, _s, _salt, _meta| {
//                 Ok(UpsertKVActionResult {
//                     prev: None,
//                     result: None,
//                 })
//             });
//
//         let api = Arc::new(api);
//         let mgr = NamespaceMgr::new(api);
//         let res = mgr.add_node(tenant_id.to_string(), namespace_id.to_string(), node);
//
//         assert_eq!(
//             res.unwrap_err().code(),
//             ErrorCode::UnknownException("").code()
//         );
//     }
//
//     Ok(())
// }
//
// #[test]
// fn test_get_nodes_normal() -> Result<()> {
//     let (res, infos) = prepare()?;
//
//     let tenant_id = "tenant_1";
//     let namespace_id = "namespace_1";
//     let mut api = MockKV::new();
//     {
//         let test_key = format!(
//             "{}/{}/{}",
//             NAMESPACE_API_KEY_PREFIX, tenant_id, namespace_id
//         );
//         api.expect_prefix_list_kv()
//             .with(predicate::function(move |v| v == test_key.as_str()))
//             .times(1)
//             .return_once(|_p| Ok(res));
//     }
//
//     let api = Arc::new(api);
//     let mgr = NamespaceMgr::new(api);
//     let actual = mgr.get_nodes(tenant_id.to_string(), namespace_id.to_string(), None)?;
//     let expect = infos;
//     assert_eq!(actual, expect);
//
//     Ok(())
// }
//
// #[test]
// fn test_get_nodes_invalid_encoding() -> Result<()> {
//     let (mut res, _infos) = prepare()?;
//     res.insert(
//         8,
//         (
//             "fake_key".to_string(),
//             (0, KVValue {
//                 meta: None,
//                 value: b"some arbitrary str".to_vec(),
//             }),
//         ),
//     );
//
//     let tenant_id = "tenant_1";
//     let namespace_id = "namespace_1";
//     let mut api = MockKV::new();
//     {
//         let test_key = format!(
//             "{}/{}/{}",
//             NAMESPACE_API_KEY_PREFIX, tenant_id, namespace_id
//         );
//         api.expect_prefix_list_kv()
//             .with(predicate::function(move |v| v == test_key.as_str()))
//             .times(1)
//             .return_once(|_p| Ok(res));
//     }
//
//     let api = Arc::new(api);
//     let mgr = NamespaceMgr::new(api);
//     let res = mgr.get_nodes(tenant_id.to_string(), namespace_id.to_string(), None);
//
//     let actual = res.unwrap_err().code();
//     let expect = ErrorCode::NamespaceIllegalNodeFormat("").code();
//     assert_eq!(actual, expect);
//
//     Ok(())
// }
//
// #[test]
// fn test_update_node_normal() -> Result<()> {
//     let tenant_id = "tenant1";
//     let namespace_id = "cluster1";
//     let node_id = "node1";
//     let key = format!(
//         "{}/{}/{}/{}",
//         NAMESPACE_API_KEY_PREFIX, tenant_id, namespace_id, node_id
//     );
//     let node = NodeInfo {
//         id: node_id.to_string(),
//         cpu_nums: 0,
//         version: 0,
//         ip: "".to_string(),
//         port: 0,
//     };
//     let new_value = serde_json::to_vec(&node)?;
//
//     let mut api = MockKV::new();
//     api.expect_upsert_kv()
//         .with(
//             predicate::function(move |v| v == key.as_str()),
//             predicate::eq(MatchSeq::GE(1)),
//             predicate::eq(Some(new_value)),
//             predicate::eq(None),
//         )
//         .times(1)
//         .return_once(|_, _, _, _meta| {
//             Ok(UpsertKVActionResult {
//                 prev: None,
//                 result: Some((0, KVValue {
//                     meta: None,
//                     value: vec![],
//                 })),
//             })
//         });
//
//     let api = Arc::new(api);
//     let mgr = NamespaceMgr::new(api);
//     let res = mgr.update_node(tenant_id.to_string(), namespace_id.to_string(), node, None);
//
//     assert!(res.is_ok());
//     Ok(())
// }
//
// #[test]
// fn test_update_node_error() -> Result<()> {
//     let tenant_id = "tenant1";
//     let namespace_id = "cluster1";
//     let node_id = "node1";
//     let key = format!(
//         "{}/{}/{}/{}",
//         NAMESPACE_API_KEY_PREFIX, tenant_id, namespace_id, node_id
//     );
//     let node = NodeInfo {
//         id: node_id.to_string(),
//         cpu_nums: 0,
//         version: 0,
//         ip: "".to_string(),
//         port: 0,
//     };
//     let new_value = serde_json::to_vec(&node)?;
//
//     let mut api = MockKV::new();
//     api.expect_upsert_kv()
//         .with(
//             predicate::function(move |v| v == key.as_str()),
//             predicate::eq(MatchSeq::GE(1)),
//             predicate::eq(Some(new_value)),
//             predicate::eq(None),
//         )
//         .times(1)
//         .return_once(|_, _, _, _meta| {
//             Ok(UpsertKVActionResult {
//                 prev: None,
//                 result: None,
//             })
//         });
//
//     let api = Arc::new(api);
//     let mgr = NamespaceMgr::new(api);
//     let res = mgr.update_node(tenant_id.to_string(), namespace_id.to_string(), node, None);
//
//     let actual = res.unwrap_err().code();
//     let expect = ErrorCode::NamespaceUnknownNode("").code();
//     assert_eq!(actual, expect);
//
//     Ok(())
// }
//
// #[test]
// fn test_drop_node_normal() -> common_exception::Result<()> {
//     let tenant_id = "tenant1";
//     let namespace_id = "cluster1";
//     let node_id = "node1";
//     let key = format!(
//         "{}/{}/{}/{}",
//         NAMESPACE_API_KEY_PREFIX, tenant_id, namespace_id, node_id
//     );
//
//     let mut api = MockKV::new();
//     api.expect_upsert_kv()
//         .with(
//             predicate::function(move |v| v == key.as_str()),
//             predicate::eq(MatchSeq::Any),
//             predicate::eq(None),
//             predicate::eq(None),
//         )
//         .times(1)
//         .returning(|_, _, _, _| {
//             Ok(UpsertKVActionResult {
//                 prev: Some((1, KVValue {
//                     meta: None,
//                     value: vec![],
//                 })),
//                 result: None,
//             })
//         });
//
//     let api = Arc::new(api);
//     let mgr = NamespaceMgr::new(api);
//     let res = mgr.drop_node(
//         tenant_id.to_string(),
//         namespace_id.to_string(),
//         node_id.to_string(),
//         None,
//     );
//
//     assert!(res.is_ok());
//
//     Ok(())
// }
//
// #[test]
// fn test_drop_node_error() -> common_exception::Result<()> {
//     let tenant_id = "tenant1";
//     let namespace_id = "cluster1";
//     let node_id = "node1";
//     let key = format!(
//         "{}/{}/{}/{}",
//         NAMESPACE_API_KEY_PREFIX, tenant_id, namespace_id, node_id
//     );
//
//     let mut api = MockKV::new();
//     api.expect_upsert_kv()
//         .with(
//             predicate::function(move |v| v == key.as_str()),
//             predicate::eq(MatchSeq::Any),
//             predicate::eq(None),
//             predicate::eq(None),
//         )
//         .times(1)
//         .returning(|_k, _seq, _none, _meta| {
//             Ok(UpsertKVActionResult {
//                 prev: None,
//                 result: None,
//             })
//         });
//
//     let api = Arc::new(api);
//     let mgr = NamespaceMgr::new(api);
//     let res = mgr.drop_node(
//         tenant_id.to_string(),
//         namespace_id.to_string(),
//         "node1".to_string(),
//         None,
//     );
//
//     let actual = res.unwrap_err().code();
//     let expect = ErrorCode::NamespaceUnknownNode("").code();
//     assert_eq!(actual, expect);
//     Ok(())
// }
