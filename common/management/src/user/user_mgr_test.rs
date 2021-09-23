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
use common_metatypes::KVMeta;
use common_metatypes::MatchSeq;
use common_runtime::tokio;
use common_store_api::kv_apis::kv_api::MGetKVActionResult;
use common_store_api::kv_apis::kv_api::PrefixListReply;
use common_store_api::GetKVActionResult;
use common_store_api::KVApi;
use common_store_api::UpsertKVActionResult;
use mockall::predicate::*;
use mockall::*;
use sha2::Digest;

use crate::user::user_api::AuthType;
use crate::user::user_api::UserInfo;
use crate::user::user_api::UserMgrApi;
use crate::user::utils::NewUser;
use crate::UserMgr;

// and mock!
mock! {
    pub KV {}
    #[async_trait]
    impl KVApi for KV {
        async fn upsert_kv(
            &self,
            key: &str,
            seq: MatchSeq,
            value: Option<Vec<u8>>,
            value_meta: Option<KVMeta>
        ) -> common_exception::Result<UpsertKVActionResult>;

        async fn update_kv_meta(
            &self,
            key: &str,
            seq: MatchSeq,
            value_meta: Option<KVMeta>
        ) -> common_exception::Result<UpsertKVActionResult>;

        async fn get_kv(&self, key: &str) -> common_exception::Result<GetKVActionResult>;

        async fn mget_kv(
            &self,
            key: &[String],
        ) -> common_exception::Result<MGetKVActionResult>;

        async fn prefix_list_kv(&self, prefix: &str) -> common_exception::Result<PrefixListReply>;
        }
}
#[test]
fn test_user_info_converter() {
    let name = "name";
    let pass = "pass";
    let auth_type = AuthType::Sha256;
    let user = NewUser::new(name, pass, auth_type);
    let user_info = UserInfo::from(&user);
    assert_eq!(name, &user_info.name);
    let digest: [u8; 32] = sha2::Sha256::digest(pass.as_bytes()).into();
    assert_eq!(digest.to_vec(), user_info.password);
}

mod add {
    use common_metatypes::KVValue;

    use super::*;

    #[tokio::test]
    async fn test_add_user() -> common_exception::Result<()> {
        let test_user_name = "test_user";
        let test_password = "test_password";
        let auth_type = AuthType::Sha256;
        let new_user = NewUser::new(test_user_name, test_password, auth_type.clone());
        let user_info = UserInfo::from(new_user);
        let value = Some(serde_json::to_vec(&user_info)?);

        let test_key = format!("__fd_users/tenant1/{}", test_user_name);
        let test_seq = MatchSeq::Exact(0);

        // normal
        {
            let test_key = test_key.clone();
            let mut api = MockKV::new();
            api.expect_upsert_kv()
                .with(
                    predicate::function(move |v| v == test_key.as_str()),
                    predicate::eq(test_seq),
                    predicate::eq(value.clone()),
                    predicate::eq(None),
                )
                .times(1)
                .return_once(|_u, _s, _salt, _meta| {
                    Ok(UpsertKVActionResult {
                        prev: None,
                        result: None,
                    })
                });
            let api = Arc::new(api);
            let user_mgr = UserMgr::new(api, "tenant1");
            let res = user_mgr.add_user(user_info).await;

            assert_eq!(
                res.unwrap_err().code(),
                ErrorCode::UnknownException("").code()
            );
        }

        // already exists
        {
            let test_key = test_key.clone();
            let mut api = MockKV::new();
            api.expect_upsert_kv()
                .with(
                    predicate::function(move |v| v == test_key.as_str()),
                    predicate::eq(test_seq),
                    predicate::eq(value.clone()),
                    predicate::eq(None),
                )
                .times(1)
                .returning(|_u, _s, _salt, _meta| {
                    Ok(UpsertKVActionResult {
                        prev: Some((1, KVValue {
                            meta: None,
                            value: vec![],
                        })),
                        result: None,
                    })
                });

            let api = Arc::new(api);
            let user_mgr = UserMgr::new(api, "tenant1");

            let new_user = NewUser::new(test_user_name, test_password, auth_type.clone());
            let user_info = UserInfo::from(new_user);

            let res = user_mgr.add_user(user_info).await;

            assert_eq!(
                res.unwrap_err().code(),
                ErrorCode::UserAlreadyExists("").code()
            );
        }

        // unknown exception
        {
            let mut api = MockKV::new();
            api.expect_upsert_kv()
                .with(
                    predicate::function(move |v| v == test_key.as_str()),
                    predicate::eq(test_seq),
                    predicate::eq(value.clone()),
                    predicate::eq(None),
                )
                .times(1)
                .returning(|_u, _s, _salt, _meta| {
                    Ok(UpsertKVActionResult {
                        prev: None,
                        result: None,
                    })
                });

            let kv = Arc::new(api);

            let user_mgr = UserMgr::new(kv, "tenant1");
            let new_user = NewUser::new(test_user_name, test_password, auth_type);
            let user_info = UserInfo::from(new_user);
            let res = user_mgr.add_user(user_info).await;

            assert_eq!(
                res.unwrap_err().code(),
                ErrorCode::UnknownException("").code()
            );
        }
        Ok(())
    }
}

mod get {
    use common_metatypes::KVValue;

    use super::*;

    #[tokio::test]
    async fn test_get_user_seq_match() -> common_exception::Result<()> {
        let test_user_name = "test";
        let test_key = format!("__fd_users/tenant1/{}", test_user_name);

        let user = NewUser::new(test_user_name, "pass", AuthType::Sha256);
        let user_info = UserInfo::from(user);
        let value = serde_json::to_vec(&user_info)?;

        let mut kv = MockKV::new();
        kv.expect_get_kv()
            .with(predicate::function(move |v| v == test_key.as_str()))
            .times(1)
            .return_once(move |_k| {
                Ok(GetKVActionResult {
                    result: Some((1, KVValue { meta: None, value })),
                })
            });

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::new(kv, "tenant1");
        let res = user_mgr.get_user(test_user_name.to_string(), Some(1)).await;
        assert!(res.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_user_do_not_care_seq() -> common_exception::Result<()> {
        let test_user_name = "test";
        let test_key = format!("__fd_users/tenant1/{}", test_user_name);

        let user = NewUser::new(test_user_name, "pass", AuthType::Sha256);
        let user_info = UserInfo::from(user);
        let value = serde_json::to_vec(&user_info)?;

        let mut kv = MockKV::new();
        kv.expect_get_kv()
            .with(predicate::function(move |v| v == test_key.as_str()))
            .times(1)
            .return_once(move |_k| {
                Ok(GetKVActionResult {
                    result: Some((100, KVValue { meta: None, value })),
                })
            });

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::new(kv, "tenant1");
        let res = user_mgr.get_user(test_user_name.to_string(), None).await;
        assert!(res.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_get_user_not_exist() -> common_exception::Result<()> {
        let test_user_name = "test";
        let test_key = format!("__fd_users/tenant1/{}", test_user_name);

        let mut kv = MockKV::new();
        kv.expect_get_kv()
            .with(predicate::function(move |v| v == test_key.as_str()))
            .times(1)
            .return_once(move |_k| Ok(GetKVActionResult { result: None }));

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::new(kv, "tenant1");
        let res = user_mgr.get_user(test_user_name.to_string(), None).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().code(), ErrorCode::UnknownUser("").code());
        Ok(())
    }

    #[tokio::test]
    async fn test_get_user_not_exist_seq_mismatch() -> common_exception::Result<()> {
        let test_user_name = "test";
        let test_key = format!("__fd_users/tenant1/{}", test_user_name);

        let mut kv = MockKV::new();
        kv.expect_get_kv()
            .with(predicate::function(move |v| v == test_key.as_str()))
            .times(1)
            .return_once(move |_k| {
                Ok(GetKVActionResult {
                    result: Some((1, KVValue {
                        meta: None,
                        value: vec![],
                    })),
                })
            });

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::new(kv, "tenant1");
        let res = user_mgr.get_user(test_user_name.to_string(), Some(2)).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().code(), ErrorCode::UnknownUser("").code());
        Ok(())
    }

    #[tokio::test]
    async fn test_get_user_invalid_user_info_encoding() -> common_exception::Result<()> {
        let test_user_name = "test";
        let test_key = format!("__fd_users/tenant1/{}", test_user_name);

        let mut kv = MockKV::new();
        kv.expect_get_kv()
            .with(predicate::function(move |v| v == test_key.as_str()))
            .times(1)
            .return_once(move |_k| {
                Ok(GetKVActionResult {
                    result: Some((1, KVValue {
                        meta: None,
                        value: vec![1],
                    })),
                })
            });

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::new(kv, "tenant1");
        let res = user_mgr.get_user(test_user_name.to_string(), None).await;
        assert_eq!(
            res.unwrap_err().code(),
            ErrorCode::IllegalUserInfoFormat("").code()
        );

        Ok(())
    }
}

mod get_users {

    use common_metatypes::KVValue;
    use common_metatypes::SeqValue;

    use super::*;

    type UserInfos = Vec<(u64, UserInfo)>;
    fn prepare() -> common_exception::Result<(Vec<(String, SeqValue<KVValue>)>, UserInfos)> {
        let mut names = vec![];
        let mut keys = vec![];
        let mut res = vec![];
        let mut user_infos = vec![];
        for i in 0..9 {
            let name = format!("test_user_{}", i);
            names.push(name.clone());
            let key = format!("{}/{}", "tenant1", name);
            keys.push(key);
            let new_user = NewUser::new(&name, "pass", AuthType::Sha256);
            let user_info = UserInfo::from(new_user);
            res.push((
                "fake_key".to_string(),
                (i, KVValue {
                    meta: None,
                    value: serde_json::to_vec(&user_info)?,
                }),
            ));
            user_infos.push((i, user_info));
        }
        Ok((res, user_infos))
    }

    #[tokio::test]
    async fn test_get_users_normal() -> common_exception::Result<()> {
        let (res, user_infos) = prepare()?;
        let mut kv = MockKV::new();
        {
            let k = "__fd_users/tenant1";
            kv.expect_prefix_list_kv()
                .with(predicate::eq(k))
                .times(1)
                .return_once(|_p| Ok(res));
        }

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::new(kv, "tenant1");
        let res = user_mgr.get_users().await?;
        assert_eq!(res, user_infos);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_all_users_invalid_user_info_encoding() -> common_exception::Result<()> {
        let (mut res, _user_infos) = prepare()?;
        res.insert(
            8,
            (
                "fake_key".to_string(),
                (0, KVValue {
                    meta: None,
                    value: b"some arbitrary str".to_vec(),
                }),
            ),
        );

        let mut kv = MockKV::new();
        {
            let k = "__fd_users/tenant1";
            kv.expect_prefix_list_kv()
                .with(predicate::eq(k))
                .times(1)
                .return_once(|_p| Ok(res));
        }

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::new(kv, "tenant1");
        let res = user_mgr.get_users().await;
        assert_eq!(
            res.unwrap_err().code(),
            ErrorCode::IllegalUserInfoFormat("").code()
        );

        Ok(())
    }
}

mod drop {
    use common_metatypes::KVValue;

    use super::*;

    #[tokio::test]
    async fn test_drop_user_normal_case() -> common_exception::Result<()> {
        let mut kv = MockKV::new();
        let test_key = "__fd_users/tenant1/test";
        kv.expect_upsert_kv()
            .with(
                predicate::function(move |v| v == test_key),
                predicate::eq(MatchSeq::Any),
                predicate::eq(None),
                predicate::eq(None),
            )
            .times(1)
            .returning(|_k, _seq, _none, _meta| {
                Ok(UpsertKVActionResult {
                    prev: Some((1, KVValue {
                        meta: None,
                        value: vec![],
                    })),
                    result: None,
                })
            });
        let kv = Arc::new(kv);
        let user_mgr = UserMgr::new(kv, "tenant1");
        let res = user_mgr.drop_user("test".to_string(), None).await;
        assert!(res.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_user_unknown() -> common_exception::Result<()> {
        let mut kv = MockKV::new();
        let test_key = "__fd_users/tenant1/test";
        kv.expect_upsert_kv()
            .with(
                predicate::function(move |v| v == test_key),
                predicate::eq(MatchSeq::Any),
                predicate::eq(None),
                predicate::eq(None),
            )
            .times(1)
            .returning(|_k, _seq, _none, _meta| {
                Ok(UpsertKVActionResult {
                    prev: None,
                    result: None,
                })
            });
        let kv = Arc::new(kv);
        let user_mgr = UserMgr::new(kv, "tenant1");
        let res = user_mgr.drop_user("test".to_string(), None).await;
        assert_eq!(res.unwrap_err().code(), ErrorCode::UnknownUser("").code());
        Ok(())
    }
}

mod update {
    use common_metatypes::KVValue;

    use super::*;

    #[tokio::test]
    async fn test_update_user_normal_partial_update() -> common_exception::Result<()> {
        let test_user_name = "name";
        let test_key = format!("__fd_users/tenant1/{}", test_user_name);
        let test_seq = None;

        let old_pass = "old_key";
        let old_auth_type = AuthType::DoubleSha1;

        let user = NewUser::new(test_user_name, old_pass, old_auth_type);
        let user_info = UserInfo::from(user);
        let prev_value = serde_json::to_vec(&user_info)?;

        // get_kv should be called
        let mut kv = MockKV::new();
        {
            let test_key = test_key.clone();
            kv.expect_get_kv()
                .with(predicate::function(move |v| v == test_key.as_str()))
                .times(1)
                .return_once(move |_k| {
                    Ok(GetKVActionResult {
                        result: Some((0, KVValue {
                            meta: None,
                            value: prev_value,
                        })),
                    })
                });
        }

        // and then, update_kv should be called

        let new_pass = "new pass";
        let new_user = NewUser::new(test_user_name, new_pass, AuthType::DoubleSha1);

        let new_user_info = UserInfo::from(new_user);
        let new_value_with_old_salt = serde_json::to_vec(&new_user_info)?;

        kv.expect_upsert_kv()
            .with(
                predicate::function(move |v| v == test_key.as_str()),
                predicate::eq(MatchSeq::GE(1)),
                predicate::eq(Some(new_value_with_old_salt)),
                predicate::eq(None),
            )
            .times(1)
            .return_once(|_, _, _, _meta| {
                Ok(UpsertKVActionResult {
                    prev: None,
                    result: Some((0, KVValue {
                        meta: None,
                        value: vec![],
                    })),
                })
            });

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::new(kv, "tenant1");

        let res = user_mgr
            .update_user(
                test_user_name.to_string(),
                Some(new_user_info.password),
                None,
                test_seq,
            )
            .await;
        assert!(res.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_update_user_normal_full_update() -> common_exception::Result<()> {
        let test_user_name = "name";
        let test_key = format!("__fd_users/tenant1/{}", test_user_name);
        let test_seq = None;

        // - get_kv should NOT be called
        // - update_kv should be called

        let new_pass = "new_pass";
        let new_auth_type = AuthType::Sha256;

        let new_user = NewUser::new(test_user_name, new_pass, new_auth_type.clone());

        let new_user_info = UserInfo::from(new_user);
        let new_value = serde_json::to_vec(&new_user_info)?;

        let mut kv = MockKV::new();
        kv.expect_upsert_kv()
            .with(
                predicate::function(move |v| v == test_key.as_str()),
                predicate::eq(MatchSeq::GE(1)),
                predicate::eq(Some(new_value)),
                predicate::eq(None),
            )
            .times(1)
            .return_once(|_, _, _, _meta| {
                Ok(UpsertKVActionResult {
                    prev: None,
                    result: Some((0, KVValue {
                        meta: None,
                        value: vec![],
                    })),
                })
            });

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::new(kv, "tenant1");

        let res = user_mgr
            .update_user(
                test_user_name.to_string(),
                Some(new_user_info.password),
                Some(new_auth_type),
                test_seq,
            )
            .await;
        assert!(res.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_update_user_none_update() -> common_exception::Result<()> {
        // mock kv expects nothing
        let test_name = "name";
        let kv = MockKV::new();

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::new(kv, "tenant1");

        let new_password: Option<Vec<u8>> = None;
        let res = user_mgr
            .update_user(test_name.to_string(), new_password, None, None)
            .await;
        assert!(res.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_update_user_partial_unknown() -> common_exception::Result<()> {
        let test_user_name = "name";
        let test_key = format!("__fd_users/tenant1/{}", test_user_name);
        let test_seq = None;

        // if partial update, and get_kv returns None
        // update_kv should NOT be called
        let mut kv = MockKV::new();
        let test_key = test_key.clone();
        kv.expect_get_kv()
            .with(predicate::function(move |v| v == test_key.as_str()))
            .times(1)
            .return_once(move |_k| Ok(GetKVActionResult { result: None }));

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::new(kv, "tenant1");

        let res = user_mgr
            .update_user(
                test_user_name.to_string(),
                Some(Vec::from("new_pass".as_bytes())),
                None,
                test_seq,
            )
            .await;
        assert_eq!(res.unwrap_err().code(), ErrorCode::UnknownUser("").code());
        Ok(())
    }

    #[tokio::test]
    async fn test_update_user_full_unknown() -> common_exception::Result<()> {
        let test_user_name = "name";
        let test_key = format!("__fd_users/tenant1/{}", test_user_name);
        let test_seq = None;

        // get_kv should not be called
        let mut kv = MockKV::new();
        let test_key = test_key.clone();

        // upsert should be called
        kv.expect_upsert_kv()
            .with(
                predicate::function(move |v| v == test_key.as_str()),
                predicate::eq(MatchSeq::GE(1)),
                predicate::always(), // a little bit relax here, as we've covered it before
                predicate::eq(None),
            )
            .times(1)
            .returning(|_u, _s, _salt, _meta| {
                Ok(UpsertKVActionResult {
                    prev: None,
                    result: None,
                })
            });

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::new(kv, "tenant1");

        let res = user_mgr
            .update_user(
                test_user_name.to_string(),
                Some(Vec::from("new_pass".as_bytes())),
                Some(AuthType::Sha256),
                test_seq,
            )
            .await;
        assert_eq!(res.unwrap_err().code(), ErrorCode::UnknownUser("").code());
        Ok(())
    }
}
