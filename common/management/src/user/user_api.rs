// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use async_trait::async_trait;
use common_exception::Result;

#[async_trait]
pub trait UserMgrApi {
    async fn add_user<U, V, W>(
        &mut self,
        username: U,
        password: V,
        salt: W,
    ) -> common_exception::Result<u64>
    where
        U: AsRef<str> + Send,
        V: AsRef<str> + Send,
        W: AsRef<str> + Send;

    async fn get_user<V>(
        &mut self,
        username: V,
        seq: Option<u64>,
    ) -> common_exception::Result<(u64, UserInfo)>
    where
        V: AsRef<str> + Send;

    async fn get_all_users(&mut self) -> Result<Vec<(u64, UserInfo)>>;

    async fn get_users<V>(&mut self, usernames: &[V]) -> Result<Vec<Option<(u64, UserInfo)>>>
    where V: AsRef<str> + Sync;

    async fn update_user<V>(
        &mut self,
        username: V,
        new_password: Option<V>,
        new_salt: Option<V>,
        seq: Option<u64>,
    ) -> Result<Option<u64>>
    where
        V: AsRef<str> + Sync + Send;

    async fn drop_user<V>(&mut self, username: V, seq: Option<u64>) -> Result<()>
    where V: AsRef<str> + Send;
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct UserInfo {
    pub name: String,
    pub password_sha256: [u8; 32],
    pub salt_sha256: [u8; 32],
}
