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

use common_exception::Result;
use common_meta_types::AuthInfo;
use common_meta_types::MatchSeq;
use common_meta_types::SeqV;
use common_meta_types::UserIdentity;
use common_meta_types::UserInfo;
use common_meta_types::UserOption;

#[async_trait::async_trait]
pub trait UserApi: Sync + Send {
    async fn add_user(&self, user_info: UserInfo) -> Result<u64>;

    async fn get_user(&self, user: UserIdentity, seq: MatchSeq) -> Result<SeqV<UserInfo>>;

    async fn get_users(&self) -> Result<Vec<SeqV<UserInfo>>>;

    /// General user's grants update.
    ///
    /// It fetches the role that matches the specified seq number, update it in place, then write it back with the seq it sees.
    ///
    /// Seq number ensures there is no other write happens between get and set.
    async fn update_user_with<F>(
        &self,
        user: UserIdentity,
        seq: MatchSeq,
        f: F,
    ) -> Result<Option<u64>>
    where
        F: FnOnce(&mut UserInfo) + Send;

    async fn update_user(
        &self,
        user: UserIdentity,
        auth_info: Option<AuthInfo>,
        user_option: Option<UserOption>,
        seq: MatchSeq,
    ) -> Result<Option<u64>>;

    async fn drop_user(&self, user: UserIdentity, seq: MatchSeq) -> Result<()>;
}
