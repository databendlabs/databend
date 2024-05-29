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

use databend_common_exception::Result;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::SeqV;

#[async_trait::async_trait]
pub trait UserApi: Sync + Send {
    async fn add_user(&self, user_info: UserInfo, create_option: &CreateOption) -> Result<()>;

    async fn get_user(&self, user: UserIdentity, seq: MatchSeq) -> Result<SeqV<UserInfo>>;

    async fn get_users(&self) -> Result<Vec<SeqV<UserInfo>>>;

    async fn get_user_nums(&self) -> Result<usize>;

    /// General user's grants update.
    ///
    /// It fetches the user that matches the specified seq number, update it in place, then write it back with the seq it sees.
    ///
    /// Seq number ensures there is no other write happens between get and set.
    /// Example:
    /// ```ignore
    /// self.update_user_with(user_ident, MatchSeq::GE(1), |ui: &mut UserInfo| ui.update_auth_option(foo())).await;
    /// ```
    async fn update_user_with<F>(
        &self,
        user: UserIdentity,
        seq: MatchSeq,
        f: F,
    ) -> Result<Option<u64>>
    where
        F: FnOnce(&mut UserInfo) + Send;

    async fn drop_user(&self, user: UserIdentity, seq: MatchSeq) -> Result<()>;
}
