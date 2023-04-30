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

use common_exception::Result;
use common_meta_app::principal::RoleInfo;
use common_meta_types::MatchSeq;
use common_meta_types::SeqV;

#[async_trait::async_trait]
pub trait RoleApi: Sync + Send {
    async fn add_role(&self, role_info: RoleInfo) -> Result<u64>;

    #[allow(clippy::ptr_arg)]
    async fn get_role(&self, role: &String, seq: MatchSeq) -> Result<SeqV<RoleInfo>>;

    async fn get_roles(&self) -> Result<Vec<SeqV<RoleInfo>>>;

    /// General role update.
    ///
    /// It fetches the role that matches the specified seq number, update it in place, then write it back with the seq it sees.
    ///
    /// Seq number ensures there is no other write happens between get and set.
    #[allow(clippy::ptr_arg)]
    async fn update_role_with<F>(&self, role: &String, seq: MatchSeq, f: F) -> Result<Option<u64>>
    where F: FnOnce(&mut RoleInfo) + Send;

    async fn drop_role(&self, role: String, seq: MatchSeq) -> Result<()>;
}
