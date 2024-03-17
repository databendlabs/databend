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
use databend_common_meta_app::principal::PasswordPolicy;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::SeqV;

#[async_trait::async_trait]
pub trait PasswordPolicyApi: Sync + Send {
    async fn add(
        &self,
        password_policy: PasswordPolicy,
        create_option: &CreateOption,
    ) -> Result<()>;

    async fn update(&self, password_policy: PasswordPolicy, seq: MatchSeq) -> Result<u64>;

    async fn remove(&self, name: &str, seq: MatchSeq) -> Result<()>;

    async fn get(&self, name: &str, seq: MatchSeq) -> Result<SeqV<PasswordPolicy>>;

    async fn list(&self) -> Result<Vec<PasswordPolicy>>;
}
