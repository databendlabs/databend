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
use databend_common_meta_app::principal::user_token::QueryTokenInfo;

#[async_trait::async_trait]
pub trait TokenApi: Sync + Send {
    async fn upsert_token(
        &self,
        token_hash: &str,
        token_info: QueryTokenInfo,
        ttl_in_secs: u64,
        is_update: bool,
    ) -> Result<bool>;

    async fn get_token(&self, token_hash: &str) -> Result<Option<QueryTokenInfo>>;

    async fn drop_token(&self, token_hash: &str) -> Result<()>;
}
