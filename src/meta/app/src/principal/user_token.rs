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

use serde::Deserialize;
use serde::Serialize;

// Instead of store diff kind of token in diff path, we store the type in value is simpler and enough.
#[derive(
    serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, num_derive::FromPrimitive,
)]
pub enum TokenType {
    // refresh token, with a longer TTL, is only used for auth when get new refresh token and session token.
    Refresh = 1,
    // session token, is used for auth when do real work, like query, upload, etc.
    Session = 2,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct QueryTokenInfo {
    pub token_type: TokenType,
    // used to delete refresh token when close session
    pub parent: Option<String>,
}
