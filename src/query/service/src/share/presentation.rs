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

use databend_common_expression::generate_like_pattern;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareShowEntry {
    pub created_on: String,
    pub kind: String,
    pub owner_account: String,
    pub name: String,
    pub database_name: String,
    pub to: String,
    pub owner: String,
    pub comment: String,
    pub listing_global_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareDescEntry {
    pub kind: String,
    pub name: String,
    pub shared_on: String,
}

pub(super) fn like_match(pattern: &str, value: &str) -> bool {
    generate_like_pattern(pattern.as_bytes(), value.len()).compare(value.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn show_shares_like_supports_sql_wildcards() {
        assert!(like_match("share_e2_", "share_e2e"));
        assert!(like_match("share%", "share_e2e"));
        assert!(!like_match("share_e2_", "share_e2"));
    }
}
