// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_expression::generate_like_pattern;

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
