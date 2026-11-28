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

use sha2::Digest;
use sha2::Sha256;

const RESULT_CACHE_PREFIX: &str = "_result_cache";
const TRUNCATED_SQL_MAX_CHARS: usize = 128;
const TRUNCATED_SQL_HEAD_CHARS: usize = 60;
const TRUNCATED_SQL_TAIL_CHARS: usize = 63;

#[inline(always)]
pub fn gen_result_cache_key(raw: &str) -> String {
    format!("{:x}", Sha256::digest(raw))
}

#[inline(always)]
pub fn gen_result_cache_meta_key(tenant: &str, key: &str) -> String {
    format!("{RESULT_CACHE_PREFIX}/{tenant}/{key}")
}

#[inline(always)]
pub fn gen_result_cache_prefix(tenant: &str) -> String {
    format!("{RESULT_CACHE_PREFIX}/{tenant}/")
}

/// Returns true if `meta_key` is a result cache meta key of `tenant`: the
/// output of [`gen_result_cache_meta_key`] for a [`gen_result_cache_key`]
/// digest, which is 64 lowercase hex chars.
pub fn is_result_cache_meta_key(tenant: &str, meta_key: &str) -> bool {
    let Some(digest) = meta_key.strip_prefix(&gen_result_cache_prefix(tenant)) else {
        return false;
    };
    digest.len() == 64
        && digest
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

#[inline(always)]
pub(crate) fn gen_result_cache_dir(key: &str) -> String {
    format!("{RESULT_CACHE_PREFIX}/{key}")
}

/// Truncate a SQL string to at most 128 characters.
/// If the SQL is longer than 128 characters, keep the first 60 chars, then "...", then the last 63 chars.
pub fn truncate_sql(sql: &str) -> String {
    let chars = sql.chars().collect::<Vec<_>>();
    if chars.len() <= TRUNCATED_SQL_MAX_CHARS {
        sql.to_string()
    } else {
        let head = chars[..TRUNCATED_SQL_HEAD_CHARS].iter().collect::<String>();
        let tail = chars[chars.len() - TRUNCATED_SQL_TAIL_CHARS..]
            .iter()
            .collect::<String>();
        format!("{head}...{tail}")
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ResultCacheValue {
    /// Associated query id
    pub query_id: String,
    /// The query time.
    pub query_time: u64,
    /// Time-to-live of this query.
    pub ttl: u64,
    /// The size of the result cache (bytes).
    pub result_size: usize,
    /// The number of rows in the result cache.
    pub num_rows: usize,
    /// The sha256 of the partitions for each table in the query.
    pub partitions_shas: Vec<String>,
    /// The location of the result cache file.
    pub location: String,
    /// Additional factors that participated in the cache key (e.g., row access policy predicates).
    #[serde(default)]
    pub cache_key_extras: Vec<String>,
    /// The SQL of the query (truncated to 128 chars).
    pub sql: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_result_cache_meta_key() {
        let digest = gen_result_cache_key("SELECT 1");
        let meta_key = gen_result_cache_meta_key("t1", &digest);

        assert!(is_result_cache_meta_key("t1", &meta_key));

        // Another tenant's key.
        assert!(!is_result_cache_meta_key("t2", &meta_key));
        assert!(!is_result_cache_meta_key("t", &meta_key));

        // Not a result cache key.
        assert!(!is_result_cache_meta_key("t1", "__fd_database/t1/default"));
        assert!(!is_result_cache_meta_key("t1", &digest));
        assert!(!is_result_cache_meta_key("t1", ""));

        // The digest part is not a sha256 hex digest.
        let prefix = gen_result_cache_prefix("t1");
        assert!(!is_result_cache_meta_key("t1", &prefix));
        assert!(!is_result_cache_meta_key(
            "t1",
            &format!("{prefix}{}", &digest[1..])
        ));
        assert!(!is_result_cache_meta_key("t1", &format!("{meta_key}0")));
        assert!(!is_result_cache_meta_key(
            "t1",
            &format!("{prefix}{}", digest.to_uppercase())
        ));
        assert!(!is_result_cache_meta_key(
            "t1",
            &format!("{prefix}t2/{}", &digest[3..])
        ));
    }
}
