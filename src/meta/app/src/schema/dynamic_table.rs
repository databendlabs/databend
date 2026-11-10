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

/// A Dynamic Table is an independent engine, deliberately not a materialized-view variant.
///
/// It reuses Fuse for physical storage, but its definition and refresh checkpoint live in its
/// own `TableMeta` options so that publishing one cannot alter materialized-view validity.
pub const DYNAMIC_TABLE_ENGINE: &str = "DYNAMIC_TABLE";

pub fn is_dynamic_table_engine(engine: &str) -> bool {
    engine.eq_ignore_ascii_case(DYNAMIC_TABLE_ENGINE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_table_engine_match_is_case_insensitive_and_exact() {
        assert!(is_dynamic_table_engine("DYNAMIC_TABLE"));
        assert!(is_dynamic_table_engine("dynamic_table"));
        assert!(!is_dynamic_table_engine("FUSE"));
        assert!(!is_dynamic_table_engine("MATERIALIZED_VIEW"));
        assert!(!is_dynamic_table_engine("DYNAMIC"));
    }
}
