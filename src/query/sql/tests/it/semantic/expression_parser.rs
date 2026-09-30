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

use databend_common_catalog::table_context::TableContextTableAccess;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::parse_to_filters;

use crate::framework::LiteTableContext;

/// Storage filters must reject both nondeterministic functions and context
/// functions that would otherwise fold into deterministic literals (#19833).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_parse_to_filters_determinism() -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.register_table_sql("CREATE TABLE filter_test(a INT, b STRING)")
        .await?;
    let table = ctx.get_table("default", "default", "filter_test").await?;

    for sql in ["a > 3", "a > 0 AND b = 'literal'"] {
        parse_to_filters(ctx.clone(), table.clone(), sql)?;
    }

    for (sql, expected_message) in [
        ("a > rand()", "is not deterministic"),
        (
            "b = current_database()",
            "`current_database()` depends on the session or query context",
        ),
        (
            "b = concat('prefix', current_database())",
            "`current_database()` depends on the session or query context",
        ),
    ] {
        let err = parse_to_filters(ctx.clone(), table.clone(), sql).unwrap_err();
        assert_eq!(err.code(), ErrorCode::SemanticError("").code(), "{sql}");
        assert!(
            err.message().contains(expected_message),
            "unexpected error for `{sql}`: {err}"
        );
    }

    Ok(())
}
