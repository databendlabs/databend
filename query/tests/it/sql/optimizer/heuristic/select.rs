// Copyright 2022 Datafuse Labs.
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

use common_base::base::tokio;
use common_exception::Result;
use databend_query::sql::optimizer::DEFAULT_REWRITE_RULES;
use goldenfile::Mint;

use super::run_suites;
use super::Suite;
use crate::tests::create_query_context;

#[tokio::test]
pub async fn test_heuristic_optimizer_select() -> Result<()> {
    let mut mint = Mint::new("tests/it/sql/optimizer/heuristic/testdata/");
    let mut file = mint.new_goldenfile("select.test")?;

    let ctx = create_query_context().await?;

    let suites = vec![
        Suite {
            comment: "".to_string(),
            query: "select * from numbers(1)".to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        },
        Suite {
            comment: "".to_string(),
            query: "select * from (select * from numbers(1)) as t1 where number = 1".to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        },
        Suite {
            comment: r#"# `b = 1` can not be pushed down"#.to_string(),
            query: "select * from (select number as a, number + 1 as b from numbers(1)) as t1 where a = 1 and b = 1".to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        },
        Suite {
            comment: "".to_string(),
            query: "select * from (select number as a, number + 1 as b from numbers(1)) as t1 where a = 1".to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        },
        Suite {
            comment: "".to_string(),
            query: "select * from numbers(1) where number = pow(1, 1 + 1)".to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        },
        Suite {
            comment: "".to_string(),
            query: "select * from numbers(1) where TRUE and 1 = 1".to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        },
        Suite {
            comment: "".to_string(),
            query: "select * from numbers(1) where number = 0 and false".to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        },
        Suite {
            comment: "".to_string(),
            query: "select * from numbers(1) where number = 0 and null".to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        },
        Suite {
            comment: "# If there is only one conjunction and the value is null, then we won't rewrite it".to_string(),
            query: "select * from numbers(1) where null".to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        },
        Suite {
            comment: "".to_string(),
            query: "select a from (select number as a, number as b from numbers(1))".to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        },
        Suite {
            comment: "".to_string(),
            query: "select a from (select number as a, number+1 as b from numbers(1))".to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        },
    ];

    run_suites(ctx, &mut file, &suites).await
}
