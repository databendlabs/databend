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
pub async fn test_heuristic_optimizer_join() -> Result<()> {
    let mut mint = Mint::new("tests/it/sql/optimizer/heuristic/testdata/");
    let mut file = mint.new_goldenfile("join.test")?;

    let ctx = create_query_context().await?;

    let suites = vec![
        Suite {
            comment: "# Transform cross join into inner join".to_string(),
            query: "select t.number from numbers(1) as t, numbers(1) as t1 where t.number = t1.number"
                .to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        }, Suite {
            comment: "".to_string(),
            query: "select t.number from numbers(1) as t, numbers(1) as t1 where t.number = t1.number and t.number = t1.number + 1"
                .to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        }, Suite {
            comment: "# Push predicates down through join".to_string(),
            query: "select t.number from numbers(1) as t, numbers(1) as t1 where t.number > 1 and 1 < t1.number"
                .to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        }, Suite {
            comment: "".to_string(),
            query: "select t.number from numbers(1) as t, numbers(1) as t1 where t.number + t1.number = 1"
                .to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        }, Suite {
            comment: "# Incompatible join keys, cannot push into join".to_string(),
            query: "select t.number from numbers(1) as t, numbers(1) as t1 where t.number = cast(t1.number as string)"
                .to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        }, Suite {
            comment: "# Join multiple tables".to_string(),
            query: "select t.number from numbers(1) as t, numbers(1) as t1, numbers(1) as t2 where t1.number = t2.number and t.number = 1"
                .to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        },
    ];

    run_suites(ctx, &mut file, &suites).await
}
