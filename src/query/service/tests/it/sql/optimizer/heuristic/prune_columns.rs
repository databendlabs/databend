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
use goldenfile::Mint;

use super::run_suites;
use super::run_test;
use super::Suite;
use crate::tests::create_query_context;

#[tokio::test]
pub async fn test_heuristic_optimizer_prune_columns() -> Result<()> {
    let mut mint = Mint::new("tests/it/sql/optimizer/heuristic/testdata/");
    let mut file = mint.new_goldenfile("prune_columns.test")?;

    let (_guard, ctx) = create_query_context().await?;

    let suites = vec![
        Suite {
            comment: "# Prune unused columns from Project".to_string(),
            query: "select * from (select a from (select number as a, number + 1 as b from numbers(1)))"
                .to_string(),
            rules: vec![],
        },
        Suite {
            comment: "# Prune unused columns from Aggregate".to_string(),
            query: "select a from (select number as a, count(*) as b from numbers(1) group by a)".to_string(),
            rules: vec![],
        },
        Suite {
            comment: "# Prune unused columns for simple plan nodes (Project, Filter, Aggregate...)".to_string(),
            query: "select a from (select number as a, number b, sum(number) as c, number as d, number as e from numbers(1) group by a, b, d, e) where b > 1 order by d limit 1".to_string(),
            rules: vec![],
        },
        Suite {
            comment: "# Prune unused columns for join plan nodes (LogicalInnerJoin ...)".to_string(),
            query: "select * from (select t1.a from (select number + 1 as a, number + 1 as b, number + 1 as c, number + 1 as d from numbers(1)) as t1, (select number + 1 as a, number + 1 as b, number + 1 as c from numbers(1)) as t2 where t1.b = t2.b and t1.c = 1)".to_string(),
            rules: vec![],
        },
        Suite {
            comment: "# Prune unused columns for correlated query".to_string(),
            query: "select t1.a from (select number + 1 as a, number + 1 as b from numbers(1)) as t1 where t1.a = (select count(*) from (select t2.a, t3.a from (select number + 1 as a, number + 1 as b, number + 1 as c, number + 1 as d from numbers(1)) as t2, (select number + 1 as a, number + 1 as b, number + 1 as c from numbers(1)) as t3 where t2.b = t3.b and t2.c = 1))".to_string(),
            rules: vec![],
        },
        Suite {
            comment: "# Prune unused columns with order by".to_string(),
            query: "select name from system.functions order by example".to_string(),
            rules: vec![],
        },
        Suite {
            comment: "# Prune unused columns with cross join".to_string(),
            query: "select t.number from numbers(10) t where exists(select * from numbers(10))".to_string(),
            rules: vec![],
        },
    ];

    run_suites(ctx, &mut file, &suites, run_test).await
}
