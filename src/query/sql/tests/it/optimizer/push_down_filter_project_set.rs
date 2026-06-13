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

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

async fn write_optimized_case(file: &mut impl std::io::Write, case: &SqlTestCase) -> Result<()> {
    let ctx = setup_context(case).await?;
    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;

    write_case_header(file, case)?;
    writeln!(file, "raw_plan:")?;
    writeln!(file, "{}", raw_plan.format_indent(Default::default())?)?;
    writeln!(file, "optimized_plan:")?;
    writeln!(
        file,
        "{}",
        optimized_plan.format_indent(Default::default())?
    )?;
    writeln!(file)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_project_set_filter_pushdown_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "push_down_filter_project_set.txt")?;

    let cases = [SqlTestCase {
        name: "srf_predicate_stays_above_project_set",
        description: "Predicates that still contain an SRF after alias rewriting must remain above ProjectSet while pure scalar predicates keep pushing down.",
        setup_sqls: &[PRODUCTS_TABLE],
        sql: "SELECT name, json_path_query(details, '$.features.*') AS all_features, json_path_query_first(details, '$.features.*') AS first_feature FROM products WHERE name = 'Laptop' AND first_feature = '16GB' AND all_features = '512GB'",
    }];

    for case in &cases {
        write_optimized_case(&mut file, case).await?;
    }

    Ok(())
}

const PRODUCTS_TABLE: &str = "CREATE TABLE products(name VARCHAR, details VARIANT)";
