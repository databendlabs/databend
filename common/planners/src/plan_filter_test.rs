// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;

use crate::test::Test;
use crate::*;

#[test]
fn test_filter_plan() -> Result<()> {
    use pretty_assertions::assert_eq;

    let source = Test::create().generate_source_plan_for_test(10000)?;
    let plan = PlanBuilder::from(&source)
        .filter(col("number").eq(lit(1i64)))?
        .project(&[col("number")])?
        .build()?;

    let expect ="\
    Projection: number:UInt64\
    \n  Filter: (number = 1)\
    \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]";
    let actual = format!("{:?}", plan);

    assert_eq!(expect, actual);
    Ok(())
}
