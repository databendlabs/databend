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

#[test]
fn test_filter_alias_push_down_optimizer() -> Result<()> {
    use pretty_assertions::assert_eq;

    use crate::optimizers::*;
    use crate::sql::*;

    struct Test {
        name: &'static str,
        query: &'static str,
        expect: &'static str,
    }

    let tests = vec![
        Test {
            name:"filter-alias-push-down",
            query: "select (number+1) as c1, number as c2 from numbers_mt(10000) where (c1+c2+1)=1",
            expect:"\
            Projection: (number + 1) as c1:UInt64, number as c2:UInt64\
            \n  Expression: (number + 1) as c1:UInt64, number as c2:UInt64 (Before Projection)\
            \n    Filter: ((((number + 1) + number) + 1) = 1)\
            \n      ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]",
        },
        Test {
            name:"group-by-alias-push-down",
            query: "select max(number+1) as c1, (number%3+1) as c2 from numbers_mt(10000) group by c2",
            expect: "\
            AggregatorFinal: groupBy=[[((number % 3) + 1) as c2]], aggr=[[max([(number + 1)]) as c1, ((number % 3) + 1) as c2]]\
            \n  AggregatorPartial: groupBy=[[((number % 3) + 1) as c2]], aggr=[[max([(number + 1)]) as c1, ((number % 3) + 1) as c2]]\
            \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]",
        },
        Test {
            name:"having-alias-push-down",
            query: "select (number+1) as c1, (number%3+1) as c2 from numbers_mt(10000) having c1 > 10",
            expect: "\
            Having: (c1 > 10)\
            \n  Projection: (number + 1) as c1:UInt64, ((number % 3) + 1) as c2:UInt16\
            \n    Expression: (number + 1) as c1:UInt64, ((number % 3) + 1) as c2:UInt16 (Before Projection)\
            \n      ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10000, read_bytes: 80000]",
        },
        Test {
            name:"order-by-alias-push-down-now-work",
            query: "select (number+1) as c1, (number%3+1) as c2 from numbers_mt(10) order by c2",
            expect: "\
            Projection: (number + 1) as c1:UInt64, ((number % 3) + 1) as c2:UInt16\
            \n  Sort: c2:UInt64\
            \n    Expression: c2:UInt64 (Before OrderBy)\
            \n      Expression: (number + 1) as c1:UInt64, ((number % 3) + 1) as c2:UInt16 (Before Projection)\
            \n        ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80]",
        },
    ];

    for t in tests {
        let ctx = crate::tests::try_create_context()?;

        let plan = PlanParser::create(ctx.clone()).build_from_sql(t.query)?;

        let mut optimizer = AliasPushDownOptimizer::create(ctx);
        let optimized = optimizer.optimize(&plan)?;
        let actual = format!("{:?}", optimized);
        assert_eq!(t.expect, actual, "{:#?}", t.name);
    }

    Ok(())
}
