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

use common_planners::*;

pub fn generate_partitions(workers: u64, total: u64) -> Partitions {
    let part_size = total / workers;
    let part_remain = total % workers;

    let mut partitions = Vec::with_capacity(workers as usize);
    if part_size == 0 {
        partitions.push(Part {
            name: format!("{}-{}-{}", total, 0, total,),
            version: 0,
        })
    } else {
        for part in 0..workers {
            let part_begin = part * part_size;
            let mut part_end = (part + 1) * part_size;
            if part == (workers - 1) && part_remain > 0 {
                part_end += part_remain;
            }
            partitions.push(Part {
                name: format!("{}-{}-{}", total, part_begin, part_end,),
                version: 0,
            })
        }
    }
    partitions
}

#[cfg(test)]
mod tests {
    use common_exception::Result;

    use crate::optimizers::Optimizers;

    #[test]
    fn test_literal_false_filter() -> Result<()> {
        let query = "select * from numbers_mt(10) where 1 + 2 = 2";
        let ctx = crate::tests::try_create_context()?;

        let plan = crate::tests::parse_query(query)?;
        let mut optimizer = Optimizers::without_scatters(ctx);
        let optimized = optimizer.optimize(&plan)?;
        let actual = format!("{:?}", optimized);

        let expect = "\
        Projection: number:UInt64\
        \n  Filter: false\
        \n    ReadDataSource: scan partitions: [0], scan schema: [number:UInt64], statistics: [read_rows: 0, read_bytes: 0], push_downs: []";

        assert_eq!(actual, expect);
        Ok(())
    }

    #[test]
    fn test_skip_read_data_source() -> Result<()> {
        #[allow(dead_code)]
        struct Test {
            name: &'static str,
            query: &'static str,
            expect: &'static str,
        }

        let tests: Vec<Test> = vec![
            Test {
                name: "Filter with 'where 1 + 2 = 2' should skip the scan",
                query: "select * from numbers_mt(10) where 1 + 2 = 2",
                expect:"\
                Projection: number:UInt64\
                \n  Filter: false\
                \n    ReadDataSource: scan partitions: [0], scan schema: [number:UInt64], statistics: [read_rows: 0, read_bytes: 0], push_downs: []",
            },
            Test {
                name: "Limit with zero should skip the scan",
                query: "select * from numbers_mt(10) where true limit 0",
                expect: "\
                Limit: 0\
                \n  Projection: number:UInt64\
                \n    Filter: true\
                \n      ReadDataSource: scan partitions: [0], scan schema: [number:UInt64], statistics: [read_rows: 0, read_bytes: 0], push_downs: []",
            },
            Test {
                name: "Having with 'having 1+1=3' should skip the scan",
                query: "select avg(number) from numbers_mt(100) group by number%10 having 1+1=3",
                expect: "\
                Projection: avg(number):Float64\
                \n  Having: false\
                \n    AggregatorFinal: groupBy=[[(number % 10)]], aggr=[[avg(number)]]\
                \n      AggregatorPartial: groupBy=[[(number % 10)]], aggr=[[avg(number)]]\
                \n        Expression: (number % 10):UInt8, number:UInt64 (Before GroupBy)\
                \n          ReadDataSource: scan partitions: [0], scan schema: [number:UInt64], statistics: [read_rows: 0, read_bytes: 0], push_downs: []",
            },
        ];

        for test in tests {
            let plan = crate::tests::parse_query(test.query)?;
            let ctx = crate::tests::try_create_context()?;
            let mut optimizer = Optimizers::without_scatters(ctx);

            let optimized_plan = optimizer.optimize(&plan)?;
            let actual = format!("{:?}", optimized_plan);
            assert_eq!(test.expect, actual, "{:#?}", test.name);
        }
        Ok(())
    }
}
