// Copyright 2021 Datafuse Labs.
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

#[cfg(test)]
mod tests {
    use common_exception::Result;

    use crate::optimizers::*;

    #[test]
    fn test_expression_transform_optimizer() -> Result<()> {
        #[allow(dead_code)]
        struct Test {
            name: &'static str,
            query: &'static str,
            expect: &'static str,
        }

        let tests: Vec<Test> = vec![
            Test {
                name: "And expression",
                query: "select number from numbers_mt(10) where not(number>1 and number<=3)",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: ((number <= 1) or (number > 3))\
                \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            },
            Test {
                name: "Complex expression",
                query: "select number from numbers_mt(10) where not(number>=5 or number<3 and toBoolean(number))",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: ((number < 5) and ((number >= 3) or (NOT toBoolean(number))))\
                \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            },
            Test {
                name: "Like and isNotNull expression",
                query: "select * from system.databases where not (isNotNull(name) and name LIKE '%sys%')",
                expect: "\
                Projection: name:String\
                \n  Filter: (isnull(name) or (name not like %sys%))\
                \n    ReadDataSource: scan partitions: [1], scan schema: [name:String], statistics: [read_rows: 0, read_bytes: 0], push_downs: []",
            },
            Test {
                name: "Not like and isNull expression",
                query: "select * from system.databases where not (name is null or name not like 'a%')",
                expect: "\
                Projection: name:String\
                \n  Filter: (isnotnull(name) and (name like a%))\
                \n    ReadDataSource: scan partitions: [1], scan schema: [name:String], statistics: [read_rows: 0, read_bytes: 0], push_downs: []",
            },
            Test {
                name: "Equal expression",
                query: "select number from numbers_mt(10) where not(number=1) and number<5",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: ((number <> 1) and (number < 5))\
                \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            },
            Test {
                name: "Not equal expression",
                query: "select number from numbers_mt(10) where not(number!=1) or number<5",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: ((number = 1) or (number < 5))\
                \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            },
            Test {
                name: "Not expression",
                query: "select number from numbers_mt(10) where not(NOT toBoolean(number))",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: toBoolean(number)\
                \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            },
            Test {
                name: "Boolean transform",
                query: "select number from numbers_mt(10) where number",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: (number != 0)\
                \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            },
            Test {
                name: "Boolean and truth transform",
                query: "select number from numbers_mt(10) where not number",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: (number = 0)\
                \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            },
            Test {
                name: "Literal boolean transform",
                query: "select number from numbers_mt(10) where false",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: false\
                \n    ReadDataSource: scan partitions: [0], scan schema: [number:UInt64], statistics: [read_rows: 0, read_bytes: 0], push_downs: []",
            },
            Test {
                name: "Limit zero transform",
                query: "select number from numbers_mt(10) where true limit 0",
                expect: "\
                Limit: 0\
                \n  Projection: number:UInt64\
                \n    Filter: true\
                \n      ReadDataSource: scan partitions: [0], scan schema: [number:UInt64], statistics: [read_rows: 0, read_bytes: 0], push_downs: []",
            },
        ];

        for test in tests {
            let ctx = crate::tests::try_create_context()?;

            let plan = crate::tests::parse_query(test.query)?;
            let mut optimizer = ExprTransformOptimizer::create(ctx);
            let optimized = optimizer.optimize(&plan)?;
            let actual = format!("{:?}", optimized);
            assert_eq!(test.expect, actual, "{:#?}", test.name);
        }
        Ok(())
    }
}
