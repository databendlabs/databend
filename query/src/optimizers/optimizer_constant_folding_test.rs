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

#[cfg(test)]
mod tests {
    use common_exception::Result;

    use crate::optimizers::*;

    #[test]
    fn test_constant_folding_optimizer() -> Result<()> {
        #[allow(dead_code)]
        struct Test {
            name: &'static str,
            query: &'static str,
            expect: &'static str,
        }

        let tests: Vec<Test> = vec![
            Test {
                name: "Projection const recursion",
                query: "SELECT 1 + 2 + 3",
                expect: "\
                Projection: ((1 + 2) + 3):UInt32\
                \n  Expression: 6:UInt32 (Before Projection)\
                \n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1], push_downs: []",
            },
            Test {
                name: "Projection left non const recursion",
                query: "SELECT dummy + 1 + 2 + 3",
                expect: "\
                Projection: (((dummy + 1) + 2) + 3):UInt64\
                \n  Expression: (((dummy + 1) + 2) + 3):UInt64 (Before Projection)\
                \n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1], push_downs: []",
            },
            Test {
                name: "Projection right non const recursion",
                query: "SELECT 1 + 2 + 3 + dummy",
                expect: "\
                Projection: (((1 + 2) + 3) + dummy):UInt64\
                \n  Expression: (6 + dummy):UInt64 (Before Projection)\
                \n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1], push_downs: []",
            },
            Test {
                name: "Projection arithmetic const recursion",
                query: "SELECT 1 + 2 + 3 / 3",
                expect: "\
                Projection: ((1 + 2) + (3 / 3)):Float64\
                \n  Expression: 4:Float64 (Before Projection)\
                \n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1], push_downs: []",
            },
            Test {
                name: "Projection comparisons const recursion",
                query: "SELECT 1 + 2 + 3 > 3",
                expect: "\
                Projection: (((1 + 2) + 3) > 3):Boolean\
                \n  Expression: true:Boolean (Before Projection)\
                \n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1], push_downs: []",
            },
            Test {
                name: "Projection cast const recursion",
                query: "SELECT CAST(1 AS bigint)",
                expect: "\
                Projection: cast(1 as Int64):Int64\
                \n  Expression: 1:Int64 (Before Projection)\
                \n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1], push_downs: []",
            },
            Test {
                name: "Projection hash const recursion",
                query: "SELECT sipHash('test_string')",
                expect: "\
                Projection: sipHash('test_string'):UInt64\
                \n  Expression: 15735157695654173841:UInt64 (Before Projection)\
                \n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1], push_downs: []",
            },
            Test {
                name: "Projection logics const recursion",
                query: "SELECT 1 = 1 AND 2 > 1",
                expect: "\
                Projection: ((1 = 1) AND (2 > 1)):Boolean\
                \n  Expression: true:Boolean (Before Projection)\
                \n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1], push_downs: []",
            },
            Test {
                name: "Projection strings const recursion",
                query: "SELECT SUBSTRING('1234567890' FROM 3 FOR 3)",
                expect: "\
                Projection: substring('1234567890', 3, 3):String\
                \n  Expression: 345:String (Before Projection)\
                \n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1], push_downs: []",
            },
            Test {
                name: "Projection to type name const recursion",
                query: "SELECT toTypeName('1234567890')",
                expect: "\
                Projection: toTypeName('1234567890'):String\
                \n  Expression: String:String (Before Projection)\
                \n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1], push_downs: []",
            },
            Test {
                name: "Filter true and cond",
                query: "SELECT number from numbers(10) where true AND number > 1",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: (number > 1)\
                \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            },
            Test {
                name: "Filter cond and true",
                query: "SELECT number from numbers(10) where number > 1 AND true",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: (number > 1)\
                \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            },
            Test {
                name: "Filter false and cond",
                query: "SELECT number from numbers(10) where false AND number > 1",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: false\
                \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            },
            Test {
                name: "Filter cond and false",
                query: "SELECT number from numbers(10) where number > 1 AND false",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: false\
                \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            },
            Test {
                name: "Filter false or cond",
                query: "SELECT number from numbers(10) where false OR number > 1",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: (number > 1)\
                \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            },
            Test {
                name: "Filter cond or false",
                query: "SELECT number from numbers(10) where number > 1 OR false",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: (number > 1)\
                \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            },
            Test {
                name: "Filter true or cond",
                query: "SELECT number from numbers(10) where true OR number > 1",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: true\
                \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            },
            Test {
                name: "Filter cond or true",
                query: "SELECT number from numbers(10) where number > 1 OR true",
                expect: "\
                Projection: number:UInt64\
                \n  Filter: true\
                \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",
            },
        ];

        for test in tests {
            let ctx = crate::tests::try_create_context()?;

            let plan = crate::tests::parse_query(test.query)?;
            let mut optimizer = ConstantFoldingOptimizer::create(ctx);
            let optimized = optimizer.optimize(&plan)?;
            let actual = format!("{:?}", optimized);
            assert_eq!(test.expect, actual, "{:#?}", test.name);
        }
        Ok(())
    }
}
