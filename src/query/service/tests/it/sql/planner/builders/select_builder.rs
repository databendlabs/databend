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

use databend_common_sql::SelectBuilder;

struct SelectBuilderCase {
    pub columns: Vec<String>,
    pub order_bys: Vec<String>,
    pub filters: Vec<String>,
    pub expect: String,
}
type SelectBuilderCaseTuple<'a> = (Vec<&'a str>, Vec<&'a str>, Vec<&'a str>, &'a str);

impl From<SelectBuilderCaseTuple<'_>> for SelectBuilderCase {
    fn from((columns, order_bys, filters, expect): SelectBuilderCaseTuple) -> Self {
        Self {
            columns: columns.into_iter().map(|s| s.to_string()).collect(),
            order_bys: order_bys.into_iter().map(|s| s.to_string()).collect(),
            filters: filters.into_iter().map(|s| s.to_string()).collect(),
            expect: expect.to_string(),
        }
    }
}

#[test]
fn test_select_builder() {
    let cases: Vec<SelectBuilderCase> = vec![
        (vec![], vec![], vec![], "SELECT * FROM tbl   "),
        (
            vec!["col0", "col1"],
            vec![],
            vec![],
            "SELECT col0,col1 FROM tbl   ",
        ),
        (
            vec!["col0 AS Col"],
            vec![],
            vec![],
            "SELECT col0 AS Col FROM tbl   ",
        ),
        (
            vec![],
            vec!["col0"],
            vec![],
            "SELECT * FROM tbl  ORDER BY col0 ",
        ),
        (
            vec![],
            vec!["col0", "col1"],
            vec![],
            "SELECT * FROM tbl  ORDER BY col0,col1 ",
        ),
        (
            vec![],
            vec![],
            vec!["col0 = '1'"],
            "SELECT * FROM tbl where col0 = '1'  ",
        ),
        (
            vec![],
            vec![],
            vec!["col0 = '1'", "col1 = '2'"],
            "SELECT * FROM tbl where col0 = '1' and col1 = '2'  ",
        ),
        (
            vec!["col3", "col4"],
            vec!["col0", "col1", "col2"],
            vec!["col0 = '1'", "col1 = '2'"],
            "SELECT col3,col4 FROM tbl where col0 = '1' and col1 = '2' ORDER BY col0,col1,col2 ",
        ),
    ]
    .into_iter()
    .map(SelectBuilderCase::from)
    .collect();

    for SelectBuilderCase {
        columns,
        order_bys,
        filters,
        expect,
    } in cases
    {
        let mut builder = SelectBuilder::from("tbl");
        for col in columns {
            builder.with_column(col);
        }
        for order_by in order_bys {
            builder.with_order_by(&order_by);
        }
        for filter in filters {
            builder.with_filter(filter);
        }
        let query = builder.build();
        assert_eq!(query, expect);
    }
}
