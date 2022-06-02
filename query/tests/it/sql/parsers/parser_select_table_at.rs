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

use common_exception::Result;
use databend_query::sql::statements::DfQueryStatement;
use databend_query::sql::*;
use sqlparser::ast::*;

use crate::sql::sql_parser::*;

#[test]
fn select_table_at() -> Result<()> {
    {
        let sql = "select * from t at(snapshot => '0de4865bba874065905625e5db6024c1');";
        let expected = DfStatement::Query(Box::new(DfQueryStatement {
            distinct: false,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![Ident {
                        value: "t".to_owned(),
                        quote_style: None,
                    }]),
                    alias: None,
                    args: vec![],
                    with_hints: vec![],
                    instant: Some(Instant::SnapshotID(
                        "0de4865bba874065905625e5db6024c1".to_owned(),
                    )),
                },
                joins: vec![],
            }],
            projection: vec![SelectItem::Wildcard],
            selection: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
            format: None,
        }));
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "select * from t at( => '0de4865bba874065905625e5db6024c1');";
        expect_parse_err(
            sql,
            "sql parser error: Expected SNAPSHOT, found: =>".to_string(),
        )?;
    }

    {
        let sql = "select * from t at( SNAPSHOT => );";
        expect_parse_err(
            sql,
            "sql parser error: Expected snapshot id, found: )".to_string(),
        )?;
    }

    Ok(())
}
