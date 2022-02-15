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
use common_planners::Optimization;
use databend_query::sql::statements::DfOptimizeTable;
use databend_query::sql::*;
use sqlparser::ast::*;

use crate::sql::sql_parser::*;

#[test]
fn optimize_table() -> Result<()> {
    {
        let sql = "optimize TABLE t1";
        let expected = DfStatement::OptimizeTable(DfOptimizeTable {
            name: ObjectName(vec![Ident::new("t1")]),
            operation: Optimization::PURGE,
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "OPTIMIZE tABLE t1";
        let expected = DfStatement::OptimizeTable(DfOptimizeTable {
            name: ObjectName(vec![Ident::new("t1")]),
            operation: Optimization::PURGE,
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "optimize TABLE t1 purge";
        let expected = DfStatement::OptimizeTable(DfOptimizeTable {
            name: ObjectName(vec![Ident::new("t1")]),
            operation: Optimization::PURGE,
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "optimize TABLE t1 compact";
        let expected = DfStatement::OptimizeTable(DfOptimizeTable {
            name: ObjectName(vec![Ident::new("t1")]),
            operation: Optimization::COMPACT,
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "optimize TABLE t1 all";
        let expected = DfStatement::OptimizeTable(DfOptimizeTable {
            name: ObjectName(vec![Ident::new("t1")]),
            operation: Optimization::ALL,
        });
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "optimize TABLE t1 unacceptable";
        expect_parse_err(
            sql,
            "sql parser error: Expected one of PURGE, COMPACT, ALL, found: unacceptable"
                .to_string(),
        )?;
    }

    {
        let sql = "optimize TABLE t1 (";
        expect_parse_err(
            sql,
            "sql parser error: Expected Nothing, or one of PURGE, COMPACT, ALL, found: ("
                .to_string(),
        )?;
    }

    Ok(())
}
