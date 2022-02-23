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
use databend_query::sql::statements::DfDeleteStatement;
use databend_query::sql::*;
use sqlparser::ast::Expr::BinaryOp;
use sqlparser::ast::Value as AstValue;
use sqlparser::ast::*;

use crate::sql::sql_parser::*;

#[test]
fn delete_from() -> Result<()> {
    {
        let sql = "delete from t1";
        let expected = DfStatement::Delete(Box::new(DfDeleteStatement {
            from: ObjectName(vec![Ident::new("t1")]),
            selection: None,
        }));
        expect_parse_ok(sql, expected)?;
    }

    {
        let sql = "delete from t1 where col = 1";
        let expected = DfStatement::Delete(Box::new(DfDeleteStatement {
            from: ObjectName(vec![Ident::new("t1")]),
            selection: Some(BinaryOp {
                left: Box::new(Expr::Identifier {
                    0: Ident {
                        value: "col".to_owned(),
                        quote_style: None,
                    },
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(AstValue::Number("1".to_owned(), false))),
            }),
        }));
        expect_parse_ok(sql, expected)?;
    }

    Ok(())
}
