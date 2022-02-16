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
use databend_query::sql::statements::DfUseDatabase;
use databend_query::sql::statements::DfUseTenant;
use databend_query::sql::*;
use sqlparser::ast::*;

use crate::sql::sql_parser::*;

#[test]
fn sudo_command_test() -> Result<()> {
    expect_parse_ok(
        "use `tenant`",
        DfStatement::UseDatabase(DfUseDatabase {
            name: ObjectName(vec![Ident::with_quote('`', "tenant")]),
        }),
    )?;

    expect_parse_ok(
        "sudo use tenant 'xx'",
        DfStatement::UseTenant(DfUseTenant {
            name: ObjectName(vec![Ident::with_quote('\'', "xx")]),
        }),
    )?;

    expect_parse_err(
        "sudo yy",
        String::from("sql parser error: Expected Unsupported sudo command, found: yy"),
    )?;

    expect_parse_err(
        "sudo use xx",
        String::from("sql parser error: Expected Unsupported sudo command, found: xx"),
    )?;

    Ok(())
}
