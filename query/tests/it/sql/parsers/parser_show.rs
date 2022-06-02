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
use databend_query::sql::statements::DfDescribeTable;
use databend_query::sql::statements::DfShowDatabases;
use databend_query::sql::statements::DfShowEngines;
use databend_query::sql::statements::DfShowFunctions;
use databend_query::sql::statements::DfShowKind;
use databend_query::sql::statements::DfShowSettings;
use databend_query::sql::statements::DfShowStages;
use databend_query::sql::statements::DfShowTabStat;
use databend_query::sql::statements::DfShowTables;
use databend_query::sql::*;
use sqlparser::ast::*;

use crate::sql::sql_parser::*;

#[test]
fn show_queries() -> Result<()> {
    let show_history = false;
    // positive case
    expect_parse_ok(
        "SHOW TABLES",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::All,
            false,
            None,
            show_history,
        )),
    )?;
    expect_parse_ok(
        "SHOW FULL TABLES",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::All,
            true,
            None,
            show_history,
        )),
    )?;
    expect_parse_ok(
        "SHOW TABLES;",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::All,
            false,
            None,
            show_history,
        )),
    )?;
    expect_parse_ok(
        "SHOW FULL TABLES;",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::All,
            true,
            None,
            show_history,
        )),
    )?;
    expect_parse_ok("SHOW SETTINGS", DfStatement::ShowSettings(DfShowSettings))?;
    expect_parse_ok(
        "SHOW TABLES LIKE 'aaa'",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::Like(Ident::with_quote('\'', "aaa")),
            false,
            None,
            false,
        )),
    )?;

    expect_parse_ok(
        "SHOW TABLES --comments should not in sql case1",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::All,
            false,
            None,
            show_history,
        )),
    )?;

    expect_parse_ok(
        "SHOW FULL TABLES --comments should not in sql case1",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::All,
            true,
            None,
            show_history,
        )),
    )?;

    expect_parse_ok(
        "SHOW TABLES LIKE 'aaa' --comments should not in sql case2",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::Like(Ident::with_quote('\'', "aaa")),
            false,
            None,
            show_history,
        )),
    )?;

    expect_parse_ok(
        "SHOW FULL TABLES LIKE 'aaa' --comments should not in sql case2",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::Like(Ident::with_quote('\'', "aaa")),
            true,
            None,
            show_history,
        )),
    )?;

    expect_parse_ok(
        "SHOW TABLES WHERE t LIKE 'aaa'",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::Where(parse_sql_to_expr("t LIKE 'aaa'")),
            false,
            None,
            show_history,
        )),
    )?;

    expect_parse_ok(
        "SHOW FULL TABLES WHERE t LIKE 'aaa'",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::Where(parse_sql_to_expr("t LIKE 'aaa'")),
            true,
            None,
            show_history,
        )),
    )?;

    expect_parse_ok(
        "SHOW TABLES LIKE 'aaa' --comments should not in sql case2",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::Like(Ident::with_quote('\'', "aaa")),
            false,
            None,
            show_history,
        )),
    )?;

    expect_parse_ok(
        "SHOW FULL TABLES LIKE 'aaa' --comments should not in sql case2",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::Like(Ident::with_quote('\'', "aaa")),
            true,
            None,
            show_history,
        )),
    )?;

    expect_parse_ok(
        "SHOW TABLES WHERE t LIKE 'aaa' AND t LIKE 'a%'",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::Where(parse_sql_to_expr("t LIKE 'aaa' AND t LIKE 'a%'")),
            false,
            None,
            show_history,
        )),
    )?;

    expect_parse_ok(
        "SHOW FULL TABLES WHERE t LIKE 'aaa' AND t LIKE 'a%'",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::Where(parse_sql_to_expr("t LIKE 'aaa' AND t LIKE 'a%'")),
            true,
            None,
            show_history,
        )),
    )?;

    Ok(())
}

#[test]
fn show_tables_test() -> Result<()> {
    let show_history = false;
    expect_parse_ok(
        "SHOW TABLES FROM `ss`",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::All,
            false,
            Some("ss".to_string()),
            show_history,
        )),
    )?;
    expect_parse_ok(
        "SHOW FULL TABLES FROM `ss`",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::All,
            true,
            Some("ss".to_string()),
            show_history,
        )),
    )?;
    expect_parse_ok(
        "SHOW TABLES IN `ss`",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::All,
            false,
            Some("ss".to_string()),
            show_history,
        )),
    )?;
    expect_parse_ok(
        "SHOW FULL TABLES IN `ss`",
        DfStatement::ShowTables(DfShowTables::create(
            DfShowKind::All,
            true,
            Some("ss".to_string()),
            show_history,
        )),
    )?;
    Ok(())
}
#[test]
fn show_functions_tests() -> Result<()> {
    // positive case
    expect_parse_ok(
        "SHOW FUNCTIONS",
        DfStatement::ShowFunctions(DfShowFunctions::create(DfShowKind::All)),
    )?;
    expect_parse_ok(
        "SHOW FUNCTIONS;",
        DfStatement::ShowFunctions(DfShowFunctions::create(DfShowKind::All)),
    )?;
    expect_parse_ok(
        "SHOW FUNCTIONS --comments should not in sql case1",
        DfStatement::ShowFunctions(DfShowFunctions::create(DfShowKind::All)),
    )?;

    expect_parse_ok(
        "SHOW FUNCTIONS LIKE 'aaa'",
        DfStatement::ShowFunctions(DfShowFunctions::create(DfShowKind::Like(
            Ident::with_quote('\'', "aaa"),
        ))),
    )?;
    expect_parse_ok(
        "SHOW FUNCTIONS LIKE 'aaa';",
        DfStatement::ShowFunctions(DfShowFunctions::create(DfShowKind::Like(
            Ident::with_quote('\'', "aaa"),
        ))),
    )?;
    expect_parse_ok(
        "SHOW FUNCTIONS LIKE 'aaa' --comments should not in sql case2",
        DfStatement::ShowFunctions(DfShowFunctions::create(DfShowKind::Like(
            Ident::with_quote('\'', "aaa"),
        ))),
    )?;

    expect_parse_ok(
        "SHOW FUNCTIONS WHERE t LIKE 'aaa'",
        DfStatement::ShowFunctions(DfShowFunctions::create(DfShowKind::Where(
            parse_sql_to_expr("t LIKE 'aaa'"),
        ))),
    )?;
    expect_parse_ok(
        "SHOW FUNCTIONS LIKE 'aaa' --comments should not in sql case2",
        DfStatement::ShowFunctions(DfShowFunctions::create(DfShowKind::Like(
            Ident::with_quote('\'', "aaa"),
        ))),
    )?;
    expect_parse_ok(
        "SHOW FUNCTIONS WHERE t LIKE 'aaa' AND t LIKE 'a%'",
        DfStatement::ShowFunctions(DfShowFunctions::create(DfShowKind::Where(
            parse_sql_to_expr("t LIKE 'aaa' AND t LIKE 'a%'"),
        ))),
    )?;

    Ok(())
}

#[test]
fn show_databases_test() -> Result<()> {
    expect_parse_ok(
        "SHOW DATABASES",
        DfStatement::ShowDatabases(DfShowDatabases::create(DfShowKind::All)),
    )?;
    expect_synonym_parse_eq("SHOW DATABASES", "SHOW SCHEMAS")?;

    expect_parse_ok(
        "SHOW DATABASES;",
        DfStatement::ShowDatabases(DfShowDatabases::create(DfShowKind::All)),
    )?;
    expect_synonym_parse_eq("SHOW DATABASES;", "SHOW SCHEMAS;")?;

    expect_parse_ok(
        "SHOW DATABASES WHERE Database = 'ss'",
        DfStatement::ShowDatabases(DfShowDatabases::create(DfShowKind::Where(
            parse_sql_to_expr("Database = 'ss'"),
        ))),
    )?;
    expect_synonym_parse_eq(
        "SHOW DATABASES WHERE Database = 'ss'",
        "SHOW SCHEMAS WHERE Database = 'ss'",
    )?;

    expect_parse_ok(
        "SHOW DATABASES WHERE Database Like 'ss%'",
        DfStatement::ShowDatabases(DfShowDatabases::create(DfShowKind::Where(
            parse_sql_to_expr("Database Like 'ss%'"),
        ))),
    )?;
    expect_synonym_parse_eq(
        "SHOW DATABASES WHERE Database Like 'ss%'",
        "SHOW SCHEMAS WHERE Database Like 'ss%'",
    )?;

    expect_parse_ok(
        "SHOW DATABASES LIKE 'ss%'",
        DfStatement::ShowDatabases(DfShowDatabases::create(DfShowKind::Like(
            Ident::with_quote('\'', "ss%"),
        ))),
    )?;
    expect_synonym_parse_eq("SHOW DATABASES LIKE 'ss%'", "SHOW SCHEMAS LIKE 'ss%'")?;

    Ok(())
}

#[test]
fn show_engines_test() -> Result<()> {
    expect_parse_ok("show engines", DfStatement::ShowEngines(DfShowEngines))?;

    expect_parse_ok("SHOW ENGINES", DfStatement::ShowEngines(DfShowEngines))?;
    Ok(())
}

#[test]
fn show_tab_stat_test() -> Result<()> {
    expect_parse_ok(
        "SHOW TABLE STATUS",
        DfStatement::ShowTabStat(DfShowTabStat::create(DfShowKind::All, None)),
    )?;

    expect_parse_ok(
        "SHOW TABLE STATUS;",
        DfStatement::ShowTabStat(DfShowTabStat::create(DfShowKind::All, None)),
    )?;
    expect_parse_ok(
        "SHOW TABLE STATUS WHERE table_schema='ss%'",
        DfStatement::ShowTabStat(DfShowTabStat::create(
            DfShowKind::Where(parse_sql_to_expr("table_schema='ss%'")),
            None,
        )),
    )?;
    expect_parse_ok(
        "SHOW TABLE STATUS WHERE table_schema Like 'ss%'",
        DfStatement::ShowTabStat(DfShowTabStat::create(
            DfShowKind::Where(parse_sql_to_expr("table_schema Like 'ss%'")),
            None,
        )),
    )?;
    expect_parse_ok(
        "SHOW TABLE STATUS Like 'ss%'",
        DfStatement::ShowTabStat(DfShowTabStat::create(
            DfShowKind::Like(Ident::with_quote('\'', "ss%")),
            None,
        )),
    )?;
    expect_parse_ok(
        "SHOW TABLE STATUS FROM `ss`",
        DfStatement::ShowTabStat(DfShowTabStat::create(
            DfShowKind::All,
            Some("ss".to_string()),
        )),
    )?;

    expect_parse_ok(
        "SHOW TABLE STATUS IN `ss`",
        DfStatement::ShowTabStat(DfShowTabStat::create(
            DfShowKind::All,
            Some("ss".to_string()),
        )),
    )?;

    Ok(())
}

#[test]
fn show_stage_test() -> Result<()> {
    expect_parse_ok(
        "SHOW STAGES",
        DfStatement::ShowStages(DfShowStages::create(DfShowKind::All)),
    )?;

    Ok(())
}

#[test]
fn show_fields_from() -> Result<()> {
    expect_parse_ok(
        "show fields from t2",
        DfStatement::DescribeTable(DfDescribeTable {
            name: ObjectName(vec![Ident::new("t2")]),
        }),
    )?;
    Ok(())
}
