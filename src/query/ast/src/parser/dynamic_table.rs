// Copyright 2021 Datafuse Labs
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

use nom::Parser;
use nom_rule::rule;

use crate::ast::CreateDynamicTableStmt;
use crate::ast::Statement;
use crate::ast::TargetLag;
use crate::ast::WarehouseOptions;
use crate::parser::Input;
use crate::parser::common::IResult;
use crate::parser::common::dot_separated_idents_1_to_3;
use crate::parser::common::map_res;
use crate::parser::common::*;
use crate::parser::expr::literal_u64;
use crate::parser::query::query;
use crate::parser::statement::cluster_option;
use crate::parser::statement::create_table_source;
use crate::parser::statement::parse_create_option;
use crate::parser::statement::table_option;
use crate::parser::statement::task_warehouse_option;
use crate::parser::token::TokenKind::*;

pub fn dynamic_table(i: Input) -> IResult<Statement> {
    rule!(
        #create_dynamic_table : "`CREATE [OR REPLACE] [TRANSIENT] DYNAMIC TABLE [ IF NOT EXISTS ] [<database>.]<table> [<source>]
  [ CLUSTER BY <expr> ]
  [ TARGET_LAG = { <num> { SECOND | MINUTE | HOUR | DAY } | DOWNSTREAM} ]
  [ { WAREHOUSE = <string> } ]
  [ COMMENT = '<string_literal>' ]
AS
  <sql>`"
    ).parse(i)
}

fn create_dynamic_table(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ TRANSIENT? ~ DYNAMIC ~ TABLE ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #dot_separated_idents_1_to_3
            ~ #create_table_source?
            ~ ( CLUSTER ~ ^BY ~ ^#cluster_option )?
            ~ #dynamic_table_options
            ~ (#table_option)?
            ~ (AS ~ ^#query)
        },
        |(
            _,
            opt_or_replace,
            opt_transient,
            _,
            _,
            opt_if_not_exists,
            (catalog, database, table),
            source,
            opt_cluster_by,
            (target_lag, warehouse_opts),
            opt_table_options,
            (_, query),
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateDynamicTable(CreateDynamicTableStmt {
                create_option,
                transient: opt_transient.is_some(),
                catalog,
                database,
                table,
                source,
                cluster_by: opt_cluster_by.map(|(_, _, cluster_by)| cluster_by),
                target_lag,
                warehouse_opts,
                table_options: opt_table_options.unwrap_or_default(),
                as_query: Box::new(query),
            }))
        },
    )(i)
}

fn dynamic_table_options(i: Input) -> IResult<(TargetLag, WarehouseOptions)> {
    alt((
        |i| dynamic_table_options_with_mode(i, false),
        |i| dynamic_table_options_with_mode(i, true),
    ))
    .parse(i)
}

fn dynamic_table_options_with_mode(
    i: Input,
    manual: bool,
) -> IResult<(TargetLag, WarehouseOptions)> {
    let target_lag = move |i| {
        if manual {
            Ok((i, TargetLag::Manual))
        } else {
            map(
                rule! { TARGET_LAG ~ "=" ~ #target_lag },
                |(_, _, target_lag)| target_lag,
            )
            .parse(i)
        }
    };

    permutation((target_lag, task_warehouse_option)).parse(i)
}

fn target_lag(i: Input) -> IResult<TargetLag> {
    let interval_sec = map(
        rule! {
             #literal_u64 ~ SECOND
        },
        |(secs, _)| TargetLag::IntervalSecs(secs),
    );
    let interval_min = map(
        rule! {
             #literal_u64 ~ MINUTE
        },
        |(mins, _)| TargetLag::IntervalSecs(mins * 60),
    );
    let interval_hour = map(
        rule! {
             #literal_u64 ~ HOUR
        },
        |(hours, _)| TargetLag::IntervalSecs(hours * 60 * 60),
    );
    let interval_day = map(
        rule! {
             #literal_u64 ~ DAY
        },
        |(days, _)| TargetLag::IntervalSecs(days * 60 * 60 * 24),
    );
    let downstream = map(
        rule! {
            DOWNSTREAM
        },
        |_| TargetLag::Downstream,
    );
    rule!(
        #interval_sec
        | #interval_min
        | #interval_hour
        | #interval_day
        | #downstream
    )
    .parse(i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Dialect;
    use crate::parser::parse_sql;
    use crate::parser::tokenize_sql;

    fn parse_statement(sql: &str) -> Statement {
        let tokens = tokenize_sql(sql).unwrap();
        parse_sql(&tokens, Dialect::PostgreSQL).unwrap().0
    }

    fn parse_create(sql: &str) -> CreateDynamicTableStmt {
        let Statement::CreateDynamicTable(statement) = parse_statement(sql) else {
            panic!("expected CREATE DYNAMIC TABLE");
        };
        statement
    }

    #[test]
    fn test_dynamic_table_refresh_syntax() {
        let statement = parse_statement("REFRESH DYNAMIC TABLE db.dt");
        assert_eq!(statement.to_string(), "REFRESH DYNAMIC TABLE db.dt");
        assert!(matches!(statement, Statement::RefreshDynamicTable(_)));
    }

    #[test]
    fn test_dynamic_table_manual_refresh_syntax() {
        for sql in [
            "CREATE DYNAMIC TABLE dt AS SELECT a.id FROM a JOIN b ON a.id = b.id",
            "CREATE DYNAMIC TABLE dt AS SELECT id FROM a",
        ] {
            let statement = parse_create(sql);
            assert_eq!(statement.target_lag, TargetLag::Manual);
            let formatted = statement.to_string();
            assert!(!formatted.contains("TARGET_LAG"));
            // Refresh is always full, so the statement must not advertise a mode it cannot vary.
            assert!(!formatted.contains("REFRESH_MODE"));
            assert!(!formatted.contains("INITIALIZE"));
            let reparsed = parse_create(&formatted);
            assert_eq!(reparsed.target_lag, statement.target_lag);
            assert_eq!(reparsed.to_string(), formatted);
        }
    }

    #[test]
    fn test_dynamic_table_scheduled_syntax_is_preserved() {
        let statement =
            parse_create("CREATE DYNAMIC TABLE dt TARGET_LAG = 10 MINUTE AS SELECT id FROM a");
        assert_eq!(statement.target_lag, TargetLag::IntervalSecs(600));
        assert_eq!(
            parse_create(&statement.to_string()).to_string(),
            statement.to_string()
        );

        let statement =
            parse_create("CREATE DYNAMIC TABLE dt TARGET_LAG = DOWNSTREAM AS SELECT id FROM a");
        assert_eq!(statement.target_lag, TargetLag::Downstream);
        assert_eq!(
            parse_create(&statement.to_string()).to_string(),
            statement.to_string()
        );
    }

    /// `REFRESH_MODE` / `INITIALIZE` are no longer part of the grammar. Pin down what the parser
    /// actually does with them so the user-visible rejection is a deliberate choice, not an
    /// accident of the generic table-option rule sitting next to these keywords.
    #[test]
    fn test_removed_options_do_not_parse_as_dynamic_table_options() {
        for sql in [
            "CREATE DYNAMIC TABLE dt REFRESH_MODE = FULL AS SELECT id FROM a",
            "CREATE DYNAMIC TABLE dt INITIALIZE = ON_CREATE AS SELECT id FROM a",
        ] {
            let tokens = tokenize_sql(sql).unwrap();
            match parse_sql(&tokens, Dialect::PostgreSQL) {
                Err(_) => {}
                Ok((Statement::CreateDynamicTable(statement), _)) => {
                    // If the generic table-option rule absorbed it, it must land in table_options
                    // where the binder rejects it as a reserved key -- never silently ignored.
                    assert!(
                        !statement.table_options.is_empty(),
                        "removed option vanished silently: {sql}"
                    );
                }
                Ok((other, _)) => panic!("unexpected statement for {sql}: {other}"),
            }
        }
    }
}
