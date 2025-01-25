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

use nom::branch::alt;
use nom::branch::permutation;
use nom::combinator::map;
use nom::combinator::value;
use nom_rule::rule;

use crate::ast::ClusterOption;
use crate::ast::ClusterType;
use crate::ast::CreateDynamicTableStmt;
use crate::ast::InitializeMode;
use crate::ast::RefreshMode;
use crate::ast::Statement;
use crate::ast::TargetLag;
use crate::ast::WarehouseOptions;
use crate::parser::common::comma_separated_list1;
use crate::parser::common::dot_separated_idents_1_to_3;
use crate::parser::common::map_res;
use crate::parser::common::IResult;
use crate::parser::common::*;
use crate::parser::expr::expr;
use crate::parser::expr::literal_u64;
use crate::parser::query::query;
use crate::parser::statement::cluster_type;
use crate::parser::statement::create_table_source;
use crate::parser::statement::parse_create_option;
use crate::parser::statement::table_option;
use crate::parser::statement::task_warehouse_option;
use crate::parser::token::TokenKind::*;
use crate::parser::Input;

pub fn dynamic_table(i: Input) -> IResult<Statement> {
    rule!(
        #create_dynamic_table : "`CREATE [OR REPLACE] [TRANSIENT] DYNAMIC TABLE [ IF NOT EXISTS ] [<database>.]<table> [<source>]
  [ CLUSTER BY <expr> ]
  TARGET_LAG = { <num> { SECOND | MINUTE | HOUR | DAY } | DOWNSTREAM}
  [ { WAREHOUSE = <string> } ]
  [ REFRESH_MODE = { AUTO | FULL | INCREMENTAL } ]
  [ INITIALIZE = { ON_CREATE | ON_SCHEDULE } ]
  [ COMMENT = '<string_literal>' ]
AS
  <sql>`"
    )(i)
}

fn create_dynamic_table(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ TRANSIENT? ~ DYNAMIC ~ TABLE ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #dot_separated_idents_1_to_3
            ~ #create_table_source?
            ~ ( CLUSTER ~ ^BY ~ ( #cluster_type )? ~ ^"(" ~ ^#comma_separated_list1(expr) ~ ^")" )?
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
            (target_lag, warehouse_opts, refresh_mode_opt, initialize_opt),
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
                cluster_by: opt_cluster_by.map(|(_, _, typ, _, cluster_exprs, _)| ClusterOption {
                    cluster_type: typ.unwrap_or(ClusterType::Linear),
                    cluster_exprs,
                }),
                target_lag,
                warehouse_opts,
                refresh_mode: refresh_mode_opt.unwrap_or(RefreshMode::Auto),
                initialize: initialize_opt.unwrap_or(InitializeMode::OnCreate),
                table_options: opt_table_options.unwrap_or_default(),
                as_query: Box::new(query),
            }))
        },
    )(i)
}

fn dynamic_table_options(
    i: Input,
) -> IResult<(
    TargetLag,
    WarehouseOptions,
    Option<RefreshMode>,
    Option<InitializeMode>,
)> {
    let target_lag = map(
        rule! {
            TARGET_LAG ~ "=" ~ #target_lag
        },
        |(_, _, target_lag)| target_lag,
    );

    let refresh_mode = alt((
        value(RefreshMode::Auto, rule! { AUTO }),
        value(RefreshMode::Full, rule! { FULL }),
        value(RefreshMode::Incremental, rule! { INCREMENTAL }),
    ));
    let refresh_mode_opt = map(
        rule! {
            (REFRESH_MODE ~ "=" ~ #refresh_mode)?
        },
        |v| v.map(|v| v.2),
    );

    let initialize_mode = alt((
        value(InitializeMode::OnCreate, rule! { ON_CREATE }),
        value(InitializeMode::OnSchedule, rule! { ON_SCHEDULE }),
    ));
    let initialize_opt = map(
        rule! {
            (INITIALIZE ~ "=" ~ #initialize_mode)?
        },
        |v| v.map(|v| v.2),
    );

    permutation((
        target_lag,
        task_warehouse_option,
        refresh_mode_opt,
        initialize_opt,
    ))(i)
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
    )(i)
}
