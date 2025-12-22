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

use super::expr::literal_string;
use crate::ast::AlterTableAction;
use crate::ast::AlterTableStmt;
use crate::ast::Statement;
use crate::ast::statements::password_policy::AlterPasswordAction;
use crate::ast::statements::password_policy::PasswordSetOptions;
use crate::ast::*;
use crate::parser::common::IResult;
use crate::parser::common::map_res;
use crate::parser::common::*;
use crate::parser::input::Input;
use crate::parser::token::*;

pub fn comment(i: Input) -> IResult<Statement> {
    rule!(
         #comment_table: "`COMMENT [IF EXISTS] ON TABLE <table_name> IS '<string_literal>'`"
         | #comment_column: "`COMMENT [IF EXISTS] ON COLUMN <table_name>.<column_name> IS '<string_literal>'`"
         | #comment_network_policy: "`COMMENT [IF EXISTS] ON NETWORK POLICY <policy_name> IS '<string_literal>'`"
         | #comment_password_policy: "`COMMENT [IF EXISTS] ON PASSWORD POLICY <policy_name> IS '<string_literal>'`"
    ).parse(i)
}

fn comment_table(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            COMMENT ~ ( IF ~ ^EXISTS )? ~ ON ~ TABLE ~ #table_reference_only ~ IS ~ ^#literal_string
        },
        |(_, opt_if_exists, _, _, table_reference, _, new_comment)| {
            Ok(Statement::AlterTable(AlterTableStmt {
                if_exists: opt_if_exists.is_some(),
                table_reference,
                action: AlterTableAction::ModifyTableComment { new_comment },
            }))
        },
    )(i)
}

fn comment_column(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            COMMENT ~ ( IF ~ ^EXISTS )? ~ ON ~ COLUMN ~  #column_reference_only ~ IS ~ ^#literal_string
        },
        |(_, opt_if_exists, _, _, column_reference, _, comment)| {
            let table_reference = column_reference.0;
            let name = column_reference.1;
            let column_comment = ColumnComment { name, comment };
            Ok(Statement::AlterTable(AlterTableStmt {
                if_exists: opt_if_exists.is_some(),
                table_reference,
                action: AlterTableAction::ModifyColumn {
                    action: ModifyColumnAction::Comment(vec![column_comment]),
                },
            }))
        },
    )(i)
}

fn comment_password_policy(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            COMMENT ~ ( IF ~ ^EXISTS )? ~ ON ~ PASSWORD ~ POLICY ~ ^#ident ~  IS ~ ^#literal_string
        },
        |(_, opt_if_exists, _, _, _, name, _, comment)| {
            let stmt = AlterPasswordPolicyStmt {
                if_exists: opt_if_exists.is_some(),
                name: name.to_string(),
                action: AlterPasswordAction::SetOptions(PasswordSetOptions {
                    min_length: None,
                    max_length: None,
                    min_upper_case_chars: None,
                    min_lower_case_chars: None,
                    min_numeric_chars: None,
                    min_special_chars: None,
                    min_age_days: None,
                    max_age_days: None,
                    max_retries: None,
                    lockout_time_mins: None,
                    history: None,
                    comment: Some(comment),
                }),
            };
            Ok(Statement::AlterPasswordPolicy(stmt))
        },
    )(i)
}

fn comment_network_policy(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            COMMENT ~ ( IF ~ ^EXISTS )? ~ ON ~ NETWORK ~ POLICY ~ ^#ident ~  IS ~ ^#literal_string
        },
        |(_, opt_if_exists, _, _, _, name, _, comment)| {
            let stmt = AlterNetworkPolicyStmt {
                if_exists: opt_if_exists.is_some(),
                name: name.to_string(),
                allowed_ip_list: None,
                blocked_ip_list: None,
                comment: Some(comment),
            };
            Ok(Statement::AlterNetworkPolicy(stmt))
        },
    )(i)
}
