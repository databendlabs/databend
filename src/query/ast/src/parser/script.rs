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

use nom::combinator::consumed;
use nom::combinator::map;
use nom_rule::rule;

use crate::ast::*;
use crate::parser::common::*;
use crate::parser::expr::*;
use crate::parser::input::Input;
use crate::parser::statement::*;
use crate::parser::token::*;

pub fn script_block(i: Input) -> IResult<ScriptBlock> {
    map(
        consumed(rule! {
            ( DECLARE ~ #semicolon_terminated_list1(declare_item) )?
            ~ BEGIN
            ~ #semicolon_terminated_list1(script_stmt)
            ~ END
            ~ ";"
        }),
        |(span, (declares, _, body, _, _))| {
            let declares = declares.map(|(_, declare)| declare).unwrap_or_default();
            ScriptBlock {
                span: transform_span(span.tokens),
                declares,
                body,
            }
        },
    )(i)
}

pub fn declare_item(i: Input) -> IResult<DeclareItem> {
    let declare_var = map(declare_var, DeclareItem::Var);
    let declare_set = map(declare_set, DeclareItem::Set);

    rule!(
        #declare_var
        | #declare_set
    )(i)
}

pub fn declare_var(i: Input) -> IResult<DeclareVar> {
    map(
        consumed(rule! {
            #ident ~ ":=" ~ ^#expr
        }),
        |(span, (name, _, default))| DeclareVar {
            span: transform_span(span.tokens),
            name,
            default,
        },
    )(i)
}

pub fn declare_set(i: Input) -> IResult<DeclareSet> {
    map(
        consumed(rule! {
            #ident ~ RESULTSET ~ ^":=" ~ ^#statement_body
        }),
        |(span, (name, _, _, stmt))| DeclareSet {
            span: transform_span(span.tokens),
            name,
            stmt,
        },
    )(i)
}

pub fn script_stmts(i: Input) -> IResult<Vec<ScriptStatement>> {
    semicolon_terminated_list1(script_stmt)(i)
}

pub fn script_stmt(i: Input) -> IResult<ScriptStatement> {
    let let_var_stmt = map(
        rule! {
            LET ~ #declare_var
        },
        |(_, declare)| ScriptStatement::LetVar { declare },
    );
    let let_stmt_stmt = map(
        rule! {
            LET ~ #declare_set
        },
        |(_, declare)| ScriptStatement::LetStatement { declare },
    );
    let run_stmt = map(
        consumed(rule! {
            #statement_body
        }),
        |(span, stmt)| ScriptStatement::RunStatement {
            span: transform_span(span.tokens),
            stmt,
        },
    );
    let assign_stmt = map(
        consumed(rule! {
            #ident ~ ":=" ~ ^#expr
        }),
        |(span, (name, _, value))| ScriptStatement::Assign {
            span: transform_span(span.tokens),
            name,
            value,
        },
    );
    let return_set_stmt = map(
        consumed(rule! {
            RETURN ~ TABLE ~ "(" ~ #ident ~ ^")"
        }),
        |(span, (_, _, _, name, _))| ScriptStatement::Return {
            span: transform_span(span.tokens),
            value: Some(ReturnItem::Set(name)),
        },
    );
    let return_stmt_stmt = map(
        consumed(rule! {
            RETURN ~ TABLE ~ "(" ~ #statement_body ~ ^")"
        }),
        |(span, (_, _, _, stmt, _))| ScriptStatement::Return {
            span: transform_span(span.tokens),
            value: Some(ReturnItem::Statement(stmt)),
        },
    );
    let return_var_stmt = map(
        consumed(rule! {
            RETURN ~ #expr
        }),
        |(span, (_, expr))| ScriptStatement::Return {
            span: transform_span(span.tokens),
            value: Some(ReturnItem::Var(expr)),
        },
    );
    let return_stmt = map(
        consumed(rule! {
            RETURN
        }),
        |(span, _)| ScriptStatement::Return {
            span: transform_span(span.tokens),
            value: None,
        },
    );
    let for_loop_stmt = map(
        consumed(rule! {
            FOR ~ ^#ident ~ ^IN ~ REVERSE?
            ~ #expr ~ TO ~ #expr ~ ^DO
            ~ ^#semicolon_terminated_list1(script_stmt)
            ~ ^END ~ ^FOR ~ #ident?
        }),
        |(
            span,
            (_, variable, _, is_reverse, lower_bound, _, upper_bound, _, body, _, _, label),
        )| ScriptStatement::ForLoop {
            span: transform_span(span.tokens),
            variable,
            is_reverse: is_reverse.is_some(),
            lower_bound,
            upper_bound,
            body,
            label,
        },
    );
    let for_in_set_stmt = map(
        consumed(rule! {
            FOR ~ ^#ident ~ ^IN ~ #ident ~ ^DO
            ~ ^#semicolon_terminated_list1(script_stmt)
            ~ ^END ~ ^FOR ~ #ident?
        }),
        |(span, (_, variable, _, resultset, _, body, _, _, label))| ScriptStatement::ForInSet {
            span: transform_span(span.tokens),
            variable,
            resultset,
            body,
            label,
        },
    );
    let for_in_stmt_stmt = map(
        consumed(rule! {
            FOR ~ ^#ident ~ ^IN ~ ^#statement_body ~ ^DO
            ~ ^#semicolon_terminated_list1(script_stmt)
            ~ ^END ~ ^FOR ~ #ident?
        }),
        |(span, (_, variable, _, stmt, _, body, _, _, label))| ScriptStatement::ForInStatement {
            span: transform_span(span.tokens),
            variable,
            stmt,
            body,
            label,
        },
    );
    let while_loop_stmt = map(
        consumed(rule! {
            WHILE ~ ^#expr ~ ^DO
            ~ ^#semicolon_terminated_list1(script_stmt)
            ~ ^END ~ ^WHILE ~ #ident?
        }),
        |(span, (_, condition, _, body, _, _, label))| ScriptStatement::WhileLoop {
            span: transform_span(span.tokens),
            condition,
            body,
            label,
        },
    );
    let repeat_loop_stmt = map(
        consumed(rule! {
            REPEAT
            ~ ^#semicolon_terminated_list1(script_stmt)
            ~ ^UNTIL ~ ^#expr
            ~ ^END ~ ^REPEAT ~ #ident?
        }),
        |(span, (_, body, _, until_condition, _, _, label))| ScriptStatement::RepeatLoop {
            span: transform_span(span.tokens),
            body,
            until_condition,
            label,
        },
    );
    let loop_stmt = map(
        consumed(rule! {
            LOOP ~ ^#semicolon_terminated_list1(script_stmt) ~ ^END ~ ^LOOP ~ #ident?
        }),
        |(span, (_, body, _, _, label))| ScriptStatement::Loop {
            span: transform_span(span.tokens),
            body,
            label,
        },
    );
    let break_stmt = map(
        consumed(rule! {
            BREAK ~ #ident?
        }),
        |(span, (_, label))| ScriptStatement::Break {
            span: transform_span(span.tokens),
            label,
        },
    );
    let continue_stmt = map(
        consumed(rule! {
            CONTINUE ~ #ident?
        }),
        |(span, (_, label))| ScriptStatement::Continue {
            span: transform_span(span.tokens),
            label,
        },
    );
    let case_stmt = map(
        consumed(rule! {
            CASE ~ #expr?
            ~ ( WHEN ~ ^#expr ~ ^THEN ~ ^#semicolon_terminated_list1(script_stmt) )+
            ~ ( ELSE ~ ^#semicolon_terminated_list1(script_stmt) )?
            ~ ^END ~ CASE?
        }),
        |(span, (_, operand, branches, else_result, _, _))| {
            let (conditions, results) = branches
                .into_iter()
                .map(|(_, cond, _, result)| (cond, result))
                .unzip();
            let else_result = else_result.map(|(_, result)| result);
            ScriptStatement::Case {
                span: transform_span(span.tokens),
                operand,
                conditions,
                results,
                else_result,
            }
        },
    );
    let if_stmt = map(
        consumed(rule! {
            IF ~ ^#expr ~ ^THEN ~ ^#semicolon_terminated_list1(script_stmt)
            ~ ( ELSEIF ~ ^#expr ~ ^THEN ~ ^#semicolon_terminated_list1(script_stmt) )*
            ~ ( ELSE ~ ^#semicolon_terminated_list1(script_stmt) )?
            ~ ^END ~ ^IF
        }),
        |(span, (_, condition, _, result, else_ifs, else_result, _, _))| {
            let (mut conditions, mut results) = (vec![condition], vec![result]);
            for (_, cond, _, result) in else_ifs {
                conditions.push(cond);
                results.push(result);
            }
            let else_result = else_result.map(|(_, result)| result);
            ScriptStatement::If {
                span: transform_span(span.tokens),
                conditions,
                results,
                else_result,
            }
        },
    );

    rule!(
        #let_stmt_stmt
        | #let_var_stmt
        | #run_stmt
        | #assign_stmt
        | #return_set_stmt
        | #return_stmt_stmt
        | #return_var_stmt
        | #return_stmt
        | #for_loop_stmt
        | #for_in_set_stmt
        | #for_in_stmt_stmt
        | #while_loop_stmt
        | #repeat_loop_stmt
        | #loop_stmt
        | #break_stmt
        | #continue_stmt
        | #case_stmt
        | #if_stmt
    )(i)
}
