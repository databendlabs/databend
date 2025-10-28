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

use std::fmt::Display;
use std::fmt::Formatter;

use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Statement;
use crate::ast::TypeName;
use crate::Span;

const INDENT_DEPTH: usize = 4;

#[derive(Debug, Clone, PartialEq)]
pub struct ScriptBlock {
    pub span: Span,
    pub declares: Vec<DeclareItem>,
    pub body: Vec<ScriptStatement>,
}

impl Display for ScriptBlock {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        writeln!(f, "DECLARE")?;
        for declare in &self.declares {
            writeln!(
                f,
                "{}",
                indent::indent_all_by(INDENT_DEPTH, format!("{};", declare))
            )?;
        }
        writeln!(f, "BEGIN")?;
        for stmt in &self.body {
            writeln!(
                f,
                "{}",
                indent::indent_all_by(INDENT_DEPTH, format!("{};", stmt))
            )?;
        }
        writeln!(f, "END;")?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum DeclareItem {
    Var(DeclareVar),
    Set(DeclareSet),
}

impl Display for DeclareItem {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            DeclareItem::Var(declare) => write!(f, "{declare}"),
            DeclareItem::Set(declare) => write!(f, "{declare}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeclareVar {
    pub span: Span,
    pub name: Identifier,
    pub data_type: Option<TypeName>,
    pub default: Option<Expr>,
}

impl std::ops::Deref for DeclareVar {
    type Target = Option<TypeName>;

    fn deref(&self) -> &Self::Target {
        &self.data_type
    }
}
impl Display for DeclareVar {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let DeclareVar {
            name,
            data_type,
            default,
            ..
        } = self;

        write!(f, "{name}")?;
        if let Some(data_type) = data_type {
            write!(f, " {data_type}")?;
        }
        if let Some(default) = default {
            write!(f, " := {default}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeclareSet {
    pub span: Span,
    pub name: Identifier,
    pub stmt: Statement,
}

impl Display for DeclareSet {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let DeclareSet { name, stmt, .. } = self;
        write!(f, "{name} RESULTSET := {stmt}")
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum ReturnItem {
    Var(Expr),
    Set(Identifier),
    Statement(Statement),
}

impl Display for ReturnItem {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ReturnItem::Var(expr) => write!(f, "{expr}"),
            ReturnItem::Set(name) => write!(f, "TABLE({name})"),
            ReturnItem::Statement(stmt) => write!(f, "TABLE({stmt})"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeclareCursor {
    pub span: Span,
    pub name: Identifier,
    pub stmt: Option<Statement>,
    pub resultset: Option<Identifier>,
}

impl Display for DeclareCursor {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let DeclareCursor {
            name,
            stmt,
            resultset,
            ..
        } = self;
        if let Some(stmt) = stmt {
            write!(f, "{name} CURSOR FOR {stmt}")
        } else if let Some(resultset) = resultset {
            write!(f, "{name} CURSOR FOR {resultset}")
        } else {
            write!(f, "{name} CURSOR")
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum IterableItem {
    Resultset(Identifier),
    Cursor(Identifier),
}

impl Display for IterableItem {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            IterableItem::Resultset(name) => write!(f, "{name}"),
            IterableItem::Cursor(name) => write!(f, "{name}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScriptStatement {
    LetVar {
        declare: DeclareVar,
    },
    LetStatement {
        declare: DeclareSet,
    },
    LetCursor {
        declare: DeclareCursor,
    },
    RunStatement {
        span: Span,
        stmt: Statement,
    },
    Assign {
        span: Span,
        name: Identifier,
        value: Expr,
    },
    OpenCursor {
        span: Span,
        cursor: Identifier,
    },
    FetchCursor {
        span: Span,
        cursor: Identifier,
        into_var: Identifier,
    },
    CloseCursor {
        span: Span,
        cursor: Identifier,
    },
    Return {
        span: Span,
        value: Option<ReturnItem>,
    },
    ForLoop {
        span: Span,
        variable: Identifier,
        is_reverse: bool,
        lower_bound: Expr,
        upper_bound: Expr,
        body: Vec<ScriptStatement>,
        label: Option<Identifier>,
    },
    ForInSet {
        span: Span,
        variable: Identifier,
        iterable: IterableItem,
        body: Vec<ScriptStatement>,
        label: Option<Identifier>,
    },
    ForInStatement {
        span: Span,
        variable: Identifier,
        stmt: Statement,
        body: Vec<ScriptStatement>,
        label: Option<Identifier>,
    },
    WhileLoop {
        span: Span,
        condition: Expr,
        body: Vec<ScriptStatement>,
        label: Option<Identifier>,
    },
    RepeatLoop {
        span: Span,
        body: Vec<ScriptStatement>,
        until_condition: Expr,
        label: Option<Identifier>,
    },
    Loop {
        span: Span,
        body: Vec<ScriptStatement>,
        label: Option<Identifier>,
    },
    Break {
        span: Span,
        label: Option<Identifier>,
    },
    Continue {
        span: Span,
        label: Option<Identifier>,
    },
    Case {
        span: Span,
        operand: Option<Expr>,
        conditions: Vec<Expr>,
        results: Vec<Vec<ScriptStatement>>,
        else_result: Option<Vec<ScriptStatement>>,
    },
    If {
        span: Span,
        conditions: Vec<Expr>,
        results: Vec<Vec<ScriptStatement>>,
        else_result: Option<Vec<ScriptStatement>>,
    },
}

impl Display for ScriptStatement {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ScriptStatement::LetVar { declare, .. } => write!(f, "LET {declare}"),
            ScriptStatement::LetStatement { declare, .. } => write!(f, "LET {declare}"),
            ScriptStatement::LetCursor { declare, .. } => write!(f, "LET {declare}"),
            ScriptStatement::RunStatement { stmt, .. } => write!(f, "{stmt}"),
            ScriptStatement::Assign { name, value, .. } => write!(f, "{name} := {value}"),
            ScriptStatement::OpenCursor { cursor, .. } => write!(f, "OPEN {cursor}"),
            ScriptStatement::FetchCursor {
                cursor, into_var, ..
            } => write!(f, "FETCH {cursor} INTO {into_var}"),
            ScriptStatement::CloseCursor { cursor, .. } => write!(f, "CLOSE {cursor}"),
            ScriptStatement::Return { value, .. } => {
                if let Some(value) = value {
                    write!(f, "RETURN {value}")
                } else {
                    write!(f, "RETURN")
                }
            }
            ScriptStatement::ForLoop {
                variable,
                is_reverse,
                lower_bound,
                upper_bound,
                body,
                label,
                ..
            } => {
                let reverse = if *is_reverse { " REVERSE" } else { "" };
                writeln!(
                    f,
                    "FOR {variable} IN{reverse} {lower_bound} TO {upper_bound} DO"
                )?;
                for stmt in body {
                    writeln!(
                        f,
                        "{}",
                        indent::indent_all_by(INDENT_DEPTH, format!("{stmt};"))
                    )?;
                }
                write!(f, "END FOR")?;
                if let Some(label) = label {
                    write!(f, " {label}")?;
                }
                Ok(())
            }
            ScriptStatement::ForInSet {
                variable,
                iterable,
                body,
                label,
                ..
            } => {
                writeln!(f, "FOR {variable} IN {iterable} DO")?;
                for stmt in body {
                    writeln!(
                        f,
                        "{}",
                        indent::indent_all_by(INDENT_DEPTH, format!("{stmt};"))
                    )?;
                }
                write!(f, "END FOR")?;
                if let Some(label) = label {
                    write!(f, " {label}")?;
                }
                Ok(())
            }
            ScriptStatement::ForInStatement {
                variable,
                stmt,
                body,
                label,
                ..
            } => {
                writeln!(f, "FOR {variable} IN")?;
                writeln!(
                    f,
                    "{}",
                    indent::indent_all_by(INDENT_DEPTH, format!("{stmt}"))
                )?;
                writeln!(f, "DO")?;
                for stmt in body {
                    writeln!(
                        f,
                        "{}",
                        indent::indent_all_by(INDENT_DEPTH, format!("{stmt};"))
                    )?;
                }
                write!(f, "END FOR")?;
                if let Some(label) = label {
                    write!(f, " {label}")?;
                }
                Ok(())
            }
            ScriptStatement::WhileLoop {
                condition,
                body,
                label,
                ..
            } => {
                writeln!(f, "WHILE {condition} DO")?;
                for stmt in body {
                    writeln!(
                        f,
                        "{}",
                        indent::indent_all_by(INDENT_DEPTH, format!("{stmt};"))
                    )?;
                }
                write!(f, "END WHILE")?;
                if let Some(label) = label {
                    write!(f, " {label}")?;
                }
                Ok(())
            }
            ScriptStatement::RepeatLoop {
                until_condition,
                body,
                label,
                ..
            } => {
                writeln!(f, "REPEAT")?;
                for stmt in body {
                    writeln!(
                        f,
                        "{}",
                        indent::indent_all_by(INDENT_DEPTH, format!("{stmt};"))
                    )?;
                }
                writeln!(f, "UNTIL {until_condition}")?;
                write!(f, "END REPEAT")?;
                if let Some(label) = label {
                    write!(f, " {label}")?;
                }
                Ok(())
            }
            ScriptStatement::Loop { body, label, .. } => {
                writeln!(f, "LOOP")?;
                for stmt in body {
                    writeln!(
                        f,
                        "{}",
                        indent::indent_all_by(INDENT_DEPTH, format!("{stmt};"))
                    )?;
                }
                write!(f, "END LOOP")?;
                if let Some(label) = label {
                    write!(f, " {label}")?;
                }
                Ok(())
            }
            ScriptStatement::Break { label, .. } => {
                write!(f, "BREAK")?;
                if let Some(label) = label {
                    write!(f, " {label}")?;
                }
                Ok(())
            }
            ScriptStatement::Continue { label, .. } => {
                write!(f, "CONTINUE")?;
                if let Some(label) = label {
                    write!(f, " {label}")?;
                }
                Ok(())
            }
            ScriptStatement::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => {
                if let Some(operand) = operand {
                    writeln!(f, "CASE {operand}")?;
                } else {
                    writeln!(f, "CASE")?;
                }
                for (condition, result) in conditions.iter().zip(results.iter()) {
                    writeln!(f, "{:INDENT_DEPTH$}WHEN {condition} THEN", " ")?;
                    for stmt in result {
                        writeln!(
                            f,
                            "{}",
                            indent::indent_all_by(INDENT_DEPTH * 2, format!("{stmt};"))
                        )?;
                    }
                }
                if let Some(else_result) = else_result {
                    writeln!(f, "{:INDENT_DEPTH$}ELSE", " ")?;
                    for stmt in else_result {
                        writeln!(
                            f,
                            "{}",
                            indent::indent_all_by(INDENT_DEPTH * 2, format!("{stmt};"))
                        )?;
                    }
                }
                write!(f, "END CASE")
            }
            ScriptStatement::If {
                conditions,
                results,
                else_result,
                ..
            } => {
                for (i, (condition, result)) in conditions.iter().zip(results.iter()).enumerate() {
                    if i == 0 {
                        writeln!(f, "IF {condition} THEN")?;
                    } else {
                        writeln!(f, "ELSEIF {condition} THEN")?;
                    }
                    for stmt in result {
                        writeln!(
                            f,
                            "{}",
                            indent::indent_all_by(INDENT_DEPTH, format!("{stmt};"))
                        )?;
                    }
                }
                if let Some(else_result) = else_result {
                    writeln!(f, "ELSE")?;
                    for stmt in else_result {
                        writeln!(
                            f,
                            "{}",
                            indent::indent_all_by(INDENT_DEPTH, format!("{stmt};"))
                        )?;
                    }
                }
                write!(f, "END IF")
            }
        }
    }
}
