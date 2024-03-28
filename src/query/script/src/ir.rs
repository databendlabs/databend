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

use std::fmt;
use std::fmt::Display;
use std::vec;

use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::Statement;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::Span;
use derive_visitor::DriveMut;
use derive_visitor::VisitorMut;

pub type VarRef = Ref<0>;
pub type SetRef = Ref<1>;
pub type IterRef = Ref<2>;
pub type LabelRef = Ref<3>;

#[derive(Debug, Clone)]
pub struct Ref<const REFKIND: usize> {
    pub span: Span,
    pub index: usize,
    pub display_name: String,
}

impl<const REFKIND: usize> Display for Ref<REFKIND> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.display_name, self.index)
    }
}

#[derive(Default)]
pub struct RefAllocator {
    next_index: usize,
}

impl<const REFKIND: usize> Ref<REFKIND> {
    pub fn new(span: Span, name: &str, allocator: &mut RefAllocator) -> Self {
        let index = allocator.next_index;
        allocator.next_index += 1;
        Ref {
            span,
            index,
            display_name: name.to_string(),
        }
    }

    pub fn new_interal(span: Span, hint: &str, allocator: &mut RefAllocator) -> Self {
        let index = allocator.next_index;
        allocator.next_index += 1;
        Ref {
            span,
            index,
            display_name: format!("__{hint}{index}"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ScriptIR {
    Query {
        stmt: StatementTemplate,
        to_set: SetRef,
    },
    Iter {
        set: SetRef,
        to_iter: IterRef,
    },
    Read {
        iter: IterRef,
        column: ColumnAccess,
        to_var: VarRef,
    },
    Next {
        iter: IterRef,
    },
    Label {
        label: LabelRef,
    },
    JumpIfEnded {
        iter: IterRef,
        to_label: LabelRef,
    },
    JumpIfTrue {
        condition: VarRef,
        to_label: LabelRef,
    },
    Goto {
        to_label: LabelRef,
    },
    Return,
    ReturnVar {
        var: VarRef,
    },
    ReturnSet {
        set: SetRef,
    },
}

impl Display for ScriptIR {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScriptIR::Query {
                stmt: query,
                to_set,
            } => write!(f, "QUERY {query}, {to_set}")?,
            ScriptIR::Iter { set, to_iter } => write!(f, "ITER {set}, {to_iter}")?,
            ScriptIR::Read {
                iter,
                column,
                to_var,
            } => write!(f, "READ {iter}, {column}, {to_var}")?,
            ScriptIR::Next { iter } => {
                write!(f, "NEXT {iter}")?;
            }
            ScriptIR::Label { label } => write!(f, "{label}:")?,
            ScriptIR::JumpIfEnded { iter, to_label } => {
                write!(f, "JUMP_IF_ENDED {iter}, {to_label}")?
            }
            ScriptIR::JumpIfTrue {
                condition,
                to_label,
            } => write!(f, "JUMP_IF_TRUE {condition}, {to_label}")?,
            ScriptIR::Goto { to_label } => write!(f, "GOTO {to_label}")?,
            ScriptIR::Return => write!(f, "RETURN")?,
            ScriptIR::ReturnVar { var } => write!(f, "RETURN {var}")?,
            ScriptIR::ReturnSet { set } => write!(f, "RETURN {set}")?,
        };
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ColumnAccess {
    Position(usize),
    Name(String),
}

impl Display for ColumnAccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnAccess::Position(index) => write!(f, "${}", index),
            ColumnAccess::Name(name) => write!(f, "\"{}\"", name),
        }
    }
}

impl Display for StatementTemplate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.stmt)
    }
}

#[derive(Debug, Clone)]
pub struct StatementTemplate {
    pub span: Span,
    pub stmt: Statement,
}

impl StatementTemplate {
    pub fn build_statement(
        span: Span,
        stmt: &Statement,
        lookup_var: impl Fn(&Identifier) -> Result<usize>,
    ) -> Result<Self> {
        #[derive(VisitorMut)]
        #[visitor(Expr(enter), Identifier(enter))]
        struct TemplateVisitor<'a> {
            lookup_var: &'a dyn Fn(&Identifier) -> Result<usize>,
            error: Option<ErrorCode>,
        }

        impl TemplateVisitor<'_> {
            fn enter_expr(&mut self, expr: &mut Expr) {
                if let Expr::Hole { span, name } = expr {
                    let index = (self.lookup_var)(&Identifier::from_name(*span, name.clone()));
                    match index {
                        Ok(index) => {
                            *expr = Expr::Hole {
                                span: *span,
                                name: index.to_string(),
                            };
                        }
                        Err(e) => {
                            self.error = Some(e.set_span(*span));
                        }
                    }
                }
            }

            fn enter_identifier(&mut self, ident: &mut Identifier) {
                if ident.is_hole {
                    let index = (self.lookup_var)(ident);
                    match index {
                        Ok(index) => {
                            *ident = Identifier::from_name(ident.span, index.to_string());
                            ident.is_hole = true;
                        }
                        Err(e) => {
                            self.error = Some(e.set_span(ident.span));
                        }
                    }
                }
            }
        }

        let mut stmt = stmt.clone();
        let mut visitor = TemplateVisitor {
            lookup_var: &lookup_var,
            error: None,
        };
        stmt.drive_mut(&mut visitor);

        if let Some(e) = visitor.error {
            return Err(e);
        }

        Ok(StatementTemplate { span, stmt })
    }

    pub fn build_expr<T>(
        expr: &Expr,
        common: &mut T,
        lookup_var: impl Fn(&mut T, &Identifier) -> Result<usize>,
        mut read_iter: impl FnMut(&mut T, &Identifier, &Identifier) -> Result<VarRef>,
    ) -> Result<Self> {
        #[derive(VisitorMut)]
        #[visitor(Expr(enter), Identifier(enter))]
        struct TemplateVisitor<'a, T> {
            common: &'a mut T,
            lookup_var: &'a dyn Fn(&mut T, &Identifier) -> Result<usize>,
            read_iter: &'a mut dyn FnMut(&mut T, &Identifier, &Identifier) -> Result<VarRef>,
            error: Option<ErrorCode>,
        }

        impl<T> TemplateVisitor<'_, T> {
            fn enter_expr(&mut self, expr: &mut Expr) {
                match expr {
                    Expr::ColumnRef {
                        span,
                        column:
                            ColumnRef {
                                database: None,
                                table: None,
                                column: ColumnID::Name(column),
                            },
                    } => {
                        let index = (self.lookup_var)(self.common, column);
                        match index {
                            Ok(index) => {
                                *expr = Expr::Hole {
                                    span: *span,
                                    name: index.to_string(),
                                };
                            }
                            Err(e) => {
                                self.error = Some(e.set_span(*span));
                            }
                        }
                    }
                    Expr::ColumnRef {
                        span,
                        column:
                            ColumnRef {
                                database: None,
                                table: Some(iter),
                                column: ColumnID::Name(column),
                            },
                    } => {
                        let index = (self.read_iter)(self.common, iter, column);
                        match index {
                            Ok(index) => {
                                *expr = Expr::Hole {
                                    span: *span,
                                    name: index.to_string(),
                                };
                            }
                            Err(e) => {
                                self.error = Some(e.set_span(*span));
                            }
                        }
                    }
                    Expr::Hole { span, .. } => {
                        self.error = Some(
                                ErrorCode::ScriptSemanticError("variable doesn't need to be quoted in this context, try removing the colon".to_string())
                                .set_span(*span),
                            );
                    }
                    _ => {}
                }
            }

            fn enter_identifier(&mut self, ident: &mut Identifier) {
                if ident.is_hole {
                    self.error = Some(
                        ErrorCode::ScriptSemanticError(
                            "variable is not allowed in this context".to_string(),
                        )
                        .set_span(ident.span),
                    );
                }
            }
        }

        let mut expr = expr.clone();
        let mut visitor = TemplateVisitor {
            common,
            lookup_var: &lookup_var,
            read_iter: &mut read_iter,
            error: None,
        };
        expr.drive_mut(&mut visitor);

        if let Some(e) = visitor.error {
            return Err(e);
        }

        let select_stmt = Statement::Query(Box::new(Query {
            span: expr.span(),
            with: None,
            body: SetExpr::Select(Box::new(SelectStmt {
                span: expr.span(),
                hints: None,
                distinct: false,
                select_list: vec![SelectTarget::AliasedExpr {
                    expr: Box::new(expr.clone()),
                    alias: None,
                }],
                from: vec![],
                selection: None,
                group_by: None,
                having: None,
                window_list: None,
                qualify: None,
            })),
            order_by: vec![],
            limit: vec![],
            offset: None,
            ignore_result: false,
        }));

        Ok(StatementTemplate {
            span: expr.span(),
            stmt: select_stmt,
        })
    }

    pub fn subst(&self, lookup_var: impl Fn(usize) -> Result<Literal>) -> Result<Statement> {
        #[derive(VisitorMut)]
        #[visitor(Expr(enter), Identifier(enter))]
        struct SubstVisitor<'a> {
            lookup_var: &'a dyn Fn(usize) -> Result<Literal>,
            error: Option<ErrorCode>,
        }

        impl SubstVisitor<'_> {
            fn enter_expr(&mut self, expr: &mut Expr) {
                if let Expr::Hole { span, name } = expr {
                    let index = name.parse::<usize>().unwrap();
                    let value = (self.lookup_var)(index);
                    match value {
                        Ok(value) => {
                            *expr = Expr::Literal { span: *span, value };
                        }
                        Err(e) => {
                            self.error = Some(e.set_span(*span));
                        }
                    }
                }
            }

            fn enter_identifier(&mut self, ident: &mut Identifier) {
                if ident.is_hole {
                    let index = ident.name.parse::<usize>().unwrap();
                    let value = (self.lookup_var)(index);
                    match value {
                        Ok(Literal::String(name)) => {
                            *ident = Identifier::from_name(ident.span, name);
                        }
                        Ok(value) => {
                            self.error = Some(
                                ErrorCode::ScriptSemanticError(format!(
                                    "expected string literal, got {value}"
                                ))
                                .set_span(ident.span),
                            );
                        }
                        Err(e) => {
                            self.error = Some(e.set_span(ident.span));
                        }
                    }
                }
            }
        }

        let mut stmt = self.stmt.clone();
        let mut visitor = SubstVisitor {
            lookup_var: &lookup_var,
            error: None,
        };
        stmt.drive_mut(&mut visitor);

        if let Some(e) = visitor.error {
            return Err(e);
        }

        Ok(stmt)
    }
}
