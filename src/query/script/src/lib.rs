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

#![allow(unreachable_code)]

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::vec;

use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Indirection;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::ScriptStatement;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::UnaryOperator;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::Span;
use derive_visitor::DriveMut;
use derive_visitor::VisitorMut;

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
struct RefAllocator {
    next_index: usize,
}

impl<const REFKIND: usize> Ref<REFKIND> {
    fn new(span: Span, name: &str, allocator: &mut RefAllocator) -> Self {
        let index = allocator.next_index;
        allocator.next_index += 1;
        Ref {
            span,
            index,
            display_name: format!("{name}"),
        }
    }

    fn new_interal(span: Span, hint: &str, allocator: &mut RefAllocator) -> Self {
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
                match expr {
                    Expr::Hole { span, name } => {
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
                    _ => {}
                }
            }

            fn enter_identifier(&mut self, ident: &mut Identifier) {
                if ident.is_hole {
                    let index = (self.lookup_var)(&ident);
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
                                ErrorCode::ScriptSematicError(format!(
                                    "variable doesn't need to be quoted in this context, try removing the colon"
                                ))
                                .set_span(*span),
                            );
                    }
                    _ => {}
                }
            }

            fn enter_identifier(&mut self, ident: &mut Identifier) {
                if ident.is_hole {
                    self.error = Some(
                        ErrorCode::ScriptSematicError(format!(
                            "variable is not allowed in this context"
                        ))
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
                match expr {
                    Expr::Hole { span, name } => {
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
                    _ => {}
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
                                ErrorCode::ScriptSematicError(format!(
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

impl Display for StatementTemplate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.stmt)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RefName(String);

impl Display for RefName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Default)]
struct Scope {
    items: HashMap<RefName, RefItem>,
    anonymous_items: Vec<RefItem>,
}

#[derive(Debug, Clone)]
enum RefItem {
    Var(VarRef),
    Set(SetRef),
    Iter(IterRef),
    Loop {
        continue_label: LabelRef,
        break_label: LabelRef,
    },
}

impl RefItem {
    pub fn is_same_kind(&self, other: &RefItem) -> bool {
        match (self, other) {
            (RefItem::Var(_), RefItem::Var(_)) => true,
            (RefItem::Set(_), RefItem::Set(_)) => true,
            (RefItem::Loop { .. }, RefItem::Loop { .. }) => true,
            _ => false,
        }
    }
}

pub fn compile(code: &[ScriptStatement]) -> Result<Vec<ScriptIR>> {
    let mut compiler = Compiler::new();
    compiler.compile(code)
}

struct Compiler {
    ref_allocator: RefAllocator,
    scopes: Vec<Scope>,
}

impl Compiler {
    pub fn new() -> Compiler {
        Compiler {
            ref_allocator: RefAllocator::default(),
            scopes: vec![Scope::default()],
        }
    }

    #[minitrace::trace]
    pub fn compile(&mut self, code: &[ScriptStatement]) -> Result<Vec<ScriptIR>> {
        let mut output = vec![];

        for line in code {
            match line {
                ScriptStatement::LetVar { declare, .. } => {
                    let to_var = self.declare_var(&declare.name)?;
                    output.append(&mut self.compile_expr(&declare.default, to_var)?);
                }
                ScriptStatement::LetStatement { span, declare } => {
                    // QUERY <stmt>, to_set
                    let stmt = self.build_sql_statement(*span, &declare.stmt)?;
                    let to_set = self.declare_set(&declare.name)?;
                    output.push(ScriptIR::Query { stmt, to_set });
                }
                ScriptStatement::RunStatement { span, stmt } => {
                    // QUERY <stmt>, unused_result
                    let stmt = self.build_sql_statement(*span, stmt)?;
                    let to_set = self.declare_anonymous_set(*span, "unused_result")?;
                    output.push(ScriptIR::Query { stmt, to_set });
                }
                ScriptStatement::Assign { name, value, .. } => {
                    let to_var = self.lookup_var(&name)?;
                    output.append(&mut self.compile_expr(value, to_var)?);
                }
                ScriptStatement::Return { value: None, .. } => {
                    output.push(ScriptIR::Return);
                }
                ScriptStatement::Return {
                    value: Some(value), ..
                } => {
                    // TODO: support returning table
                    let to_var = self.declare_anonymous_var(value.span(), "return_val")?;
                    output.append(&mut self.compile_expr(value, to_var.clone())?);
                    output.push(ScriptIR::ReturnVar { var: to_var });
                }
                ScriptStatement::ForLoop {
                    span,
                    variable,
                    is_reverse,
                    lower_bound,
                    upper_bound,
                    body,
                    label,
                } => {
                    output.append(&mut self.compile_for_loop(
                        *span,
                        variable,
                        *is_reverse,
                        lower_bound,
                        upper_bound,
                        body,
                        label,
                    )?);
                }
                ScriptStatement::ForIn {
                    span,
                    variable,
                    resultset,
                    body,
                    label,
                } => {
                    output
                        .append(&mut self.compile_for_in(*span, variable, resultset, body, label)?);
                }
                ScriptStatement::WhileLoop {
                    span,
                    condition,
                    body,
                    label,
                } => {
                    output.append(&mut self.compile_while_loop(*span, condition, body, label)?);
                }
                ScriptStatement::RepeatLoop {
                    span,
                    body,
                    until_condition,
                    label,
                } => {
                    output.append(&mut self.compile_repeat_loop(
                        *span,
                        until_condition,
                        body,
                        label,
                    )?);
                }
                ScriptStatement::Loop { span, body, label } => {
                    output.append(&mut self.compile_loop(*span, body, label)?);
                }
                ScriptStatement::Break {
                    label: Some(label), ..
                } => {
                    let (_, break_label) = self.lookup_loop(label)?;
                    output.push(ScriptIR::Goto {
                        to_label: break_label,
                    });
                }
                ScriptStatement::Break { span, label: None } => {
                    let (_, break_label) = self.current_loop(*span)?;
                    output.push(ScriptIR::Goto {
                        to_label: break_label,
                    });
                }
                ScriptStatement::Continue {
                    label: Some(label), ..
                } => {
                    let (continue_label, _) = self.lookup_loop(label)?;
                    output.push(ScriptIR::Goto {
                        to_label: continue_label,
                    });
                }
                ScriptStatement::Continue { span, label: None } => {
                    let (continue_label, _) = self.current_loop(*span)?;
                    output.push(ScriptIR::Goto {
                        to_label: continue_label,
                    });
                }
                ScriptStatement::If {
                    span,
                    conditions,
                    results,
                    else_result,
                } => {
                    output.append(&mut self.compile_if(*span, conditions, results, else_result)?);
                }
                ScriptStatement::Case {
                    span,
                    operand,
                    conditions,
                    results,
                    else_result,
                } => todo!(),
            }
        }

        Ok(output)
    }

    fn compile_expr(&mut self, expr: &Expr, to_var: VarRef) -> Result<Vec<ScriptIR>> {
        let mut output = vec![];

        let stmt = StatementTemplate::build_expr(
            expr,
            self,
            |this, hole| {
                let var = this.lookup_var(hole)?;
                Ok(var.index)
            },
            |this, iter, column| {
                // READ <iter>, <column>, to_var
                let to_var =
                    this.declare_anonymous_var(column.span, &format!("{iter}.{column}"))?;
                let iter = this.lookup_iter(iter)?;
                let column = ColumnAccess::Name(this.normalize_ident(column).0);
                output.push(ScriptIR::Read {
                    iter,
                    column,
                    to_var: to_var.clone(),
                });

                Ok(to_var)
            },
        )?;

        // QUERY 'SELECT <expr>', expr_result
        let set_ref = self.declare_anonymous_set(expr.span(), "expr_result")?;
        output.push(ScriptIR::Query {
            stmt,
            to_set: set_ref.clone(),
        });

        // ITER expr_result, expr_result_iter
        let iter_ref = self.declare_anonymous_iter(expr.span(), "expr_result_iter")?;
        output.push(ScriptIR::Iter {
            set: set_ref,
            to_iter: iter_ref.clone(),
        });

        // READ expr_result_iter, $0, to_var
        output.push(ScriptIR::Read {
            iter: iter_ref,
            column: ColumnAccess::Position(0),
            to_var,
        });

        Ok(output)
    }

    fn compile_for_loop(
        &mut self,
        span: Span,
        variable: &Identifier,
        is_reverse: bool,
        lower_bound: &Expr,
        upper_bound: &Expr,
        body: &[ScriptStatement],
        label: &Option<Identifier>,
    ) -> Result<Vec<ScriptIR>> {
        let mut output = vec![];

        self.push_scope();

        let (continue_label, break_label) = match label {
            Some(label) => self.declare_loop(label)?,
            None => self.declare_anonymous_loop(span)?,
        };

        // QUERY 'SELECT * FROM generate_series(<start>, <end>, <step>)', for_index_set
        let (start, end, step) = if is_reverse {
            (upper_bound, lower_bound, -1)
        } else {
            (lower_bound, upper_bound, 1)
        };
        let select_stmt = Statement::Query(Box::new(Query {
            span: variable.span,
            with: None,
            body: SetExpr::Select(Box::new(SelectStmt {
                span: variable.span,
                hints: None,
                distinct: false,
                select_list: vec![SelectTarget::StarColumns {
                    qualified: vec![Indirection::Star(None)],
                    column_filter: None,
                }],
                from: vec![TableReference::TableFunction {
                    span: variable.span,
                    lateral: false,
                    name: Identifier::from_name(span: variable.span, "generate_series"),
                    params: vec![start.clone(), end.clone(), Expr::Literal {
                        span: variable.span,
                        value: Literal::Decimal256 {
                            value: step.into(),
                            precision: 1,
                            scale: 0,
                        },
                    }],
                    named_params: vec![],
                    alias: None,
                }],
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
        let stmt = self.build_sql_statement(variable.span, &select_stmt)?;
        let to_set = self.declare_anonymous_set(variable.span, "for_index_set")?;
        output.push(ScriptIR::Query {
            stmt,
            to_set: to_set.clone(),
        });

        // ITER for_index_set, for_index_iter
        let iter = self.declare_anonymous_iter(variable.span, "for_index_iter")?;
        output.push(ScriptIR::Iter {
            set: to_set,
            to_iter: iter.clone(),
        });

        // Label LOOP
        output.push(ScriptIR::Label {
            label: continue_label.clone(),
        });

        // JUMP_IF_ENDED for_index_iter, LOOP_END
        output.push(ScriptIR::JumpIfEnded {
            iter: iter.clone(),
            to_label: break_label.clone(),
        });

        // READ for_index_iter, $0, variable
        let variable = self.declare_var(variable)?;
        output.push(ScriptIR::Read {
            iter: iter.clone(),
            column: ColumnAccess::Position(0),
            to_var: variable.clone(),
        });

        // <body>
        output.append(&mut self.compile(body)?);

        // NEXT for_index_iter
        output.push(ScriptIR::Next { iter: iter.clone() });

        // GOTO LOOP
        output.push(ScriptIR::Goto {
            to_label: continue_label.clone(),
        });

        // Label LOOP_END
        output.push(ScriptIR::Label { label: break_label });

        self.pop_scope();

        Ok(output)
    }

    fn compile_for_in(
        &mut self,
        span: Span,
        variable: &Identifier,
        resultset: &Identifier,
        body: &[ScriptStatement],
        label: &Option<Identifier>,
    ) -> Result<Vec<ScriptIR>> {
        let mut output = vec![];

        self.push_scope();

        let (continue_label, break_label) = match label {
            Some(label) => self.declare_loop(label)?,
            None => self.declare_anonymous_loop(span)?,
        };

        // ITER resultset, for_iter
        let set = self.lookup_set(resultset)?;
        let iter = self.declare_iter(variable.span, variable)?;
        output.push(ScriptIR::Iter {
            set,
            to_iter: iter.clone(),
        });

        // Label LOOP
        output.push(ScriptIR::Label {
            label: continue_label.clone(),
        });

        // JUMP_IF_ENDED for_iter, LOOP_END
        output.push(ScriptIR::JumpIfEnded {
            iter: iter.clone(),
            to_label: break_label.clone(),
        });

        // <body>
        output.append(&mut self.compile(body)?);

        // NEXT for_iter
        output.push(ScriptIR::Next { iter: iter.clone() });

        // GOTO LOOP
        output.push(ScriptIR::Goto {
            to_label: continue_label.clone(),
        });

        // Label LOOP_END
        output.push(ScriptIR::Label { label: break_label });

        self.pop_scope();

        Ok(output)
    }

    fn compile_while_loop(
        &mut self,
        span: Span,
        condition: &Expr,
        body: &[ScriptStatement],
        label: &Option<Identifier>,
    ) -> Result<Vec<ScriptIR>> {
        let mut output = vec![];

        self.push_scope();

        let (continue_label, break_label) = match label {
            Some(label) => self.declare_loop(label)?,
            None => self.declare_anonymous_loop(span)?,
        };

        // Label LOOP
        output.push(ScriptIR::Label {
            label: continue_label.clone(),
        });

        // <let break_condition := NOT is_true(<condition>)>
        // JUMP_IF_TRUE break_condition, LOOP_END
        let break_condition = wrap_not(wrap_is_true(condition.clone()));
        let break_condition_var =
            self.declare_anonymous_var(break_condition.span(), "break_condition")?;
        output.append(&mut self.compile_expr(&break_condition, break_condition_var.clone())?);
        output.push(ScriptIR::JumpIfTrue {
            condition: break_condition_var,
            to_label: break_label.clone(),
        });

        // <body>
        output.append(&mut self.compile(&body)?);

        // GOTO LOOP
        output.push(ScriptIR::Goto {
            to_label: continue_label.clone(),
        });

        // Label LOOP_END
        output.push(ScriptIR::Label { label: break_label });

        self.pop_scope();

        Ok(output)
    }

    fn compile_repeat_loop(
        &mut self,
        span: Span,
        until_condition: &Expr,
        body: &[ScriptStatement],
        label: &Option<Identifier>,
    ) -> Result<Vec<ScriptIR>> {
        let mut output = vec![];

        self.push_scope();

        let (continue_label, break_label) = match label {
            Some(label) => self.declare_loop(label)?,
            None => self.declare_anonymous_loop(span)?,
        };

        // Label LOOP
        output.push(ScriptIR::Label {
            label: continue_label.clone(),
        });

        // <body>
        output.append(&mut self.compile(&body)?);

        // <let break_condition := is_true(until_condition)>
        // JUMP_IF_TRUE break_condition, LOOP_END
        let break_condition = wrap_is_true(until_condition.clone());
        let break_condition_var =
            self.declare_anonymous_var(break_condition.span(), "break_condition")?;
        output.append(&mut self.compile_expr(&break_condition, break_condition_var.clone())?);
        output.push(ScriptIR::JumpIfTrue {
            condition: break_condition_var,
            to_label: break_label.clone(),
        });

        // GOTO LOOP
        output.push(ScriptIR::Goto {
            to_label: continue_label.clone(),
        });

        // Label LOOP_END
        output.push(ScriptIR::Label { label: break_label });

        self.pop_scope();

        Ok(output)
    }

    fn compile_loop(
        &mut self,
        span: Span,
        body: &[ScriptStatement],
        label: &Option<Identifier>,
    ) -> Result<Vec<ScriptIR>> {
        let mut output = vec![];

        self.push_scope();

        let (continue_label, break_label) = match label {
            Some(label) => self.declare_loop(label)?,
            None => self.declare_anonymous_loop(span)?,
        };

        // Label LOOP
        output.push(ScriptIR::Label {
            label: continue_label.clone(),
        });

        // <body>
        output.append(&mut self.compile(&body)?);

        // GOTO LOOP
        output.push(ScriptIR::Goto {
            to_label: continue_label.clone(),
        });

        // Label LOOP_END
        output.push(ScriptIR::Label { label: break_label });

        self.pop_scope();

        Ok(output)
    }

    fn compile_if(
        &mut self,
        span: Span,
        conditions: &[Expr],
        results: &Vec<Vec<ScriptStatement>>,
        else_result: &Option<Vec<ScriptStatement>>,
    ) -> Result<Vec<ScriptIR>> {
        let mut output = vec![];

        let then_labels = conditions
            .iter()
            .map(|condition| {
                LabelRef::new_interal(condition.span(), "IF_THEN", &mut self.ref_allocator)
            })
            .collect::<Vec<_>>();
        let end_label = LabelRef::new_interal(span, "IF_END", &mut self.ref_allocator);

        for (condition, then_label) in conditions.iter().zip(&then_labels) {
            // <let condition := is_true(condition)>
            // JUMP_IF_TRUE condition, IF_THEN
            let condition = wrap_is_true(condition.clone());
            let condition_var = self.declare_anonymous_var(condition.span(), "condition")?;
            output.append(&mut self.compile_expr(&condition, condition_var.clone())?);
            output.push(ScriptIR::JumpIfTrue {
                condition: condition_var,
                to_label: then_label.clone(),
            });
        }

        if let Some(else_result) = else_result {
            // <else_result>
            self.push_scope();
            output.append(&mut self.compile(&else_result)?);
            self.pop_scope();
        }

        // GOTO IF_END
        output.push(ScriptIR::Goto {
            to_label: end_label.clone(),
        });

        for (result, then_label) in results.iter().zip(&then_labels) {
            // Label IF_THEN
            output.push(ScriptIR::Label {
                label: then_label.clone(),
            });

            // <result>
            self.push_scope();
            output.append(&mut self.compile(&result)?);
            self.pop_scope();

            // GOTO IF_END
            output.push(ScriptIR::Goto {
                to_label: end_label.clone(),
            });
        }

        // Label IF_END
        output.push(ScriptIR::Label {
            label: end_label.clone(),
        });

        Ok(output)
    }

    fn build_sql_statement(&self, span: Span, stmt: &Statement) -> Result<StatementTemplate> {
        StatementTemplate::build_statement(span, stmt, |hole| {
            let var = self.lookup_var(hole)?;
            Ok(var.index)
        })
    }

    fn push_scope(&mut self) {
        self.scopes.push(Scope::default());
    }

    fn pop_scope(&mut self) {
        self.scopes.pop().unwrap();
    }

    fn normalize_ident(&self, ident: &Identifier) -> RefName {
        // todo!()
        RefName(ident.name.clone())
    }

    fn declare_ref(&mut self, ident: &Identifier, item: RefItem) -> Result<()> {
        let name = self.normalize_ident(ident);
        for scope in self.scopes.iter().rev() {
            if let Some(shadowed) = scope.items.get(&name) {
                if !shadowed.is_same_kind(&item) {
                    return Err(ErrorCode::ScriptSematicError(format!(
                        "`{name}` is already defined as a different kind of variable"
                    ))
                    .set_span(ident.span));
                }
                break;
            }
        }
        self.scopes.last_mut().unwrap().items.insert(name, item);
        Ok(())
    }

    fn declare_anonymous_ref(&mut self, item: RefItem) -> Result<()> {
        self.scopes.last_mut().unwrap().anonymous_items.push(item);
        Ok(())
    }

    fn declare_var(&mut self, ident: &Identifier) -> Result<VarRef> {
        let name = self.normalize_ident(ident);
        let var = VarRef::new(ident.span, &name.0, &mut self.ref_allocator);
        self.declare_ref(ident, RefItem::Var(var.clone()))?;
        Ok(var)
    }

    fn declare_anonymous_var(&mut self, span: Span, hint: &str) -> Result<VarRef> {
        let var = VarRef::new_interal(span, hint, &mut self.ref_allocator);
        self.declare_anonymous_ref(RefItem::Var(var.clone()))?;
        Ok(var)
    }

    fn declare_set(&mut self, ident: &Identifier) -> Result<SetRef> {
        let name = self.normalize_ident(ident);
        let set = SetRef::new(ident.span, &name.0, &mut self.ref_allocator);
        self.declare_ref(ident, RefItem::Set(set.clone()))?;
        Ok(set)
    }

    fn declare_anonymous_set(&mut self, span: Span, hint: &str) -> Result<SetRef> {
        let set = SetRef::new_interal(span, hint, &mut self.ref_allocator);
        self.declare_anonymous_ref(RefItem::Set(set.clone()))?;
        Ok(set)
    }

    fn declare_iter(&mut self, span: Span, ident: &Identifier) -> Result<IterRef> {
        let name = self.normalize_ident(ident);
        let iter = IterRef::new(span, &name.0, &mut self.ref_allocator);
        self.declare_ref(ident, RefItem::Iter(iter.clone()))?;
        Ok(iter)
    }

    fn declare_anonymous_iter(&mut self, span: Span, hint: &str) -> Result<IterRef> {
        let iter = IterRef::new_interal(span, hint, &mut self.ref_allocator);
        self.declare_anonymous_ref(RefItem::Iter(iter.clone()))?;
        Ok(iter)
    }

    fn declare_loop(&mut self, ident: &Identifier) -> Result<(LabelRef, LabelRef)> {
        let name = self.normalize_ident(ident);
        let continue_label = LabelRef::new(
            ident.span,
            &format!("{}_LOOP", &name.0),
            &mut self.ref_allocator,
        );
        let break_label = LabelRef::new(
            ident.span,
            &format!("{}_LOOP_END", &name.0),
            &mut self.ref_allocator,
        );
        self.declare_ref(ident, RefItem::Loop {
            continue_label: continue_label.clone(),
            break_label: break_label.clone(),
        })?;
        Ok((continue_label, break_label))
    }

    fn declare_anonymous_loop(&mut self, span: Span) -> Result<(LabelRef, LabelRef)> {
        let continue_label = LabelRef::new_interal(span, "LOOP", &mut self.ref_allocator);
        let break_label = LabelRef::new_interal(span, "LOOP_END", &mut self.ref_allocator);
        self.declare_anonymous_ref(RefItem::Loop {
            continue_label: continue_label.clone(),
            break_label: break_label.clone(),
        })?;
        Ok((continue_label, break_label))
    }

    fn lookup_ref(&self, ident: &Identifier) -> Result<RefItem> {
        let name = self.normalize_ident(ident);
        for scope in self.scopes.iter().rev() {
            if let Some(item) = scope.items.get(&name) {
                return Ok(item.clone());
            }
        }
        Err(ErrorCode::ScriptSematicError(format!("`{name}` is not defined")).set_span(ident.span))
    }

    fn lookup_var(&self, ident: &Identifier) -> Result<VarRef> {
        let RefItem::Var(var) = self.lookup_ref(ident)? else {
            let name = self.normalize_ident(ident);
            return Err(ErrorCode::ScriptSematicError(format!(
                "`{name}` is not a scalar variable"
            ))
            .set_span(ident.span));
        };
        Ok(var)
    }

    fn lookup_set(&self, ident: &Identifier) -> Result<SetRef> {
        let RefItem::Set(set) = self.lookup_ref(ident)? else {
            let name = self.normalize_ident(ident);
            return Err(
                ErrorCode::ScriptSematicError(format!("`{name}` is not a set"))
                    .set_span(ident.span),
            );
        };
        Ok(set)
    }

    fn lookup_iter(&self, ident: &Identifier) -> Result<IterRef> {
        let RefItem::Iter(iter) = self.lookup_ref(ident)? else {
            let name = self.normalize_ident(ident);
            return Err(
                ErrorCode::ScriptSematicError(format!("`{name}` is not a row variable"))
                    .set_span(ident.span),
            );
        };
        Ok(iter)
    }

    fn lookup_loop(&self, ident: &Identifier) -> Result<(LabelRef, LabelRef)> {
        let RefItem::Loop {
            continue_label,
            break_label,
        } = self.lookup_ref(ident)?
        else {
            let name = self.normalize_ident(ident);
            return Err(
                ErrorCode::ScriptSematicError(format!("`{name}` is not a loop"))
                    .set_span(ident.span),
            );
        };
        Ok((continue_label, break_label))
    }

    fn current_loop(&self, span: Span) -> Result<(LabelRef, LabelRef)> {
        for scope in self.scopes.iter().rev() {
            for item in scope.anonymous_items.iter().chain(scope.items.values()) {
                if let RefItem::Loop {
                    continue_label,
                    break_label,
                } = item
                {
                    return Ok((continue_label.clone(), break_label.clone()));
                }
            }
        }
        Err(ErrorCode::ScriptSematicError(format!("not in a loop")).set_span(span))
    }
}

fn wrap_not(expr: Expr) -> Expr {
    Expr::UnaryOp {
        span: expr.span(),
        op: UnaryOperator::Not,
        expr: Box::new(expr),
    }
}

fn wrap_is_true(expr: Expr) -> Expr {
    Expr::FunctionCall {
        span: expr.span(),
        func: FunctionCall {
            distinct: false,
            name: Identifier::from_name(expr.span(), "is_true"),
            args: vec![expr],
            params: vec![],
            window: None,
            lambda: None,
        },
    }
}

// /// Replace column references with holes.
// ///
// /// For example, `a + 1 = b` will be transformed to `:a + 1 = :b`.
// fn quote_expr(expr: &Expr) -> Result<Expr> {
//     #[derive(VisitorMut)]
//     #[visitor(Expr(enter), Identifier(enter))]
//     struct QuoteVisitor {
//         error: Option<ErrorCode>,
//     }

//     impl QuoteVisitor {
//         fn enter_expr(&mut self, expr: &mut Expr) {
//             match expr {
//                 Expr::ColumnRef {
//                     span,
//                     column:
//                         ColumnRef {
//                             database: None,
//                             table: None,
//                             column: ColumnID::Name(column),
//                         },
//                 } => {
//                     *expr = Expr::Hole {
//                         span: *span,
//                         name: column.name.clone(),
//                     }
//                 }
//                 Expr::Hole { span, .. } => {
//                     self.error = Some(
//                             ErrorCode::ScriptSematicError(format!(
//                                 "variable doesn't need to be quoted in this context, try removing the colon"
//                             ))
//                             .set_span(*span),
//                         );
//                 }
//                 _ => {}
//             }
//         }

//         fn enter_identifier(&mut self, ident: &mut Identifier) {
//             if ident.is_hole {
//                 self.error = Some(
//                     ErrorCode::ScriptSematicError(format!(
//                         "variable is not allowed in this context"
//                     ))
//                     .set_span(ident.span),
//                 );
//             }
//         }
//     }

//     let mut expr = expr.clone();
//     let mut visitor = QuoteVisitor { error: None };
//     expr.drive_mut(&mut visitor);

//     match visitor.error {
//         Some(e) => Err(e),
//         None => Ok(expr),
//     }
// }

// pub struct Executor {
//     pub code: Vec<ScriptLIR>,
//     pub func_ctx: FunctionContext,
//     pub variables: DataBlock,
//     pub result_sets: HashMap<usize, ()>,
//     pub label_to_pc: HashMap<usize, usize>,
//     pub return_value: ReturnValue,
//     pub pc: usize,
// }

// impl Executor {
//     fn load(code: Vec<ScriptLIR>, func_ctx: FunctionContext) -> Executor {
//         let mut max_variable_index = 0;
//         let mut labels = HashMap::new();

//         for (pc, ir) in code.iter().enumerate() {
//             match ir {
//                 ScriptLIR::Evaluate { to, .. } => {
//                     max_variable_index = max_variable_index.max(to.index);
//                 }
//                 ScriptLIR::GetResult { to, .. } => {
//                     max_variable_index = max_variable_index.max(to.index);
//                 }
//                 ScriptLIR::Label { label } => {
//                     labels.insert(label.index, pc);
//                 }
//                 _ => {}
//             }
//         }

//         Executor {
//             code,
//             func_ctx,
//             variables: vec![Scalar::Null; max_variable_index + 1],
//             result_sets: HashMap::new(),
//             label_to_pc: labels,
//             return_value: ReturnValue::None,
//             pc: 0,
//         }
//     }

//     pub fn run(mut self, code: Vec<ScriptLIR>, func_ctx: FunctionContext) -> Result<ReturnValue> {
//         let mut executor = Executor::load(code, func_ctx);
//         while self.pc < self.code.len() {
//             self.step()?;
//         }
//         Ok(self.return_value)
//     }

//     fn step(&mut self) -> Result<()> {
//         let ir = &self.code.get(self.pc).ok_or_else(|| {
//             ErrorCode::ScriptMemoryViolation(format!("pc: {} is out of range", self.pc))
//         })?;

//         // match ir {
//         //     ScriptLIR::Eval { expr, to } => {
//         //         let expr = expr.as_expr();
//         //         let mut evaluator = Evaluator::new(todo!(), &self.func_ctx, &BUILTIN_FUNCTIONS);
//         //         evaluator.run(&expr)?;
//         //     }
//         //     ScriptLIR::RunQuery { query, to } => todo!(),
//         //     ScriptLIR::DropQuery { result_set } => {
//         //         self.result_sets.remove(&result_set.index);
//         //     }
//         //     ScriptLIR::GetResult {
//         //         result_set,
//         //         row,
//         //         col,
//         //         to,
//         //     } => todo!(),
//         //     ScriptLIR::Label { label } => {}
//         //     ScriptLIR::Goto { label } => {
//         //         self.pc = self.labels.get(&label.index).ok_or_else(|| {
//         //             ErrorCode::ScriptMemoryViolation(format!("label: {} is not found", label))
//         //         })?;
//         //     }
//         //     ScriptLIR::JumpIf { condition, label } => {

//         //     }
//         //     _ => todo!(),
//         // }

//         Ok(())
//     }
// }
