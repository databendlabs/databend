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

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::vec;

use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::IdentifierType;
use databend_common_ast::ast::Indirection;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::ReturnItem;
use databend_common_ast::ast::ScriptStatement;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::UnaryOperator;
use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use derive_visitor::DriveMut;
use derive_visitor::VisitorMut;

use crate::ir::ColumnAccess;
use crate::ir::IterRef;
use crate::ir::LabelRef;
use crate::ir::Ref;
use crate::ir::ScriptIR;
use crate::ir::SetRef;
use crate::ir::StatementTemplate;
use crate::ir::VarRef;

#[fastrace::trace]
pub fn compile(code: &[ScriptStatement]) -> Result<Vec<ScriptIR>> {
    if code.is_empty() {
        return Err(ErrorCode::ScriptSemanticError("empty script".to_string()));
    }

    let mut compiler = Compiler::new();
    let result = compiler.compile(code);

    assert!(compiler.scopes.len() == 1 || result.is_err());

    result
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

    pub fn compile(&mut self, code: &[ScriptStatement]) -> Result<Vec<ScriptIR>> {
        let mut output = vec![];

        for line in code {
            match line {
                ScriptStatement::LetVar { declare, .. } => {
                    let to_var = VarRef::new(
                        declare.name.span,
                        &declare.name.name,
                        &mut self.ref_allocator,
                    );
                    output.append(&mut self.compile_expr(&declare.default, to_var.clone())?);
                    self.declare_ref(&declare.name, RefItem::Var(to_var))?;
                }
                ScriptStatement::LetStatement { declare } => {
                    let to_set = SetRef::new(
                        declare.name.span,
                        &declare.name.name,
                        &mut self.ref_allocator,
                    );
                    output.append(&mut self.compile_sql_statement(
                        declare.span,
                        &declare.stmt,
                        to_set.clone(),
                    )?);
                    self.declare_ref(&declare.name, RefItem::Set(to_set))?;
                }
                ScriptStatement::RunStatement { span, stmt } => {
                    let to_set =
                        SetRef::new_internal(*span, "unused_result", &mut self.ref_allocator);
                    output.append(&mut self.compile_sql_statement(*span, stmt, to_set)?);
                }
                ScriptStatement::Assign { name, value, .. } => {
                    let to_var = self.lookup_var(name)?;
                    output.append(&mut self.compile_expr(value, to_var)?);
                }
                ScriptStatement::Return { value: None, .. } => {
                    output.push(ScriptIR::Return);
                }
                ScriptStatement::Return {
                    value: Some(ReturnItem::Var(expr)),
                    ..
                } => {
                    let to_var =
                        VarRef::new_internal(expr.span(), "return_val", &mut self.ref_allocator);
                    output.append(&mut self.compile_expr(expr, to_var.clone())?);
                    output.push(ScriptIR::ReturnVar { var: to_var });
                }
                ScriptStatement::Return {
                    value: Some(ReturnItem::Set(name)),
                    ..
                } => {
                    let set = self.lookup_set(name)?;
                    output.push(ScriptIR::ReturnSet { set });
                }
                ScriptStatement::Return {
                    span,
                    value: Some(ReturnItem::Statement(stmt)),
                } => {
                    let to_set = SetRef::new_internal(*span, "return_set", &mut self.ref_allocator);
                    output.append(&mut self.compile_sql_statement(*span, stmt, to_set.clone())?);
                    output.push(ScriptIR::ReturnSet { set: to_set });
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
                ScriptStatement::ForInSet {
                    span,
                    variable,
                    resultset,
                    body,
                    label,
                } => {
                    let set = self.lookup_set(resultset)?;
                    output.append(&mut self.compile_for_in(*span, variable, set, body, label)?);
                }
                ScriptStatement::ForInStatement {
                    span,
                    variable,
                    stmt,
                    body,
                    label,
                } => {
                    let to_set = SetRef::new_internal(*span, "for_in_set", &mut self.ref_allocator);
                    output.append(&mut self.compile_sql_statement(*span, stmt, to_set.clone())?);
                    output.append(&mut self.compile_for_in(*span, variable, to_set, body, label)?);
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
                    let loop_item = self.lookup_loop(label)?;
                    output.push(ScriptIR::Goto {
                        to_label: loop_item.break_label,
                    });
                }
                ScriptStatement::Break { span, label: None } => {
                    let loop_item = self.current_loop(*span)?;
                    output.push(ScriptIR::Goto {
                        to_label: loop_item.break_label,
                    });
                }
                ScriptStatement::Continue {
                    label: Some(label), ..
                } => {
                    let loop_item = self.lookup_loop(label)?;
                    output.push(ScriptIR::Goto {
                        to_label: loop_item.continue_label,
                    });
                }
                ScriptStatement::Continue { span, label: None } => {
                    let loop_item = self.current_loop(*span)?;
                    output.push(ScriptIR::Goto {
                        to_label: loop_item.continue_label,
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
                    operand: None,
                    conditions,
                    results,
                    else_result,
                } => {
                    output.append(&mut self.compile_if(*span, conditions, results, else_result)?);
                }
                ScriptStatement::Case {
                    span,
                    operand: Some(operand),
                    conditions,
                    results,
                    else_result,
                } => {
                    output.append(&mut self.compile_case(
                        *span,
                        operand,
                        conditions,
                        results,
                        else_result,
                    )?);
                }
            }
        }

        Ok(output)
    }

    fn compile_expr(&mut self, expr: &Expr, to_var: VarRef) -> Result<Vec<ScriptIR>> {
        let mut output = vec![];

        let (mut lines, expr) = self.quote_expr(expr)?;
        output.append(&mut lines);

        // QUERY 'SELECT <expr>', expr_result
        let select_stmt = Statement::Query(Box::new(Query {
            span: expr.span(),
            with: None,
            body: SetExpr::Select(Box::new(SelectStmt {
                span: expr.span(),
                hints: None,
                distinct: false,
                top_n: None,
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
        let stmt = StatementTemplate::new(expr.whole_span(), select_stmt);
        let set_ref = SetRef::new_internal(expr.span(), "expr_result", &mut self.ref_allocator);
        output.push(ScriptIR::Query {
            stmt,
            to_set: set_ref.clone(),
        });

        // ITER expr_result, expr_result_iter
        let iter_ref =
            IterRef::new_internal(expr.span(), "expr_result_iter", &mut self.ref_allocator);
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

        let loop_item = match label {
            Some(label) => self.declare_loop(label)?,
            None => self.declare_anonymous_loop(span)?,
        };

        // QUERY 'SELECT * FROM generate_series(<start>, <end>, <step>)', for_index_set
        let (mut lines, lower_bound) = self.quote_expr(lower_bound)?;
        output.append(&mut lines);
        let (mut lines, upper_bound) = self.quote_expr(upper_bound)?;
        output.append(&mut lines);
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
                top_n: None,
                select_list: vec![SelectTarget::StarColumns {
                    qualified: vec![Indirection::Star(None)],
                    column_filter: None,
                }],
                from: vec![TableReference::TableFunction {
                    span: variable.span,
                    lateral: false,
                    name: Identifier::from_name(variable.span, "generate_series"),
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
                    sample: None,
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
        let stmt = StatementTemplate::new(variable.span, select_stmt);
        let to_set = SetRef::new_internal(variable.span, "for_index_set", &mut self.ref_allocator);
        output.push(ScriptIR::Query {
            stmt,
            to_set: to_set.clone(),
        });

        // ITER for_index_set, for_index_iter
        let iter = IterRef::new_internal(variable.span, "for_index_iter", &mut self.ref_allocator);
        output.push(ScriptIR::Iter {
            set: to_set,
            to_iter: iter.clone(),
        });

        // Label LOOP
        output.push(ScriptIR::Label {
            label: loop_item.continue_label.clone(),
        });

        // JUMP_IF_ENDED for_index_iter, LOOP_END
        output.push(ScriptIR::JumpIfEnded {
            iter: iter.clone(),
            to_label: loop_item.break_label.clone(),
        });

        // READ for_index_iter, $0, to_var
        let to_var = VarRef::new(variable.span, &variable.name, &mut self.ref_allocator);
        self.declare_ref(variable, RefItem::Var(to_var.clone()))?;
        output.push(ScriptIR::Read {
            iter: iter.clone(),
            column: ColumnAccess::Position(0),
            to_var,
        });

        // <body>
        output.append(&mut self.compile(body)?);

        // NEXT for_index_iter
        output.push(ScriptIR::Next { iter: iter.clone() });

        // GOTO LOOP
        output.push(ScriptIR::Goto {
            to_label: loop_item.continue_label.clone(),
        });

        // Label LOOP_END
        output.push(ScriptIR::Label {
            label: loop_item.break_label,
        });

        self.pop_scope();

        Ok(output)
    }

    fn compile_for_in(
        &mut self,
        span: Span,
        variable: &Identifier,
        set: SetRef,
        body: &[ScriptStatement],
        label: &Option<Identifier>,
    ) -> Result<Vec<ScriptIR>> {
        let mut output = vec![];

        self.push_scope();

        let loop_item = match label {
            Some(label) => self.declare_loop(label)?,
            None => self.declare_anonymous_loop(span)?,
        };

        // ITER resultset, iter
        let iter = IterRef::new(variable.span, &variable.name, &mut self.ref_allocator);
        self.declare_ref(variable, RefItem::Iter(iter.clone()))?;
        output.push(ScriptIR::Iter {
            set,
            to_iter: iter.clone(),
        });

        // Label LOOP
        output.push(ScriptIR::Label {
            label: loop_item.continue_label.clone(),
        });

        // JUMP_IF_ENDED iter, LOOP_END
        output.push(ScriptIR::JumpIfEnded {
            iter: iter.clone(),
            to_label: loop_item.break_label.clone(),
        });

        // <body>
        output.append(&mut self.compile(body)?);

        // NEXT iter
        output.push(ScriptIR::Next { iter: iter.clone() });

        // GOTO LOOP
        output.push(ScriptIR::Goto {
            to_label: loop_item.continue_label.clone(),
        });

        // Label LOOP_END
        output.push(ScriptIR::Label {
            label: loop_item.break_label,
        });

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

        let loop_item = match label {
            Some(label) => self.declare_loop(label)?,
            None => self.declare_anonymous_loop(span)?,
        };

        // Label LOOP
        output.push(ScriptIR::Label {
            label: loop_item.continue_label.clone(),
        });

        // <let break_condition := NOT is_true(<condition>)>
        // JUMP_IF_TRUE break_condition, LOOP_END
        let break_condition = wrap_not(wrap_is_true(condition.clone()));
        let break_condition_var = VarRef::new_internal(
            break_condition.span(),
            "break_condition",
            &mut self.ref_allocator,
        );
        output.append(&mut self.compile_expr(&break_condition, break_condition_var.clone())?);
        output.push(ScriptIR::JumpIfTrue {
            condition: break_condition_var,
            to_label: loop_item.break_label.clone(),
        });

        // <body>
        output.append(&mut self.compile(body)?);

        // GOTO LOOP
        output.push(ScriptIR::Goto {
            to_label: loop_item.continue_label.clone(),
        });

        // Label LOOP_END
        output.push(ScriptIR::Label {
            label: loop_item.break_label,
        });

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

        let loop_item = match label {
            Some(label) => self.declare_loop(label)?,
            None => self.declare_anonymous_loop(span)?,
        };

        // Label LOOP
        output.push(ScriptIR::Label {
            label: loop_item.continue_label.clone(),
        });

        // <body>
        output.append(&mut self.compile(body)?);

        // <let break_condition := is_true(until_condition)>
        // JUMP_IF_TRUE break_condition, LOOP_END
        let break_condition = wrap_is_true(until_condition.clone());
        let break_condition_var = VarRef::new_internal(
            break_condition.span(),
            "break_condition",
            &mut self.ref_allocator,
        );
        output.append(&mut self.compile_expr(&break_condition, break_condition_var.clone())?);
        output.push(ScriptIR::JumpIfTrue {
            condition: break_condition_var,
            to_label: loop_item.break_label.clone(),
        });

        // GOTO LOOP
        output.push(ScriptIR::Goto {
            to_label: loop_item.continue_label.clone(),
        });

        // Label LOOP_END
        output.push(ScriptIR::Label {
            label: loop_item.break_label,
        });

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

        let loop_item = match label {
            Some(label) => self.declare_loop(label)?,
            None => self.declare_anonymous_loop(span)?,
        };

        // Label LOOP
        output.push(ScriptIR::Label {
            label: loop_item.continue_label.clone(),
        });

        // <body>
        output.append(&mut self.compile(body)?);

        // GOTO LOOP
        output.push(ScriptIR::Goto {
            to_label: loop_item.continue_label.clone(),
        });

        // Label LOOP_END
        output.push(ScriptIR::Label {
            label: loop_item.break_label,
        });

        self.pop_scope();

        Ok(output)
    }

    fn compile_if(
        &mut self,
        span: Span,
        conditions: &[Expr],
        results: &[Vec<ScriptStatement>],
        else_result: &Option<Vec<ScriptStatement>>,
    ) -> Result<Vec<ScriptIR>> {
        let mut output = vec![];

        self.push_scope();

        let then_labels = conditions
            .iter()
            .map(|condition| {
                LabelRef::new_internal(condition.span(), "IF_THEN", &mut self.ref_allocator)
            })
            .collect::<Vec<_>>();
        let end_label = LabelRef::new_internal(span, "IF_END", &mut self.ref_allocator);

        for (condition, then_label) in conditions.iter().zip(&then_labels) {
            // <let condition := is_true(condition)>
            // JUMP_IF_TRUE condition, IF_THEN
            let condition = wrap_is_true(condition.clone());
            let condition_var =
                VarRef::new_internal(condition.span(), "condition", &mut self.ref_allocator);
            output.append(&mut self.compile_expr(&condition, condition_var.clone())?);
            output.push(ScriptIR::JumpIfTrue {
                condition: condition_var,
                to_label: then_label.clone(),
            });
        }

        if let Some(else_result) = else_result {
            // <else_result>
            self.push_scope();
            output.append(&mut self.compile(else_result)?);
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
            output.append(&mut self.compile(result)?);
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

        self.pop_scope();

        Ok(output)
    }

    fn compile_case(
        &mut self,
        span: Span,
        oprand: &Expr,
        conditions: &[Expr],
        results: &[Vec<ScriptStatement>],
        else_result: &Option<Vec<ScriptStatement>>,
    ) -> Result<Vec<ScriptIR>> {
        let conditions = conditions
            .iter()
            .map(|condition| wrap_eq(condition.span(), oprand.clone(), condition.clone()))
            .collect::<Vec<_>>();
        self.compile_if(span, &conditions, results, else_result)
    }

    fn compile_sql_statement(
        &self,
        span: Span,
        stmt: &Statement,
        to_set: SetRef,
    ) -> Result<Vec<ScriptIR>> {
        #[derive(VisitorMut)]
        #[visitor(Expr(enter), Identifier(enter), Statement(enter))]
        struct QuoteVisitor<'a> {
            compiler: &'a Compiler,
            error: Option<ErrorCode>,
        }

        impl QuoteVisitor<'_> {
            fn enter_expr(&mut self, expr: &mut Expr) {
                if let Expr::Hole { span, name } = expr {
                    let index = self
                        .compiler
                        .lookup_var(&Identifier::from_name(*span, name.clone()));
                    match index {
                        Ok(index) => {
                            *expr = Expr::Hole {
                                span: *span,
                                name: index.index.to_string(),
                            };
                        }
                        Err(e) => {
                            self.error = Some(e.set_span(*span));
                        }
                    }
                }
            }

            fn enter_identifier(&mut self, ident: &mut Identifier) {
                if ident.is_hole() {
                    let index = self.compiler.lookup_var(ident);
                    match index {
                        Ok(index) => {
                            *ident = Identifier::from_name(ident.span, index.to_string());
                            ident.ident_type = IdentifierType::Hole;
                        }
                        Err(e) => {
                            self.error = Some(e.set_span(ident.span));
                        }
                    }
                }
            }

            // TODO(andylokandy: handle these statement)
            fn enter_statement(&mut self, stmt: &mut Statement) {
                match stmt {
                    Statement::Begin => {
                        self.error = Some(ErrorCode::Unimplemented(
                            "BEGIN in script is not supported yet".to_string(),
                        ));
                    }
                    Statement::Commit => {
                        self.error = Some(ErrorCode::Unimplemented(
                            "COMMIT in script is not supported yet".to_string(),
                        ));
                    }
                    Statement::Abort => {
                        self.error = Some(ErrorCode::Unimplemented(
                            "ABORT in script is not supported yet".to_string(),
                        ));
                    }
                    Statement::Call { .. } => {
                        self.error = Some(ErrorCode::Unimplemented(
                            "CALL in script is not supported yet".to_string(),
                        ));
                    }
                    _ => (),
                }
            }
        }

        let mut stmt = stmt.clone();
        let mut visitor = QuoteVisitor {
            compiler: self,
            error: None,
        };
        stmt.drive_mut(&mut visitor);

        if let Some(e) = visitor.error {
            return Err(e);
        }

        // QUERY <stmt>, to_set
        let stmt = StatementTemplate::new(span, stmt);
        let output = vec![ScriptIR::Query { stmt, to_set }];

        Ok(output)
    }

    fn push_scope(&mut self) {
        self.scopes.push(Scope::default());
    }

    fn pop_scope(&mut self) {
        self.scopes.pop().unwrap();
    }

    fn normalize_ident(&self, ident: &Identifier) -> RefName {
        // TODO(andylokandy): use NameResolutionCtx
        RefName(ident.name.clone())
    }

    fn declare_ref(&mut self, ident: &Identifier, item: RefItem) -> Result<()> {
        let name = self.normalize_ident(ident);
        self.scopes.last_mut().unwrap().items.insert(name, item);
        Ok(())
    }

    fn declare_loop(&mut self, ident: &Identifier) -> Result<LoopItem> {
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
        let loop_item = LoopItem {
            name: Some(name.clone()),
            continue_label: continue_label.clone(),
            break_label: break_label.clone(),
        };
        self.scopes.last_mut().unwrap().loop_item = Some(loop_item.clone());
        Ok(loop_item)
    }

    fn declare_anonymous_loop(&mut self, span: Span) -> Result<LoopItem> {
        let continue_label = LabelRef::new_internal(span, "LOOP", &mut self.ref_allocator);
        let break_label = LabelRef::new_internal(span, "LOOP_END", &mut self.ref_allocator);
        let loop_item = LoopItem {
            name: None,
            continue_label: continue_label.clone(),
            break_label: break_label.clone(),
        };
        self.scopes.last_mut().unwrap().loop_item = Some(loop_item.clone());
        Ok(loop_item)
    }

    fn lookup_ref(&self, ident: &Identifier) -> Result<RefItem> {
        let name = self.normalize_ident(ident);
        for scope in self.scopes.iter().rev() {
            if let Some(item) = scope.items.get(&name) {
                return Ok(item.clone());
            }
        }
        Err(ErrorCode::ScriptSemanticError(format!("`{name}` is not defined")).set_span(ident.span))
    }

    fn lookup_var(&self, ident: &Identifier) -> Result<VarRef> {
        let RefItem::Var(var) = self.lookup_ref(ident)? else {
            let name = self.normalize_ident(ident);
            return Err(ErrorCode::ScriptSemanticError(format!(
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
                ErrorCode::ScriptSemanticError(format!("`{name}` is not a set"))
                    .set_span(ident.span),
            );
        };
        Ok(set)
    }

    fn lookup_iter(&self, ident: &Identifier) -> Result<IterRef> {
        let RefItem::Iter(iter) = self.lookup_ref(ident)? else {
            let name = self.normalize_ident(ident);
            return Err(
                ErrorCode::ScriptSemanticError(format!("`{name}` is not a row variable"))
                    .set_span(ident.span),
            );
        };
        Ok(iter)
    }

    fn lookup_loop(&self, ident: &Identifier) -> Result<LoopItem> {
        let name = self.normalize_ident(ident);
        for scope in self.scopes.iter().rev() {
            if let Some(item) = &scope.loop_item
                && item.name.as_ref() == Some(&name)
            {
                return Ok(item.clone());
            }
        }
        Err(ErrorCode::ScriptSemanticError(format!("`{name}` is not defined")).set_span(ident.span))
    }

    fn current_loop(&self, span: Span) -> Result<LoopItem> {
        for scope in self.scopes.iter().rev() {
            if let Some(loop_item) = &scope.loop_item {
                return Ok(loop_item.clone());
            }
        }
        Err(ErrorCode::ScriptSemanticError("not in a loop".to_string()).set_span(span))
    }

    fn quote_expr(&mut self, expr: &Expr) -> Result<(Vec<ScriptIR>, Expr)> {
        #[derive(VisitorMut)]
        #[visitor(Expr(enter), Identifier(enter))]
        struct QuoteVisitor<'a> {
            compiler: &'a mut Compiler,
            output: Vec<ScriptIR>,
            error: Option<ErrorCode>,
        }

        impl QuoteVisitor<'_> {
            fn enter_expr(&mut self, expr: &mut Expr) {
                match expr {
                    // Transform `variable` to `:index`.
                    Expr::ColumnRef {
                        span,
                        column:
                            ColumnRef {
                                database: None,
                                table: None,
                                column: ColumnID::Name(column),
                            },
                    } => {
                        let index = self.compiler.lookup_var(column);
                        match index {
                            Ok(index) => {
                                *expr = Expr::Hole {
                                    span: *span,
                                    name: index.index.to_string(),
                                };
                            }
                            Err(e) => {
                                self.error = Some(e.set_span(*span));
                            }
                        }
                    }
                    // Transform `iter.column` to `READ <iter>, <column>, to_var`, and replace the
                    // expression with `:to_var_index`.
                    Expr::ColumnRef {
                        span,
                        column:
                            ColumnRef {
                                database: None,
                                table: Some(iter),
                                column: ColumnID::Name(column),
                            },
                    } => {
                        let res = try {
                            // READ <iter>, <column>, to_var
                            let to_var = VarRef::new_internal(
                                column.span,
                                &format!("{iter}.{column}"),
                                &mut self.compiler.ref_allocator,
                            );
                            let iter = self.compiler.lookup_iter(iter)?;
                            let column =
                                ColumnAccess::Name(self.compiler.normalize_ident(column).0);
                            self.output.push(ScriptIR::Read {
                                iter,
                                column,
                                to_var: to_var.clone(),
                            });

                            *expr = Expr::Hole {
                                span: *span,
                                name: to_var.index.to_string(),
                            };
                        };

                        if let Err(err) = res {
                            self.error = Some(err);
                        }
                    }
                    Expr::Hole { span, .. } => {
                        self.error = Some(
                            ErrorCode::ScriptSemanticError(
                                "variable doesn't need to be quoted in this context, \
                                    try removing the colon"
                                    .to_string(),
                            )
                            .set_span(*span),
                        );
                    }
                    _ => {}
                }
            }

            fn enter_identifier(&mut self, ident: &mut Identifier) {
                if ident.is_hole() {
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
        let mut visitor = QuoteVisitor {
            compiler: self,
            output: vec![],
            error: None,
        };
        expr.drive_mut(&mut visitor);

        if let Some(err) = visitor.error {
            return Err(err);
        }

        Ok((visitor.output, expr))
    }
}

#[derive(Debug, Default)]
struct Scope {
    items: HashMap<RefName, RefItem>,
    loop_item: Option<LoopItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RefName(String);

impl Display for RefName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
enum RefItem {
    Var(VarRef),
    Set(SetRef),
    Iter(IterRef),
}

#[derive(Debug, Clone)]
struct LoopItem {
    name: Option<RefName>,
    continue_label: LabelRef,
    break_label: LabelRef,
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

    pub fn new_internal(span: Span, hint: &str, allocator: &mut RefAllocator) -> Self {
        let index = allocator.next_index;
        allocator.next_index += 1;
        Ref {
            span,
            index,
            display_name: format!("__{hint}{index}"),
        }
    }

    pub fn placeholder(index: usize) -> Self {
        Ref {
            span: None,
            index,
            display_name: format!(":{}", index),
        }
    }
}

fn wrap_eq(span: Span, lhs: Expr, rhs: Expr) -> Expr {
    Expr::BinaryOp {
        span,
        op: BinaryOperator::Eq,
        left: Box::new(lhs),
        right: Box::new(rhs),
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
