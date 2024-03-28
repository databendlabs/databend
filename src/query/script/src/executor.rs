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
