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

use derive_visitor::DriveMut;
use derive_visitor::VisitorMut;

use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Statement;

/// used in bendsql
#[derive(VisitorMut)]
#[visitor(Expr(enter), Identifier(enter))]
pub struct StatementReplacer<F: FnMut(&mut Expr), G: FnMut(&mut Identifier)> {
    replace_expr: F,
    replace_ident: G,
}

impl<F: FnMut(&mut Expr), G: FnMut(&mut Identifier)> StatementReplacer<F, G> {
    pub fn new(replace_expr: F, replace_ident: G) -> Self {
        Self {
            replace_expr,
            replace_ident,
        }
    }

    fn enter_expr(&mut self, expr: &mut Expr) {
        (self.replace_expr)(expr);
    }

    fn enter_identifier(&mut self, ident: &mut Identifier) {
        (self.replace_ident)(ident);
    }

    pub fn visit(&mut self, stmt: &mut Statement) {
        stmt.drive_mut(self);
    }
}
