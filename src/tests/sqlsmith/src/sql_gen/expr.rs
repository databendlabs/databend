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

use common_ast::ast::BinaryOperator;
use common_ast::ast::ColumnID;
use common_ast::ast::Expr;
use common_ast::ast::Identifier;
use common_ast::ast::Literal;
use rand::distributions::Alphanumeric;
use rand::Rng;

use crate::sql_gen::SqlGenerator;

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn gen_expr(&mut self) -> Expr {
        match self.rng.gen_range(0..=9) {
            0..=4 => self.gen_column(),
            5..=8 => self.gen_literal(),
            9 => self.gen_binary_op(),
            // TODO other exprs
            _ => unreachable!(),
        }
    }

    fn gen_column(&mut self) -> Expr {
        // TODO: get table from context
        let table = &self.tables[0];

        let column = match self.rng.gen_range(0..=5) {
            0..=5 => {
                let fields = table.schema.fields();
                let index = self.rng.gen_range(0..fields.len());
                let name = Identifier::from_name(fields[index].name.clone());
                ColumnID::Name(name)
            }
            // TODO ColumnID::Position
            _ => unreachable!(),
        };

        Expr::ColumnRef {
            span: None,
            // TODO
            database: None,
            // TODO
            table: None,
            column,
        }
    }

    fn gen_literal(&mut self) -> Expr {
        let lit = match self.rng.gen_range(0..=4) {
            0 => Literal::Null,
            1 => Literal::Boolean(self.rng.gen_bool(0.5)),
            2 => {
                let v = self
                    .rng
                    .sample_iter(&Alphanumeric)
                    .take(5)
                    .map(u8::from)
                    .collect::<Vec<_>>();
                Literal::String(unsafe { String::from_utf8_unchecked(v) })
            }
            3 => {
                let v = self.rng.gen_range(0..=1000);
                Literal::UInt64(v)
            }
            4 => {
                let v = self.rng.gen_range(-40.0..1.3e5);
                Literal::Float64(v)
            }
            // TODO other literals
            _ => unreachable!(),
        };
        Expr::Literal { span: None, lit }
    }

    fn gen_binary_op(&mut self) -> Expr {
        let left = self.gen_expr();
        let right = self.gen_expr();
        let op = match self.rng.gen_range(0..=8) {
            0 => BinaryOperator::Gt,
            1 => BinaryOperator::Lt,
            2 => BinaryOperator::Gte,
            3 => BinaryOperator::Lte,
            4 => BinaryOperator::Eq,
            5 => BinaryOperator::NotEq,
            6 => BinaryOperator::And,
            7 => BinaryOperator::Or,
            8 => BinaryOperator::Xor,
            // TODO other binary operators
            _ => unreachable!(),
        };
        Expr::BinaryOp {
            span: None,
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }
}
