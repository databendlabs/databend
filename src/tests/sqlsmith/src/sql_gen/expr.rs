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

use common_ast::ast::ColumnID;
use common_ast::ast::Expr;
use common_ast::ast::Identifier;
use rand::Rng;

use crate::sql_gen::SqlGenerator;

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn gen_expr(&mut self) -> Expr {
        match self.rng.gen_range(0..=9) {
            0..=9 => self.gen_column(),
            // TODO other exprs
            _ => unreachable!(),
        }
    }

    pub(crate) fn gen_column(&mut self) -> Expr {
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
}
