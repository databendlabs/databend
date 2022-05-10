// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;

use crate::sessions::QueryContext;
use crate::sql::statements::query::query_ast_ir::QueryASTIRVisitor;
use crate::sql::statements::query::query_schema_joined::JoinedTableDesc;
use crate::sql::statements::query::JoinedSchema;
use crate::sql::statements::query::QueryASTIR;

pub struct QualifiedRewriter {
    tables_schema: JoinedSchema,
    ctx: Arc<QueryContext>,
}

impl QueryASTIRVisitor<QualifiedRewriter> for QualifiedRewriter {
    fn visit_expr(expr: &mut Expression, data: &mut QualifiedRewriter) -> Result<()> {
        match expr {
            Expression::Column(v) => {
                *expr = Self::rewrite_column(data, v)?;
                Ok(())
            }
            Expression::QualifiedColumn(names) => {
                *expr = Self::rewrite_qualified_column(data, names)?;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn visit_projection(exprs: &mut Vec<Expression>, data: &mut QualifiedRewriter) -> Result<()> {
        let mut new_exprs = Vec::with_capacity(exprs.len());

        // TODO: alias.*
        for projection_expr in exprs.iter_mut() {
            if let Expression::Alias(_, x) = projection_expr {
                if let Expression::Wildcard = x.as_ref() {
                    return Err(ErrorCode::SyntaxException("* AS alias is wrong syntax"));
                }
            }

            match projection_expr {
                Expression::Wildcard => Self::expand_wildcard(data, &mut new_exprs),
                _ => {
                    Self::visit_recursive_expr(projection_expr, data)?;
                    new_exprs.push(projection_expr.clone());
                }
            }
        }

        *exprs = new_exprs;
        Ok(())
    }
}

impl QualifiedRewriter {
    pub fn rewrite(
        schema: &JoinedSchema,
        ctx: Arc<QueryContext>,
        ir: &mut QueryASTIR,
    ) -> Result<()> {
        let mut rewriter = QualifiedRewriter {
            tables_schema: schema.clone(),
            ctx,
        };
        QualifiedRewriter::visit(ir, &mut rewriter)
    }

    fn expand_wildcard(&self, columns_expression: &mut Vec<Expression>) {
        for table_desc in self.tables_schema.get_tables_desc() {
            for column_desc in table_desc.get_columns_desc() {
                let name = column_desc.short_name.clone();
                match column_desc.is_ambiguity {
                    true => {
                        let prefix = table_desc.get_name_parts().join(".");
                        columns_expression.push(Expression::Column(format!("{}.{}", prefix, name)));
                    }
                    false => columns_expression.push(Expression::Column(name)),
                }
            }
        }
    }

    fn rewrite_column(&self, name: &str) -> Result<Expression> {
        match self.tables_schema.contains_column(name) {
            true => Ok(Expression::Column(name.to_string())),
            false => Err(ErrorCode::UnknownColumn(format!("Unknown column {}", name))),
        }
    }

    fn rewrite_qualified_column(&self, ref_names: &[String]) -> Result<Expression> {
        match self.best_match_table(ref_names) {
            None => Err(ErrorCode::UnknownColumn(format!(
                "Unknown column {}",
                ref_names.join(".")
            ))),
            Some((pos, table_ref)) => {
                let column_name = &ref_names[pos..];
                match column_name.len() {
                    1 => Self::find_column(&table_ref, &column_name[0]),
                    // TODO: column.field_a.field_b => GetField(field_b, GetField(field_a, column))
                    _ => Err(ErrorCode::SyntaxException(
                        "Unsupported complex type field access",
                    )),
                }
            }
        }
    }

    fn find_column(table_desc: &JoinedTableDesc, name: &str) -> Result<Expression> {
        let name_parts = table_desc.get_name_parts();
        for column_desc in table_desc.get_columns_desc() {
            if column_desc.short_name == name {
                return match column_desc.is_ambiguity {
                    true => Ok(Expression::Column(format!(
                        "{}.{}",
                        name_parts.join("."),
                        name
                    ))),
                    false => Ok(Expression::Column(name.to_string())),
                };
            }
        }

        Err(ErrorCode::UnknownColumn(format!(
            "Unknown column: {}.{}",
            name_parts.join("."),
            name
        )))
    }

    fn first_diff_pos(left: &[String], right: &[String]) -> usize {
        let min_len = std::cmp::min(left.len(), right.len());

        for index in 0..min_len {
            if left[index] != right[index] {
                return index;
            }
        }

        min_len
    }

    fn best_match_table(&self, ref_names: &[String]) -> Option<(usize, JoinedTableDesc)> {
        if ref_names.len() <= 1 {
            return None;
        }

        let current_database = self.ctx.get_current_database();
        let current_catalog = self.ctx.get_current_catalog();
        for table_desc in self.tables_schema.get_tables_desc() {
            let name_parts = table_desc.get_name_parts();
            if Self::first_diff_pos(ref_names, name_parts) == name_parts.len() {
                // alias.column or database.table.column
                return Some((name_parts.len(), table_desc.clone()));
            }

            if name_parts.len() == 2
                && Self::first_diff_pos(ref_names, &name_parts[1..]) == 1
                && current_database == name_parts[0]
            {
                // use current_database; table.column
                return Some((1, table_desc.clone()));
            }

            if name_parts.len() == 3
                && Self::first_diff_pos(ref_names, &name_parts[2..]) == 1
                && current_catalog == name_parts[0]
                && current_database == name_parts[1]
            {
                // use current catalog; current_database; table.column
                return Some((1, table_desc.clone()));
            }
        }

        None
    }
}
