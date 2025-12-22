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

use std::sync::Arc;

use databend_common_ast::ast::Expr as AExpr;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Expr;
use databend_common_meta_app::schema::Constraint;
use parking_lot::RwLock;

use crate::BindContext;
use crate::ColumnBindingBuilder;
use crate::ColumnSet;
use crate::Metadata;
use crate::MetadataRef;
use crate::NameResolutionContext;
use crate::ScalarExpr;
use crate::TypeChecker;
use crate::Visibility;

/// Helper for binding scalar expression with `BindContext`.
pub struct ConstraintExprBinder {
    pub bind_context: BindContext,
    ctx: Arc<dyn TableContext>,
    dialect: Dialect,
    name_resolution_ctx: NameResolutionContext,
    metadata: MetadataRef,

    schema: DataSchemaRef,
}

impl ConstraintExprBinder {
    pub fn try_new(ctx: Arc<dyn TableContext>, schema: DataSchemaRef) -> Result<Self> {
        let settings = ctx.get_settings();
        let dialect = ctx.get_settings().get_sql_dialect().unwrap_or_default();
        let mut bind_context = BindContext::new();
        for (index, field) in schema.fields().iter().enumerate() {
            let column = ColumnBindingBuilder::new(
                field.name().clone(),
                index,
                Box::new(field.data_type().clone()),
                Visibility::Visible,
            )
            .build();

            bind_context.add_column_binding(column);
        }
        let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
        let metadata = Arc::new(RwLock::new(Metadata::default()));

        Ok(ConstraintExprBinder {
            bind_context,
            ctx,
            dialect,
            name_resolution_ctx,
            metadata,
            schema,
        })
    }

    fn parse(&mut self, default_expr: &str) -> Result<AExpr> {
        let tokens = tokenize_sql(default_expr)?;
        let ast = parse_expr(&tokens, self.dialect)?;
        Ok(ast)
    }

    pub(crate) fn bind(&mut self, ast: &AExpr) -> Result<ScalarExpr> {
        let mut type_checker = TypeChecker::try_create(
            &mut self.bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            true,
        )?;
        let (scalar, _) = *type_checker.resolve(ast)?;
        Ok(scalar)
    }

    pub(crate) fn default_name(
        &self,
        table: &str,
        used_columns: &ColumnSet,
        constraint: &Constraint,
    ) -> String {
        let mut constraint_name = format!("{}_", table);

        for i in used_columns.iter() {
            constraint_name
                .push_str(format!("{}_", self.bind_context.columns[*i].column_name).as_str());
        }
        match constraint {
            Constraint::Check(_) => constraint_name.push_str("check"),
        }
        constraint_name
    }

    pub fn parse_and_bind(&mut self, constraint_name: &str, expr: &str) -> Result<ScalarExpr> {
        let ast = self.parse(expr).map_err(|e| {
            format!(
                "fail to parse constraint expr `{}` (string length = {}) of constraint {}, {}",
                expr,
                expr.len(),
                constraint_name,
                e
            )
        })?;
        self.bind(&ast)
    }

    pub fn get_expr(&mut self, constraint_name: &str, expr: &str) -> Result<Expr> {
        let scalar_expr = self.parse_and_bind(constraint_name, expr)?;
        scalar_expr
            .as_expr()?
            .project_column_ref(|col| self.schema.index_of(&col.column_name))
    }
}
