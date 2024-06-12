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
use std::sync::Arc;

use databend_common_ast::ast::Expr;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use indexmap::IndexMap;

use crate::binder::wrap_cast;
use crate::binder::CteInfo;
use crate::planner::binder::BindContext;
use crate::planner::semantic::NameResolutionContext;
use crate::planner::semantic::TypeChecker;
use crate::plans::ScalarExpr;
use crate::IndexType;
use crate::MetadataRef;

/// Helper for binding scalar expression with `BindContext`.
pub struct ScalarBinder<'a> {
    bind_context: &'a mut BindContext,
    ctx: Arc<dyn TableContext>,
    dialect: Dialect,
    name_resolution_ctx: &'a NameResolutionContext,
    metadata: MetadataRef,
    m_cte_bound_ctx: HashMap<IndexType, BindContext>,
    ctes_map: Box<IndexMap<String, CteInfo>>,
    aliases: &'a [(String, ScalarExpr)],
    forbid_udf: bool,
}

impl<'a> ScalarBinder<'a> {
    pub fn new(
        bind_context: &'a mut BindContext,
        ctx: Arc<dyn TableContext>,
        name_resolution_ctx: &'a NameResolutionContext,
        metadata: MetadataRef,
        aliases: &'a [(String, ScalarExpr)],
        m_cte_bound_ctx: HashMap<IndexType, BindContext>,
        ctes_map: Box<IndexMap<String, CteInfo>>,
    ) -> Self {
        let dialect = ctx.get_settings().get_sql_dialect().unwrap_or_default();

        ScalarBinder {
            bind_context,
            ctx,
            dialect,
            name_resolution_ctx,
            metadata,
            m_cte_bound_ctx,
            ctes_map,
            aliases,
            forbid_udf: false,
        }
    }

    pub fn forbid_udf(&mut self) {
        self.forbid_udf = true;
    }

    pub fn bind(&mut self, expr: &Expr) -> Result<(ScalarExpr, DataType)> {
        let mut type_checker = TypeChecker::try_create(
            self.bind_context,
            self.ctx.clone(),
            self.name_resolution_ctx,
            self.metadata.clone(),
            self.aliases,
            self.forbid_udf,
        )?;
        type_checker.set_m_cte_bound_ctx(self.m_cte_bound_ctx.clone());
        type_checker.set_ctes_map(self.ctes_map.clone());
        Ok(*type_checker.resolve_new(expr)?)
    }

    pub fn get_func_ctx(&self) -> Result<FunctionContext> {
        self.ctx.get_function_context()
    }

    pub async fn get_default_value(
        &mut self,
        field: &DataField,
        schema: &DataSchema,
    ) -> Result<databend_common_expression::Expr> {
        if let Some(default_expr) = field.default_expr() {
            let tokens = tokenize_sql(default_expr)?;
            let ast = parse_expr(&tokens, self.dialect)?;
            let (mut scalar, _) = self.bind(&ast)?;
            scalar = wrap_cast(&scalar, field.data_type());

            let expr = scalar
                .as_expr()?
                .project_column_ref(|col| schema.index_of(&col.index.to_string()).unwrap());
            Ok(expr)
        } else {
            // If field data type is nullable, then we'll fill it with null.
            if field.data_type().is_nullable() {
                let expr = databend_common_expression::Expr::Constant {
                    span: None,
                    scalar: Scalar::Null,
                    data_type: field.data_type().clone(),
                };
                Ok(expr)
            } else {
                let data_type = field.data_type().clone();
                let default_value = Scalar::default_value(&data_type);
                let expr = databend_common_expression::Expr::Constant {
                    span: None,
                    scalar: default_value,
                    data_type,
                };
                Ok(expr)
            }
        }
    }
}
