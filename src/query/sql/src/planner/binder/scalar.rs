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

use common_ast::ast::Expr;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::FunctionContext;

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
    name_resolution_ctx: &'a NameResolutionContext,
    metadata: MetadataRef,
    m_cte_bound_ctx: HashMap<IndexType, BindContext>,
    aliases: &'a [(String, ScalarExpr)],
    allow_pushdown: bool,
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
    ) -> Self {
        ScalarBinder {
            bind_context,
            ctx,
            name_resolution_ctx,
            metadata,
            m_cte_bound_ctx,
            aliases,
            allow_pushdown: false,
            forbid_udf: false,
        }
    }

    pub fn allow_pushdown(&mut self) {
        self.allow_pushdown = true;
    }

    pub fn forbid_udf(&mut self) {
        self.forbid_udf = true;
    }

    #[async_backtrace::framed]
    pub async fn bind(&mut self, expr: &Expr) -> Result<(ScalarExpr, DataType)> {
        let mut type_checker = TypeChecker::new(
            self.bind_context,
            self.ctx.clone(),
            self.name_resolution_ctx,
            self.metadata.clone(),
            self.aliases,
            self.allow_pushdown,
            self.forbid_udf,
        );
        type_checker.set_m_cte_bound_ctx(self.m_cte_bound_ctx.clone());
        Ok(*type_checker.resolve(expr).await?)
    }

    pub fn get_func_ctx(&self) -> Result<FunctionContext> {
        self.ctx.get_function_context()
    }
}
