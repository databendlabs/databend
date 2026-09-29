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

use databend_common_ast::ast::Expr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::types::DataType;

use crate::MetadataRef;
use crate::planner::binder::BindContext;
use crate::planner::semantic::FullTypeCheckAdapter;
use crate::planner::semantic::NameResolutionContext;
use crate::planner::semantic::TypeChecker;
use crate::plans::ScalarExpr;

/// Helper for binding scalar expression with `BindContext`.
pub struct ScalarBinder<'a> {
    bind_context: &'a mut BindContext,
    ctx: Arc<dyn TableContext>,
    name_resolution_ctx: &'a NameResolutionContext,
    metadata: MetadataRef,
    aliases: &'a [(String, ScalarExpr)],
    forbid_udf: bool,
    context_independent: bool,
}

impl<'a> ScalarBinder<'a> {
    pub fn new(
        bind_context: &'a mut BindContext,
        ctx: Arc<dyn TableContext>,
        name_resolution_ctx: &'a NameResolutionContext,
        metadata: MetadataRef,
        aliases: &'a [(String, ScalarExpr)],
    ) -> Self {
        ScalarBinder {
            bind_context,
            ctx,
            name_resolution_ctx,
            metadata,
            aliases,
            forbid_udf: false,
            context_independent: false,
        }
    }

    pub fn forbid_udf(&mut self) {
        self.forbid_udf = true;
    }

    /// Reject session/query context functions such as `current_database()`,
    /// for expressions that are persisted or evaluated at the storage level.
    pub fn require_context_independent(&mut self) {
        self.context_independent = true;
    }

    pub fn bind(&mut self, expr: &Expr) -> Result<(ScalarExpr, DataType)> {
        let adapter = FullTypeCheckAdapter::new(self.ctx.clone())?
            .with_forbid_udf(self.forbid_udf)
            .with_context_independent(self.context_independent);
        let mut type_checker = TypeChecker::try_create_with_adapter(
            self.bind_context,
            adapter,
            self.name_resolution_ctx,
            self.metadata.clone(),
            self.aliases,
        )?;
        Ok(*type_checker.resolve(expr)?)
    }

    pub fn get_func_ctx(&self) -> Result<FunctionContext> {
        self.ctx.get_function_context()
    }
}
