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
use crate::planner::semantic::TypeCheckAdapter;
use crate::planner::semantic::TypeChecker;
use crate::plans::ScalarExpr;

/// Helper for binding scalar expressions with explicitly supplied capabilities.
pub struct ScalarBinder<'a, A = FullTypeCheckAdapter> {
    bind_context: &'a mut BindContext,
    // Preserve the infallible legacy constructor, reporting initialization errors
    // when binding. `with_adapter` always stores an already constructed adapter.
    adapter: Result<A>,
    name_resolution_ctx: &'a NameResolutionContext,
    metadata: MetadataRef,
    aliases: &'a [(String, ScalarExpr)],
}

impl<'a> ScalarBinder<'a, FullTypeCheckAdapter> {
    pub fn new(
        bind_context: &'a mut BindContext,
        ctx: Arc<dyn TableContext>,
        name_resolution_ctx: &'a NameResolutionContext,
        metadata: MetadataRef,
        aliases: &'a [(String, ScalarExpr)],
    ) -> Self {
        Self {
            bind_context,
            adapter: FullTypeCheckAdapter::new(ctx),
            name_resolution_ctx,
            metadata,
            aliases,
        }
    }

    pub fn forbid_udf(&mut self) {
        self.adapter = self
            .adapter
            .clone()
            .map(|adapter| adapter.with_forbid_udf(true));
    }
}

impl<'a, A: TypeCheckAdapter> ScalarBinder<'a, A> {
    pub fn with_adapter(
        bind_context: &'a mut BindContext,
        adapter: A,
        name_resolution_ctx: &'a NameResolutionContext,
        metadata: MetadataRef,
        aliases: &'a [(String, ScalarExpr)],
    ) -> Self {
        Self {
            bind_context,
            adapter: Ok(adapter),
            name_resolution_ctx,
            metadata,
            aliases,
        }
    }

    pub fn bind(&mut self, expr: &Expr) -> Result<(ScalarExpr, DataType)> {
        let mut type_checker = TypeChecker::try_create_with_adapter(
            self.bind_context,
            self.adapter.clone()?,
            self.name_resolution_ctx,
            self.metadata.clone(),
            self.aliases,
        )?;
        Ok(*type_checker.resolve(expr)?)
    }

    pub fn get_func_ctx(&self) -> Result<FunctionContext> {
        self.adapter.clone()?.function_context()
    }
}
