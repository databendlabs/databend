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

use std::mem;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::FunctionKind;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::binder::select::SelectList;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::SExpr;
use crate::plans::walk_expr_mut;
use crate::plans::BoundColumnRef;
use crate::plans::FunctionCall;
use crate::plans::ProjectSet;
use crate::plans::ScalarItem;
use crate::plans::VisitorMut;
use crate::BindContext;
use crate::Binder;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::Visibility;

struct SetReturningRewriter<'a> {
    bind_context: &'a mut BindContext,
    metadata: MetadataRef,
}

impl<'a> SetReturningRewriter<'a> {
    fn new(bind_context: &'a mut BindContext, metadata: MetadataRef) -> Self {
        Self {
            bind_context,
            metadata,
        }
    }

    /// Replace the set returning function with a BoundColumnRef.
    fn replace_set_returning_function(&mut self, func: &FunctionCall) -> Result<ScalarExpr> {
        let srf_func = ScalarExpr::FunctionCall(func.clone());
        let data_type = srf_func.data_type()?;

        let column_index = self.metadata.write().add_derived_column(
            func.func_name.clone(),
            data_type.clone(),
            Some(srf_func.clone()),
        );
        let column = ColumnBindingBuilder::new(
            func.func_name.clone(),
            column_index,
            Box::new(data_type),
            Visibility::InVisible,
        )
        .build();

        // Add the srf to bind context, build ProjectSet plan later.
        self.bind_context.srfs.push(ScalarItem {
            index: column_index,
            scalar: srf_func,
        });

        Ok(BoundColumnRef {
            span: func.span,
            column,
        }
        .into())
    }
}

impl<'a> VisitorMut<'a> for SetReturningRewriter<'a> {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        if let ScalarExpr::FunctionCall(func) = expr {
            if BUILTIN_FUNCTIONS
                .get_property(&func.func_name)
                .map(|property| property.kind == FunctionKind::SRF)
                .unwrap_or(false)
            {
                *expr = self.replace_set_returning_function(func)?;
                return Ok(());
            }
        }

        walk_expr_mut(self, expr)
    }
}

impl Binder {
    /// Analyze project sets in select clause, this will rewrite project set functions.
    /// See [`SetReturningRewriter`] for more details.
    pub(crate) fn analyze_project_set_select(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
    ) -> Result<()> {
        let mut rewriter = SetReturningRewriter::new(bind_context, self.metadata.clone());
        for item in select_list.items.iter_mut() {
            rewriter.visit(&mut item.scalar)?;
        }

        Ok(())
    }

    pub(crate) fn bind_project_set(
        &mut self,
        bind_context: &mut BindContext,
        child: SExpr,
    ) -> Result<SExpr> {
        if bind_context.srfs.is_empty() {
            return Ok(child);
        }

        // Build a ProjectSet Plan.
        let srfs = mem::take(&mut bind_context.srfs);
        let project_set = ProjectSet { srfs };

        let new_expr = SExpr::create_unary(Arc::new(project_set.into()), Arc::new(child));

        Ok(new_expr)
    }
}
