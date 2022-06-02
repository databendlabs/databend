// Copyright 2022 Datafuse Labs.
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

use common_ast::ast::Expr;
use common_datavalues::DataTypeImpl;
use common_exception::Result;

use crate::sessions::QueryContext;
use crate::sql::planner::binder::BindContext;
use crate::sql::planner::metadata::MetadataRef;
use crate::sql::planner::semantic::TypeChecker;
use crate::sql::plans::Scalar;

/// Helper for binding scalar expression with `BindContext`.
pub struct ScalarBinder<'a> {
    bind_context: &'a BindContext,
    ctx: Arc<QueryContext>,
    metadata: MetadataRef,
}

impl<'a> ScalarBinder<'a> {
    pub fn new(
        bind_context: &'a BindContext,
        ctx: Arc<QueryContext>,
        metadata: MetadataRef,
    ) -> Self {
        ScalarBinder {
            bind_context,
            ctx,
            metadata,
        }
    }

    pub async fn bind(&mut self, expr: &Expr<'a>) -> Result<(Scalar, DataTypeImpl)> {
        let mut type_checker =
            TypeChecker::new(self.bind_context, self.ctx.clone(), self.metadata.clone());
        type_checker.resolve(expr, None).await
    }
}
