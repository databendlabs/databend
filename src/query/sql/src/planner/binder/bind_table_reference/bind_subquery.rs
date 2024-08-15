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

use databend_common_ast::ast::Query;
use databend_common_ast::ast::TableAlias;
use databend_common_exception::Result;

use crate::binder::Binder;
use crate::optimizer::SExpr;
use crate::BindContext;

impl Binder {
    /// Bind a subquery.
    pub(crate) fn bind_subquery(
        &mut self,
        bind_context: &mut BindContext,
        lateral: bool,
        subquery: &Query,
        alias: &Option<TableAlias>,
    ) -> Result<(SExpr, BindContext)> {
        // If the subquery is a lateral subquery, we need to let it see the columns
        // from the previous queries.
        let (result, mut result_bind_context) = if lateral {
            let mut new_bind_context = BindContext::with_parent(Box::new(bind_context.clone()));
            self.bind_query(&mut new_bind_context, subquery)?
        } else {
            let mut new_bind_context = BindContext::with_parent(
                bind_context
                    .parent
                    .clone()
                    .unwrap_or_else(|| Box::new(BindContext::new())),
            );
            self.bind_query(&mut new_bind_context, subquery)?
        };

        if let Some(alias) = alias {
            result_bind_context.apply_table_alias(alias)?;
            // Reset column name as alias column name
            for i in 0..alias.columns.len() {
                let column = &result_bind_context.columns[i];
                self.metadata
                    .write()
                    .change_derived_column_alias(column.index, column.column_name.clone());
            }
        }
        Ok((result, result_bind_context))
    }
}
