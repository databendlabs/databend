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

use std::collections::HashSet;

use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::TableReference;
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::Visitor;
use databend_common_ast::visit::VisitorMut;
use databend_common_ast::visit::Walk;

use crate::NameResolutionContext;
use crate::normalize_identifier;

/// Qualifies table references without qualifying possible CTE references.
///
/// CTE definitions may be bound eagerly (materialized) or in the referencing scope
/// (non-materialized). An AST walk cannot decide which binding a name will have. Leave
/// every name that could refer to a CTE unqualified, and let the binder resolve it in
/// the stored definition's default catalog/database. Other table names retain the
/// existing database qualification, as do references the user qualified explicitly.
pub struct ViewRewriter {
    current_database: String,
    name_resolution_ctx: NameResolutionContext,
    cte_names: Option<HashSet<String>>,
}

impl ViewRewriter {
    pub fn new(
        current_database: impl Into<String>,
        name_resolution_ctx: NameResolutionContext,
    ) -> Self {
        Self {
            current_database: current_database.into(),
            name_resolution_ctx,
            cte_names: None,
        }
    }
}

impl VisitorMut for ViewRewriter {
    fn visit_query(&mut self, query: &mut Query) -> Result<VisitControl, !> {
        if self.cte_names.is_none() {
            let mut collector = CteNameCollector {
                names: HashSet::new(),
                name_resolution_ctx: &self.name_resolution_ctx,
            };
            let Ok(_) = query.walk(&mut collector);
            self.cte_names = Some(collector.names);
        }
        Ok(VisitControl::Continue)
    }

    fn visit_table_reference(&mut self, table_ref: &mut TableReference) -> Result<VisitControl, !> {
        if let TableReference::Table { span, table, .. } = table_ref
            && table.database.is_none()
        {
            let name = normalize_identifier(&table.table, &self.name_resolution_ctx).name;
            if !self
                .cte_names
                .as_ref()
                .is_some_and(|names| names.contains(&name))
            {
                table.database = Some(Identifier::from_name(*span, self.current_database.clone()));
            }
        }
        Ok(VisitControl::Continue)
    }
}

struct CteNameCollector<'a> {
    names: HashSet<String>,
    name_resolution_ctx: &'a NameResolutionContext,
}

impl Visitor for CteNameCollector<'_> {
    fn visit_query(&mut self, query: &Query) -> Result<VisitControl, !> {
        if let Some(with) = &query.with {
            self.names.extend(
                with.ctes.iter().map(|cte| {
                    normalize_identifier(&cte.alias.name, self.name_resolution_ctx).name
                }),
            );
        }
        Ok(VisitControl::Continue)
    }
}
