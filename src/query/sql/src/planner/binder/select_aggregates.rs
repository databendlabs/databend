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

//! The aggregates of a SELECT that decisions about it depend on, found
//! before its clauses are bound, see `type_check/aggregate_facts.rs`.

use std::collections::BTreeSet;
use std::collections::HashSet;

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::OrderByExpr;
use databend_common_ast::ast::SelectTarget;
use databend_common_base::runtime::block_on;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_users::UserApiProvider;

use crate::BindContext;
use crate::binder::Binder;
use crate::normalize_identifier;
use crate::planner::semantic::CoreExprArena;
use crate::planner::semantic::FullTypeCheckAdapter;
use crate::planner::semantic::UdafNames;
use crate::planner::semantic::sql_udf_aggregates;
use crate::planner::semantic::sql_udf_body;

/// The aggregates of the clauses of a SELECT after FROM.
pub(crate) struct SelectAggregates {
    /// The user-defined functions they call that aggregate: UDAFs, and SQL
    /// UDFs whose body calls an aggregate function.
    pub(crate) udaf_names: HashSet<String>,
    /// Whether the SELECT list, HAVING or ORDER BY calls an aggregate
    /// function, which makes the SELECT grouped even without GROUP BY.
    pub(crate) contains_aggregate: bool,
}

impl Binder {
    /// Lower the clauses of a SELECT after FROM, load the user-defined
    /// aggregate functions they call and find whether they aggregate. Nothing
    /// is bound.
    pub(crate) fn analyze_select_aggregates(
        &self,
        bind_context: &BindContext,
        select_list: &[SelectTarget],
        having: Option<&Expr>,
        qualify: Option<&Expr>,
        order_by: &[OrderByExpr],
    ) -> Result<SelectAggregates> {
        let mut arena = CoreExprArena::for_adapter(&FullTypeCheckAdapter::new(self.ctx.clone())?)?;
        let select_list = select_list
            .iter()
            .filter_map(|target| match target {
                SelectTarget::AliasedExpr { expr, .. } => Some(expr.as_ref()),
                SelectTarget::StarColumns { .. } => None,
            })
            .map(|expr| arena.lower_ast_expr(expr))
            .collect::<Result<Vec<_>>>()?;
        let having = having.map(|expr| arena.lower_ast_expr(expr)).transpose()?;
        let qualify = qualify.map(|expr| arena.lower_ast_expr(expr)).transpose()?;
        let order_by = order_by
            .iter()
            .map(|order| arena.lower_ast_expr(&order.expr))
            .collect::<Result<Vec<_>>>()?;

        let roots = || {
            select_list
                .iter()
                .chain(&having)
                .chain(&qualify)
                .chain(&order_by)
                .copied()
        };
        let function_names = roots()
            .flat_map(|root| arena.udf_call_names(root))
            .map(|name| normalize_identifier(name, &self.name_resolution_ctx).name)
            .collect();
        let udaf_names = self.resolve_udaf_names(bind_context, function_names)?;
        let udafs = UdafNames {
            names: &udaf_names,
            name_resolution_ctx: &self.name_resolution_ctx,
        };

        if qualify.is_some_and(|root| arena.contains_aggregate_outside_windows(root, &udafs)) {
            return Err(ErrorCode::SemanticError(
                "Qualify clause must not contain aggregate functions",
            ));
        }
        let contains_aggregate = select_list
            .iter()
            .chain(&having)
            .chain(&order_by)
            .any(|root| arena.contains_grouping_aggregate(*root, &udafs));
        Ok(SelectAggregates {
            udaf_names,
            contains_aggregate,
        })
    }

    fn resolve_udaf_names(
        &self,
        bind_context: &BindContext,
        function_names: BTreeSet<String>,
    ) -> Result<HashSet<String>> {
        let tenant = self.ctx.get_tenant();
        let provider = UserApiProvider::instance();
        let mut load = |name: &str| -> Result<Option<UserDefinedFunction>> {
            if let Some(udf) = bind_context.udf_cache.read().get(name).cloned() {
                return Ok(udf);
            }
            let udf = block_on(provider.get_udf(&tenant, name))?;
            bind_context
                .udf_cache
                .write()
                .insert(name.to_string(), udf.clone());
            Ok(udf)
        };

        let mut udaf_names = HashSet::new();
        for name in function_names {
            let Some(udf) = load(&name)? else {
                continue;
            };
            let aggregates = if udf.definition.is_aggregate() {
                true
            } else if let Some(body) = sql_udf_body(&udf, self.dialect)? {
                sql_udf_aggregates(&body, &self.name_resolution_ctx, self.dialect, &mut load, 0)?
            } else {
                false
            };
            if aggregates {
                udaf_names.insert(name);
            }
        }

        Ok(udaf_names)
    }
}
