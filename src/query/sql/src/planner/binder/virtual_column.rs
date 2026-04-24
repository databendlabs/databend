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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;

use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::CTE;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::MapAccessor;
use databend_common_ast::ast::RefreshVirtualColumnStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::ShowVirtualColumnsStmt;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::VacuumVirtualColumnStmt;
use databend_common_ast::ast::With;
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::Visitor;
use databend_common_ast::visit::VisitorMut;
use databend_common_ast::visit::Walk;
use databend_common_ast::visit::WalkMut;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::debug;

use crate::BindContext;
use crate::NameResolutionContext;
use crate::SelectBuilder;
use crate::binder::Binder;
use crate::normalize_identifier;
use crate::plans::Plan;
use crate::plans::RefreshSelection;
use crate::plans::RefreshVirtualColumnPlan;
use crate::plans::RewriteKind;
use crate::plans::VacuumVirtualColumnPlan;

pub(in crate::planner::binder) const MATERIALIZED_CTE_VIRTUAL_COLUMN_PREFIX: &str =
    "__databend_virtual_column__";

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_refresh_virtual_column(
        &mut self,
        stmt: &RefreshVirtualColumnStmt,
    ) -> Result<Plan> {
        let RefreshVirtualColumnStmt {
            catalog,
            database,
            table,
            selection,
            limit,
            overwrite,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let parsed_selection = if let Some(selection) = selection {
            Some(self.parse_refresh_virtual_column_selection(selection)?)
        } else {
            None
        };

        Ok(Plan::RefreshVirtualColumn(Box::new(
            RefreshVirtualColumnPlan {
                catalog,
                database,
                table,
                limit: *limit,
                overwrite: *overwrite,
                selection: parsed_selection,
            },
        )))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_virtual_columns(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowVirtualColumnsStmt,
    ) -> Result<Plan> {
        let ShowVirtualColumnsStmt {
            catalog,
            database,
            table,
            limit,
        } = stmt;

        let catalog_name = match catalog {
            None => self.ctx.get_current_catalog(),
            Some(ident) => {
                let catalog = normalize_identifier(ident, &self.name_resolution_ctx).name;
                self.ctx.get_catalog(&catalog).await?;
                catalog
            }
        };
        let catalog = self.ctx.get_catalog(&catalog_name).await?;
        let database = match database {
            None => self.ctx.get_current_database(),
            Some(ident) => {
                let database = normalize_identifier(ident, &self.name_resolution_ctx).name;
                catalog
                    .get_database(&self.ctx.get_tenant(), &database)
                    .await?;
                database
            }
        };

        let mut select_builder = SelectBuilder::from("default.system.virtual_columns");
        select_builder
            .with_column("database")
            .with_column("table")
            .with_column("source_column")
            .with_column("virtual_column_id")
            .with_column("virtual_column_name")
            .with_column("virtual_column_type");

        select_builder.with_filter(format!("database = '{database}'"));
        if let Some(table) = table {
            let table = normalize_identifier(table, &self.name_resolution_ctx).name;
            select_builder.with_filter(format!("table = '{table}'"));
        }

        let query = match limit {
            None => select_builder.build(),
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("virtual_column_name LIKE '{pattern}'"));
                select_builder.build()
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
                select_builder.build()
            }
        };
        debug!("show virtual columns rewrite to: {:?}", query);

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowVirtualColumns)
            .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_vacuum_virtual_column(
        &mut self,
        stmt: &VacuumVirtualColumnStmt,
    ) -> Result<Plan> {
        let VacuumVirtualColumnStmt {
            catalog,
            database,
            table,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        Ok(Plan::VacuumVirtualColumn(Box::new(
            VacuumVirtualColumnPlan {
                catalog,
                database,
                table,
            },
        )))
    }

    fn parse_refresh_virtual_column_selection(&self, expr: &Expr) -> Result<RefreshSelection> {
        match expr {
            Expr::BinaryOp {
                op, left, right, ..
            } if op == &BinaryOperator::Eq => {
                if let Some(selection) =
                    self.try_build_selection_from_operands(left.as_ref(), right.as_ref())?
                {
                    return Ok(selection);
                }
                if let Some(selection) =
                    self.try_build_selection_from_operands(right.as_ref(), left.as_ref())?
                {
                    return Ok(selection);
                }
                Err(ErrorCode::BadArguments(
                    "Only equality predicate between segment_location, block_location and string literal is supported",
                ))
            }
            _ => Err(ErrorCode::BadArguments(
                "Only equality predicate between segment_location, block_location and string literal is supported",
            )),
        }
    }

    fn try_build_selection_from_operands(
        &self,
        column_expr: &Expr,
        literal_expr: &Expr,
    ) -> Result<Option<RefreshSelection>> {
        let column_name = match column_expr {
            Expr::ColumnRef {
                column:
                    ColumnRef {
                        database: None,
                        table: None,
                        column: ColumnID::Name(ident),
                    },
                ..
            } => normalize_identifier(ident, &self.name_resolution_ctx).name,
            _ => {
                return Ok(None);
            }
        };

        let literal_value = match literal_expr {
            Expr::Literal {
                value: Literal::String(value),
                ..
            } => value.clone(),
            _ => {
                return Ok(None);
            }
        };

        let column_name_lower = column_name.to_lowercase();
        match column_name_lower.as_str() {
            "block_location" => Ok(Some(RefreshSelection::BlockLocation(literal_value))),
            "segment_location" => Ok(Some(RefreshSelection::SegmentLocation(literal_value))),
            _ => Ok(None),
        }
    }
}

/// Collects aliases that refer to visible materialized CTEs in a consumer query.
///
/// The rewriter uses this map to resolve both direct CTE references (`FROM logs`) and aliased
/// references (`FROM logs AS l`) back to the producer CTE name.
struct MaterializedCteAliasCollector {
    cte_names: HashSet<String>,
    aliases: HashMap<String, String>,
    name_resolution_ctx: NameResolutionContext,
}

impl Visitor for MaterializedCteAliasCollector {
    fn visit_table_reference(
        &mut self,
        table_ref: &TableReference,
    ) -> std::result::Result<VisitControl, !> {
        if let TableReference::Table { table, alias, .. } = table_ref {
            let table_name = normalize_identifier(&table.table, &self.name_resolution_ctx).name;
            if self.cte_names.contains(&table_name) {
                let alias_name = alias
                    .as_ref()
                    .map(|alias| normalize_identifier(&alias.name, &self.name_resolution_ctx).name)
                    .unwrap_or_else(|| table_name.clone());
                self.aliases.insert(alias_name, table_name);
            }
        }
        Ok(VisitControl::Continue)
    }
}

struct MaterializedCteRequirement {
    alias: String,
    expr: Expr,
}

/// Checks whether a materialized CTE producer can safely add hidden JSON extraction outputs.
///
/// This checker does not require the source table to have Fuse virtual columns enabled. If virtual
/// columns exist, normal variant binding can still push the hidden expression to the scan; otherwise
/// the hidden expression is evaluated inside the materialized CTE producer, reducing the size of
/// data passed to later consumers.
struct MaterializedCteSourceChecker {
    has_source: bool,
}

impl Visitor for MaterializedCteSourceChecker {
    fn visit_table_reference(
        &mut self,
        table_ref: &TableReference,
    ) -> std::result::Result<VisitControl, !> {
        match table_ref {
            TableReference::Table { .. } => {
                self.has_source = true;
            }
            TableReference::TableFunction { .. } | TableReference::Location { .. } => {
                self.has_source = false;
                return Ok(VisitControl::Break(()));
            }
            _ => {}
        }
        Ok(VisitControl::Continue)
    }
}

/// Rewrites static JSON path accesses on materialized CTE outputs into hidden CTE columns.
///
/// When a consumer reads `message['a']` from a materialized CTE output, this visitor records the
/// full producer expression, assigns it a hidden alias, and replaces the consumer expression with
/// that alias. The caller later appends all recorded expressions to the producer CTE select list.
struct MaterializedCteVirtualColumnRewriter {
    ctes: HashMap<String, HashMap<String, Expr>>,
    table_aliases: HashMap<String, String>,
    requirements: HashMap<String, BTreeMap<String, MaterializedCteRequirement>>,
    next_id: usize,
    name_resolution_ctx: NameResolutionContext,
}

impl VisitorMut for MaterializedCteVirtualColumnRewriter {
    fn visit_expr(&mut self, expr: &mut Expr) -> std::result::Result<VisitControl, !> {
        let Some((column, accessors)) = extract_static_column_map_access(expr) else {
            return Ok(VisitControl::Continue);
        };

        let column_name = normalize_column_id(&column.column, &self.name_resolution_ctx);
        let Some(cte_name) = self.resolve_cte_name(&column, &column_name) else {
            return Ok(VisitControl::Continue);
        };
        let Some(producer_outputs) = self.ctes.get(&cte_name) else {
            return Ok(VisitControl::Continue);
        };
        let Some(producer_expr) = producer_outputs.get(&column_name) else {
            return Ok(VisitControl::Continue);
        };

        let requirement_expr = Self::append_map_accessors(producer_expr.clone(), &accessors);
        let requirement_key = requirement_expr.to_string();
        let requirements = self.requirements.entry(cte_name).or_default();
        let requirement = requirements.entry(requirement_key).or_insert_with(|| {
            let alias = format!("{MATERIALIZED_CTE_VIRTUAL_COLUMN_PREFIX}{}", self.next_id);
            self.next_id += 1;
            MaterializedCteRequirement {
                alias,
                expr: requirement_expr,
            }
        });

        *expr = Expr::ColumnRef {
            span: expr.span(),
            column: ColumnRef {
                database: None,
                table: column.table.clone(),
                column: ColumnID::Name(Identifier::from_name(None, requirement.alias.clone())),
            },
        };
        Ok(VisitControl::SkipChildren)
    }
}

impl MaterializedCteVirtualColumnRewriter {
    fn append_map_accessors(mut expr: Expr, accessors: &[MapAccessor]) -> Expr {
        for accessor in accessors {
            expr = Expr::MapAccess {
                span: None,
                expr: Box::new(expr),
                accessor: accessor.clone(),
            };
        }
        expr
    }

    fn resolve_cte_name(&self, column: &ColumnRef, column_name: &str) -> Option<String> {
        if let Some(table) = &column.table {
            let table_name = normalize_identifier(table, &self.name_resolution_ctx).name;
            return self.table_aliases.get(&table_name).cloned();
        }

        let mut matched_cte = None;
        for cte_name in self.table_aliases.values() {
            let Some(outputs) = self.ctes.get(cte_name) else {
                continue;
            };
            if outputs.contains_key(column_name) {
                if matched_cte.is_some() {
                    return None;
                }
                matched_cte = Some(cte_name.clone());
            }
        }
        matched_cte
    }
}

fn normalize_column_id(
    column_id: &ColumnID,
    name_resolution_ctx: &NameResolutionContext,
) -> String {
    match column_id {
        ColumnID::Name(ident) => normalize_identifier(ident, name_resolution_ctx).name,
        ColumnID::Position(pos) => pos.name(),
    }
}

fn extract_static_column_map_access(expr: &Expr) -> Option<(ColumnRef, Vec<MapAccessor>)> {
    if let Expr::ColumnRef { column, .. } = expr {
        return Some((column.clone(), Vec::new()));
    }
    if !matches!(expr, Expr::MapAccess { .. }) {
        return None;
    }

    let mut accessors = Vec::new();
    let mut current = expr;
    while let Expr::MapAccess { expr, accessor, .. } = current {
        match accessor {
            MapAccessor::Bracket { key } => {
                if !matches!(key.as_ref(), Expr::Literal {
                    value: Literal::String(_) | Literal::UInt64(_),
                    ..
                }) {
                    return None;
                }
            }
            MapAccessor::DotNumber { .. } | MapAccessor::Colon { .. } => {}
        }
        accessors.push(accessor.clone());
        current = expr;
    }
    accessors.reverse();

    if let Expr::ColumnRef { column, .. } = current {
        Some((column.clone(), accessors))
    } else {
        None
    }
}

impl Binder {
    /// Rewrites JSON path accesses on materialized CTE outputs back into the CTE producer.
    ///
    /// Materializing a CTE can hide the original base-table variant column from later binding.
    /// For example, after `logs` is materialized, the consumer only sees `message`:
    ///
    /// ```sql
    /// WITH logs AS (
    ///     SELECT v['message'] AS message
    ///     FROM t
    /// )
    /// SELECT message['attribute']['user_id']
    /// FROM logs;
    /// ```
    ///
    /// Without this rewrite, the final access is bound against the materialized CTE output
    /// `message`, so the virtual-column rewrite can no longer see the full source-table path
    /// `v['message']['attribute']['user_id']`. This function rewrites the query shape to:
    ///
    /// ```sql
    /// WITH logs AS (
    ///     SELECT
    ///         v['message'] AS message,
    ///         v['message']['attribute']['user_id']
    ///             AS __databend_virtual_column__0
    ///     FROM t
    /// )
    /// SELECT __databend_virtual_column__0
    /// FROM logs;
    /// ```
    ///
    /// The rewrite runs in three steps:
    /// 1. Collect auto-materialized CTEs whose producers can safely add hidden static JSON
    ///    extraction outputs. This does not require source tables to have virtual columns enabled:
    ///    the hidden expression is still evaluated earlier inside the CTE producer, and normal
    ///    variant binding can push it to a Fuse virtual column when one exists.
    /// 2. Visit downstream CTEs and the query body. When a consumer reads a static JSON path
    ///    from a materialized CTE output, record the corresponding full producer expression and
    ///    replace the consumer expression with a generated hidden column alias.
    /// 3. Append all recorded hidden expressions to the producer CTE select list, so normal
    ///    variant virtual-column binding can still resolve the full base-table path later.
    pub(crate) fn rewrite_materialized_cte_virtual_columns(
        &mut self,
        bind_context: &BindContext,
        with: &mut With,
        body: &mut SetExpr,
    ) {
        if !bind_context.allow_virtual_column || with.recursive {
            return;
        }

        let mut materialized_ctes = Vec::new();
        for (index, cte) in with.ctes.iter().enumerate() {
            // Explicit `AS MATERIALIZED` currently creates a temporary table before the outer
            // query is bound. Rewriting it here could leak hidden columns into that temp table
            // schema, so only auto-materialized CTEs participate in this rewrite.
            if cte.user_specified_materialized || !cte.materialized {
                continue;
            }
            let cte_name = self.normalize_identifier(&cte.alias.name).name;
            if let Some(outputs) = self.collect_materialized_cte_virtual_column_outputs(cte) {
                materialized_ctes.push((index, cte_name, outputs));
            }
        }
        if materialized_ctes.is_empty() {
            return;
        }

        let mut rewriter = MaterializedCteVirtualColumnRewriter {
            ctes: HashMap::new(),
            table_aliases: HashMap::new(),
            requirements: HashMap::new(),
            next_id: 0,
            name_resolution_ctx: self.name_resolution_ctx.clone(),
        };

        for (consumer_index, consumer_cte) in with.ctes.iter_mut().enumerate() {
            let visible_materialized_cte_count = materialized_ctes
                .iter()
                .position(|(index, _, _)| *index >= consumer_index)
                .unwrap_or(materialized_ctes.len());
            Self::rewrite_materialized_cte_consumer_with_visible_ctes(
                &materialized_ctes[..visible_materialized_cte_count],
                &mut rewriter,
                &mut consumer_cte.query.body,
                self.name_resolution_ctx.clone(),
            );
        }

        Self::rewrite_materialized_cte_consumer_with_visible_ctes(
            &materialized_ctes,
            &mut rewriter,
            body,
            self.name_resolution_ctx.clone(),
        );

        if rewriter.requirements.is_empty() {
            return;
        }

        for cte in &mut with.ctes {
            let cte_name = normalize_identifier(&cte.alias.name, &self.name_resolution_ctx).name;
            let Some(requirements) = rewriter.requirements.remove(&cte_name) else {
                continue;
            };
            let select = match &mut cte.query.body {
                SetExpr::Select(select) => select.as_mut(),
                _ => continue,
            };
            for requirement in requirements.into_values() {
                select.select_list.push(SelectTarget::AliasedExpr {
                    expr: Box::new(requirement.expr),
                    alias: Some(Identifier::from_name(None, requirement.alias)),
                });
            }
        }
    }

    fn collect_materialized_cte_virtual_column_outputs(
        &self,
        cte: &CTE,
    ) -> Option<HashMap<String, Expr>> {
        let select = match &cte.query.body {
            SetExpr::Select(select) => select.as_ref(),
            _ => return None,
        };
        let mut source_checker = MaterializedCteSourceChecker { has_source: false };
        if select.from.walk(&mut source_checker).is_err() {
            return None;
        }
        if !source_checker.has_source {
            return None;
        }

        let mut outputs = HashMap::new();
        for (index, item) in select.select_list.iter().enumerate() {
            let SelectTarget::AliasedExpr { expr, alias } = item else {
                continue;
            };
            if extract_static_column_map_access(expr.as_ref()).is_none() {
                continue;
            }
            let column = if !cte.alias.columns.is_empty() {
                let Some(column) = cte.alias.columns.get(index) else {
                    continue;
                };
                column
            } else if let Some(alias) = alias {
                alias
            } else {
                continue;
            };
            let output_name = self.normalize_identifier(column).name;
            outputs.insert(output_name, expr.as_ref().clone());
        }
        if outputs.is_empty() {
            None
        } else {
            Some(outputs)
        }
    }

    fn rewrite_materialized_cte_consumer_with_visible_ctes(
        visible_ctes: &[(usize, String, HashMap<String, Expr>)],
        rewriter: &mut MaterializedCteVirtualColumnRewriter,
        body: &mut SetExpr,
        name_resolution_ctx: NameResolutionContext,
    ) -> bool {
        let ctes = visible_ctes
            .iter()
            .map(|(_, name, outputs)| (name.clone(), outputs.clone()))
            .collect::<HashMap<_, _>>();
        if ctes.is_empty() {
            return false;
        }
        let cte_names = ctes.keys().cloned().collect();
        Self::rewrite_materialized_cte_consumer(
            rewriter,
            ctes,
            cte_names,
            body,
            name_resolution_ctx,
        )
    }

    fn rewrite_materialized_cte_consumer(
        rewriter: &mut MaterializedCteVirtualColumnRewriter,
        ctes: HashMap<String, HashMap<String, Expr>>,
        cte_names: HashSet<String>,
        body: &mut SetExpr,
        name_resolution_ctx: NameResolutionContext,
    ) -> bool {
        let mut alias_collector = MaterializedCteAliasCollector {
            cte_names,
            aliases: HashMap::new(),
            name_resolution_ctx,
        };
        if body.walk(&mut alias_collector).is_err() || alias_collector.aliases.is_empty() {
            return false;
        }

        rewriter.ctes = ctes;
        rewriter.table_aliases = alias_collector.aliases;
        body.walk_mut(rewriter).is_ok()
    }
}
