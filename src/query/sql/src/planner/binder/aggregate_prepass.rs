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

use std::collections::BTreeSet;
use std::collections::HashSet;

use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::OrderByExpr;
use databend_common_ast::ast::Query;
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::VisitResult;
use databend_common_ast::visit::Visitor;
use databend_common_ast::visit::Walk;
use databend_common_base::runtime::block_on;
use databend_common_exception::Result;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_functions::is_builtin_function;
use databend_common_meta_app::principal::UDFDefinition;
use databend_common_users::UserApiProvider;

use super::ExprContext;
use crate::BindContext;
use crate::NameResolutionContext;
use crate::binder::Binder;
use crate::binder::select::SelectAliasCatalog;
use crate::binder::select::SelectList;
use crate::normalize_identifier;
use crate::plans::ScalarExpr;

macro_rules! try_ast_walk {
    ($expr:expr) => {
        match $expr? {
            VisitControl::Continue | VisitControl::SkipChildren => {}
            VisitControl::Break(value) => return Ok(VisitControl::Break(value)),
        }
    };
}

#[derive(Debug, Clone, PartialEq, Eq, enum_as_inner::EnumAsInner)]
pub(super) enum AggregatePrepassSource {
    DirectClause,
    AliasExpansion(String),
}

fn is_aggregate_target(
    name_resolution_ctx: &NameResolutionContext,
    udaf_names: &HashSet<String>,
    func: &FunctionCall,
) -> bool {
    if func.window.is_some() {
        return false;
    }

    let func_name = normalize_identifier(&func.name, name_resolution_ctx).name;
    AggregateFunctionFactory::instance().contains(func_name.as_str())
        || func_name.eq_ignore_ascii_case("grouping")
        || udaf_names.contains(func_name.as_str())
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct AggregatePrepassFact {
    pub expr_context: ExprContext,
    pub source: AggregatePrepassSource,
    pub expr: Expr,
    pub contains_window: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct ExprFlags {
    contains_aggregate: bool,
    contains_window: bool,
    contains_subquery: bool,
}

trait ExprInspection {
    fn observe_top_level_expr(&mut self, _expr: &Expr) {}

    fn observe_top_level_column_ref(&mut self, _column: &ColumnRef) {}

    fn mark_contains_subquery(&mut self) {}

    fn walk_expr(&mut self, expr: &Expr)
    where Self: std::marker::Sized {
        expr.walk(&mut ExprWalker {
            inspection: self,
            in_subquery: false,
        })
        .unwrap();
    }
}

impl<L: ExprInspection, R: ExprInspection> ExprInspection for (L, R) {
    fn observe_top_level_expr(&mut self, expr: &Expr) {
        self.0.observe_top_level_expr(expr);
        self.1.observe_top_level_expr(expr);
    }

    fn observe_top_level_column_ref(&mut self, column: &ColumnRef) {
        self.0.observe_top_level_column_ref(column);
        self.1.observe_top_level_column_ref(column);
    }

    fn mark_contains_subquery(&mut self) {
        self.0.mark_contains_subquery();
        self.1.mark_contains_subquery();
    }
}

struct ExprWalker<'a, T> {
    inspection: &'a mut T,
    in_subquery: bool,
}

impl<T> Visitor for ExprWalker<'_, T>
where T: ExprInspection
{
    fn visit_expr(&mut self, expr: &Expr) -> VisitResult {
        if !self.in_subquery {
            self.inspection.observe_top_level_expr(expr);
            if let Expr::ColumnRef { column, .. } = expr {
                self.inspection.observe_top_level_column_ref(column);
            }
        }
        Ok(VisitControl::Continue)
    }

    fn visit_query(&mut self, _query: &Query) -> VisitResult {
        self.inspection.mark_contains_subquery();
        if self.in_subquery {
            return Ok(VisitControl::SkipChildren);
        }
        self.in_subquery = true;
        Ok(VisitControl::Continue)
    }

    fn leave_query(&mut self, _query: &Query) -> std::result::Result<(), !> {
        self.in_subquery = false;
        Ok(())
    }
}

struct ExprFlagsProbe<'a> {
    name_resolution_ctx: &'a NameResolutionContext,
    udaf_names: &'a HashSet<String>,
    result: ExprFlags,
}

impl ExprInspection for ExprFlagsProbe<'_> {
    fn observe_top_level_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::CountAll { window: None, .. } => self.result.contains_aggregate = true,
            Expr::FunctionCall { func, .. }
                if is_aggregate_target(self.name_resolution_ctx, self.udaf_names, func) =>
            {
                self.result.contains_aggregate = true
            }
            _ if is_window_expr(expr) => {
                self.result.contains_window = true;
            }
            _ => {}
        }
    }

    fn mark_contains_subquery(&mut self) {
        self.result.contains_subquery = true;
    }
}

struct ReferencedAliasProbe<'a> {
    name_resolution_ctx: &'a NameResolutionContext,
    aliases: &'a HashSet<&'a str>,
    referenced_aliases: BTreeSet<String>,
}

impl ExprInspection for ReferencedAliasProbe<'_> {
    fn observe_top_level_column_ref(&mut self, column: &ColumnRef) {
        let Some(alias) = resolve_unqualified_alias_name(self.name_resolution_ctx, column) else {
            return;
        };

        if self.aliases.contains(alias.as_str()) {
            self.referenced_aliases.insert(alias);
        }
    }
}

struct FunctionNameProbe<'a> {
    name_resolution_ctx: &'a NameResolutionContext,
    names: BTreeSet<String>,
}

impl ExprInspection for FunctionNameProbe<'_> {
    fn observe_top_level_expr(&mut self, expr: &Expr) {
        let Expr::FunctionCall { func, .. } = expr else {
            return;
        };

        if func.window.is_some() {
            return;
        }

        self.names
            .insert(normalize_identifier(&func.name, self.name_resolution_ctx).name);
    }
}

pub(super) fn collect_function_names(
    name_resolution_ctx: &NameResolutionContext,
    expr: &Expr,
) -> BTreeSet<String> {
    let mut probe = FunctionNameProbe {
        name_resolution_ctx,
        names: BTreeSet::new(),
    };
    probe.walk_expr(expr);
    probe.names
}

fn is_window_expr(expr: &Expr) -> bool {
    match expr {
        Expr::CountAll {
            window: Some(_), ..
        } => true,
        Expr::FunctionCall { func, .. } => func.window.is_some(),
        _ => false,
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct AggregatePrepassFacts {
    items: Vec<AggregatePrepassFact>,
    direct_len: usize,
}

impl AggregatePrepassFacts {
    fn insert_prioritized_unique(&mut self, fact: AggregatePrepassFact) {
        if fact.source.is_direct_clause() {
            if self.items[..self.direct_len].contains(&fact) {
                return;
            }
            self.items.push(fact);
            let last = self.items.len() - 1;
            self.items.swap(self.direct_len, last);
            self.direct_len += 1;
        } else {
            if self.items[self.direct_len..].contains(&fact) {
                return;
            }
            self.items.push(fact);
        };
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AggregateExprInfo {
    pub ast: Expr,
    pub contains_aggregate: bool,
    pub contains_window: bool,
    #[allow(dead_code)]
    pub contains_subquery: bool,
    pub referenced_aliases: Vec<String>,
}

impl AggregateExprInfo {
    pub(crate) fn analyze(
        name_resolution_ctx: &NameResolutionContext,
        udaf_names: &HashSet<String>,
        aliases: &HashSet<&str>,
        expr: &Expr,
    ) -> Self {
        let mut probes = (
            ExprFlagsProbe {
                name_resolution_ctx,
                udaf_names,
                result: ExprFlags::default(),
            },
            ReferencedAliasProbe {
                name_resolution_ctx,
                aliases,
                referenced_aliases: BTreeSet::new(),
            },
        );
        probes.walk_expr(expr);
        let (expr_flags_probe, alias_probe) = probes;

        Self {
            ast: expr.clone(),
            contains_aggregate: expr_flags_probe.result.contains_aggregate,
            contains_window: expr_flags_probe.result.contains_window,
            contains_subquery: expr_flags_probe.result.contains_subquery,
            referenced_aliases: alias_probe.referenced_aliases.into_iter().collect(),
        }
    }
}

struct Scanner<'a> {
    expr_context: ExprContext,
    name_resolution_ctx: &'a NameResolutionContext,
    udaf_names: &'a HashSet<String>,
    ast_aliases: &'a SelectAliasCatalog,
    in_subquery: bool,
    window_depth: usize,
    expanding_aliases: HashSet<String>,
    expansion_stack: Vec<String>,
    facts: Vec<AggregatePrepassFact>,
}

impl Scanner<'_> {
    fn scan(
        expr_context: ExprContext,
        name_resolution_ctx: &NameResolutionContext,
        udaf_names: &HashSet<String>,
        ast_aliases: &SelectAliasCatalog,
        expr: &Expr,
    ) -> Vec<AggregatePrepassFact> {
        let mut scanner = Scanner {
            expr_context,
            name_resolution_ctx,
            udaf_names,
            ast_aliases,
            in_subquery: false,
            window_depth: 0,
            expanding_aliases: HashSet::new(),
            expansion_stack: Vec::new(),
            facts: Vec::new(),
        };
        expr.walk(&mut scanner).unwrap();
        scanner.facts
    }

    fn visit_window_expr_children(&mut self, expr: &Expr) -> VisitResult {
        match expr {
            Expr::CountAll {
                window, qualified, ..
            } => {
                for item in qualified {
                    if let databend_common_ast::ast::Indirection::Identifier(ident) = item {
                        try_ast_walk!(ident.walk(self));
                    }
                }
                if let Some(window) = window {
                    try_ast_walk!(window.walk(self));
                }
            }
            Expr::FunctionCall { func, .. } => {
                try_ast_walk!(func.walk(self));
            }
            _ => unreachable!("window expr helper must only be called for window exprs"),
        }

        Ok(VisitControl::Continue)
    }

    fn handle_column_ref(&mut self, column: &ColumnRef) {
        if self.in_subquery || self.window_depth > 0 {
            return;
        }

        let Some((alias, alias_expr)) =
            Self::find_aggregate_prepass_alias(self.name_resolution_ctx, column, self.ast_aliases)
        else {
            return;
        };

        if self.expanding_aliases.insert(alias.clone()) {
            self.expansion_stack.push(alias.clone());
            let _ = alias_expr.walk(self);
            self.expansion_stack.pop();
            self.expanding_aliases.remove(&alias);
        }
    }

    fn enter_expr(&mut self, expr: &Expr) {
        if is_window_expr(expr) {
            self.window_depth += 1;
        }

        if self.window_depth > 0 || self.in_subquery {
            return;
        }

        if let Some(fact) = match expr {
            Expr::CountAll { window: None, .. } => self.build_fact(expr),
            Expr::FunctionCall { func, .. }
                if is_aggregate_target(self.name_resolution_ctx, self.udaf_names, func) =>
            {
                self.build_fact(expr)
            }
            _ => None,
        } {
            self.facts.push(fact);
        }
    }

    fn build_fact(&self, expr: &Expr) -> Option<AggregatePrepassFact> {
        let mut probe = ExprFlagsProbe {
            name_resolution_ctx: self.name_resolution_ctx,
            udaf_names: self.udaf_names,
            result: ExprFlags::default(),
        };
        probe.walk_expr(expr);
        if probe.result.contains_subquery {
            return None;
        }

        Some(AggregatePrepassFact {
            expr_context: self.expr_context,
            source: self.current_source(),
            expr: expr.clone(),
            contains_window: probe.result.contains_window,
        })
    }

    fn current_source(&self) -> AggregatePrepassSource {
        match self.expansion_stack.first() {
            Some(alias) => AggregatePrepassSource::AliasExpansion(alias.clone()),
            None => AggregatePrepassSource::DirectClause,
        }
    }

    fn find_aggregate_prepass_alias<'a>(
        name_resolution_ctx: &NameResolutionContext,
        column: &ColumnRef,
        ast_aliases: &'a SelectAliasCatalog,
    ) -> Option<(String, &'a Expr)> {
        let alias = resolve_unqualified_alias_name(name_resolution_ctx, column)?;
        let ast = &ast_aliases.unique_aggregate_prepass_expr_info(&alias)?.ast;
        Some((alias, ast))
    }
}

impl Visitor for Scanner<'_> {
    fn visit_expr(&mut self, expr: &Expr) -> VisitResult {
        self.enter_expr(expr);

        if let Expr::ColumnRef { column, .. } = expr {
            self.handle_column_ref(column);
        }

        if is_window_expr(expr) {
            let result = self.visit_window_expr_children(expr);
            self.window_depth -= 1;
            result?;
            return Ok(VisitControl::SkipChildren);
        }

        Ok(VisitControl::Continue)
    }

    fn visit_query(&mut self, _query: &Query) -> VisitResult {
        if self.in_subquery {
            return Ok(VisitControl::SkipChildren);
        }
        self.in_subquery = true;
        Ok(VisitControl::Continue)
    }

    fn leave_query(&mut self, _query: &Query) -> std::result::Result<(), !> {
        self.in_subquery = false;
        Ok(())
    }
}

fn resolve_unqualified_alias_name(
    name_resolution_ctx: &NameResolutionContext,
    column: &ColumnRef,
) -> Option<String> {
    if column.database.is_some() || column.table.is_some() {
        return None;
    }

    let ColumnID::Name(ident) = &column.column else {
        return None;
    };

    Some(normalize_identifier(ident, name_resolution_ctx).name)
}

impl Binder {
    pub(super) fn bind_aggregate_prepass_facts(
        &mut self,
        bind_context: &mut BindContext,
        aliases: &[(String, ScalarExpr)],
        facts: &AggregatePrepassFacts,
    ) -> Result<()> {
        for fact in &facts.items {
            self.bind_and_rewrite_aggregate_expr(
                bind_context,
                aliases,
                fact.expr_context,
                &fact.expr,
            )?;
        }

        Ok(())
    }

    pub(super) fn find_and_load_udaf(
        &self,
        bind_context: &BindContext,
        select_list: &SelectList<'_>,
        having: Option<&Expr>,
        qualify: Option<&Expr>,
        order_by: &[OrderByExpr],
    ) -> Result<HashSet<String>> {
        let mut function_names = BTreeSet::new();
        function_names.extend(
            select_list
                .items
                .iter()
                .flat_map(|item| item.source_function_names.iter().cloned()),
        );
        function_names.extend(
            having
                .into_iter()
                .chain(qualify)
                .chain(order_by.iter().map(|order| &order.expr))
                .flat_map(|expr| collect_function_names(&self.name_resolution_ctx, expr)),
        );
        self.resolve_udaf_names(bind_context, function_names)
    }

    fn resolve_udaf_names(
        &self,
        bind_context: &BindContext,
        function_names: BTreeSet<String>,
    ) -> Result<HashSet<String>> {
        let mut udaf_names = HashSet::new();
        let tenant = self.ctx.get_tenant();
        let provider = UserApiProvider::instance();

        for name in function_names {
            if name.eq_ignore_ascii_case("grouping") || is_builtin_function(&name) {
                continue;
            }

            let udf = if let Some(udf) = bind_context.udf_cache.read().get(&name).cloned() {
                udf
            } else {
                let udf = block_on(provider.get_udf(&tenant, &name))?;
                bind_context
                    .udf_cache
                    .write()
                    .insert(name.clone(), udf.clone());
                udf
            };

            if let Some(udf) = udf
                && matches!(udf.definition, UDFDefinition::UDAFScript(_))
            {
                udaf_names.insert(name);
            }
        }

        Ok(udaf_names)
    }

    pub(super) fn derive_aggregate_prepass_facts(
        &self,
        udaf_names: &HashSet<String>,
        aliases: &SelectAliasCatalog,
        ast_iter: impl Iterator<Item = (Expr, ExprContext)>,
    ) -> AggregatePrepassFacts {
        ast_iter
            .flat_map(|(ast_expr, expr_context)| {
                Scanner::scan(
                    expr_context,
                    &self.name_resolution_ctx,
                    udaf_names,
                    aliases,
                    &ast_expr,
                )
            })
            .fold(AggregatePrepassFacts::default(), |mut facts, fact| {
                facts.insert_prioritized_unique(fact);
                facts
            })
    }
}

#[cfg(test)]
mod tests {
    use NameResolutionContext;
    use databend_common_ast::ast::Identifier;
    use databend_common_ast::ast::IdentifierType;
    use databend_common_ast::ast::SelectTarget;
    use databend_common_ast::parser::Dialect;
    use databend_common_ast::parser::parse_expr;
    use databend_common_ast::parser::tokenize_sql;
    use databend_common_expression::Scalar;

    use super::*;
    use crate::binder::select::SelectItem;
    use crate::binder::select::SelectList;
    use crate::plans::ConstantExpr;

    fn parse_ast_expr(text: &str) -> Expr {
        let tokens = tokenize_sql(text).unwrap();
        parse_expr(&tokens, Dialect::PostgreSQL).unwrap()
    }

    struct AggregatePrepassTestBed {
        name_resolution_ctx: NameResolutionContext,
        udaf_names: HashSet<String>,
    }

    impl AggregatePrepassTestBed {
        fn new() -> Self {
            Self {
                name_resolution_ctx: NameResolutionContext::default(),
                udaf_names: HashSet::new(),
            }
        }

        fn catalog(&self, aliases: &[(&str, &str)]) -> SelectAliasCatalog {
            let aliases = aliases
                .iter()
                .map(|(name, expr)| ((*name).to_string(), parse_ast_expr(expr)))
                .collect();
            self.build_alias_catalog(aliases)
        }

        fn scan(
            &self,
            expr_context: ExprContext,
            aliases: &[(&str, &str)],
            expr: &str,
        ) -> Vec<AggregatePrepassFact> {
            let aliases = self.catalog(aliases);
            Scanner::scan(
                expr_context,
                &self.name_resolution_ctx,
                &self.udaf_names,
                &aliases,
                &parse_ast_expr(expr),
            )
        }

        fn build_alias_catalog(&self, aliases: Vec<(String, Expr)>) -> SelectAliasCatalog {
            let select_targets = aliases
                .into_iter()
                .map(|(name, expr)| SelectTarget::AliasedExpr {
                    expr: Box::new(expr),
                    alias: Some(Identifier {
                        span: None,
                        name,
                        quote: None,
                        ident_type: IdentifierType::None,
                    }),
                })
                .collect::<Vec<_>>();

            let select_list = SelectList {
                items: select_targets
                    .iter()
                    .map(|select_target| {
                        let SelectTarget::AliasedExpr {
                            alias: Some(alias), ..
                        } = select_target
                        else {
                            unreachable!("aggregate prepass test helper only builds aliased exprs");
                        };

                        SelectItem {
                            select_target,
                            scalar: ConstantExpr {
                                span: None,
                                value: Scalar::Null,
                            }
                            .into(),
                            alias: alias.name.clone(),
                            source_function_names: Default::default(),
                        }
                    })
                    .collect(),
            };

            let mut catalog = select_list.alias_catalog();
            catalog.analyze_aggregate_prepass_exprs(
                &select_list,
                &self.name_resolution_ctx,
                &self.udaf_names,
            );
            catalog
        }
    }

    #[test]
    fn aggregate_prepass_alias_catalog_tracks_features_and_refs() {
        let test_bed = AggregatePrepassTestBed::new();
        let aliases = test_bed.catalog(&[
            ("s", "sum(number)"),
            ("rn", "row_number() OVER (ORDER BY s)"),
            ("sub", "(SELECT max(number) FROM t)"),
        ]);

        let sum_alias = aliases.unique_aggregate_prepass_expr_info("s").unwrap();
        assert!(sum_alias.contains_aggregate);
        assert!(!sum_alias.contains_window);
        assert!(!sum_alias.contains_subquery);
        assert!(sum_alias.referenced_aliases.is_empty());

        let window_alias = aliases.unique_aggregate_prepass_expr_info("rn").unwrap();
        assert!(!window_alias.contains_aggregate);
        assert!(window_alias.contains_window);
        assert!(!window_alias.contains_subquery);
        assert_eq!(window_alias.referenced_aliases, vec!["s".to_string()]);

        let subquery_alias = aliases.unique_aggregate_prepass_expr_info("sub").unwrap();
        assert!(!subquery_alias.contains_aggregate);
        assert!(!subquery_alias.contains_window);
        assert!(subquery_alias.contains_subquery);
        assert!(subquery_alias.referenced_aliases.is_empty());
    }

    #[test]
    fn aggregate_prepass_facts_track_alias_expansion_source() {
        let test_bed = AggregatePrepassTestBed::new();
        let facts = test_bed.scan(ExprContext::HavingClause, &[("s", "sum(number)")], "s > 0");

        assert_eq!(facts.len(), 1);
        assert_eq!(
            facts[0].source,
            AggregatePrepassSource::AliasExpansion("s".to_string())
        );
        assert!(matches!(facts[0].expr_context, ExprContext::HavingClause));
        assert!(!facts[0].contains_window);
    }

    #[test]
    fn aggregate_prepass_duplicate_aliases_do_not_expand() {
        let test_bed = AggregatePrepassTestBed::new();
        let facts = test_bed.scan(
            ExprContext::OrderByClause,
            &[("s", "sum(number)"), ("s", "max(number)")],
            "s > 0",
        );

        assert!(facts.is_empty());
    }

    #[test]
    fn aggregate_prepass_facts_deduplicate_identical_candidates() {
        let test_bed = AggregatePrepassTestBed::new();
        let aliases = test_bed.catalog(&[("s", "sum(number)")]);

        let mut facts = AggregatePrepassFacts::default();
        for fact in Scanner::scan(
            ExprContext::HavingClause,
            &test_bed.name_resolution_ctx,
            &test_bed.udaf_names,
            &aliases,
            &parse_ast_expr("sum(number) > 0"),
        ) {
            facts.insert_prioritized_unique(fact);
        }
        for fact in Scanner::scan(
            ExprContext::HavingClause,
            &test_bed.name_resolution_ctx,
            &test_bed.udaf_names,
            &aliases,
            &parse_ast_expr("sum(number) > 0"),
        ) {
            facts.insert_prioritized_unique(fact);
        }

        assert_eq!(facts.items.len(), 1);
    }

    #[test]
    fn aggregate_prepass_nested_alias_expansion_keeps_alias_source() {
        let test_bed = AggregatePrepassTestBed::new();
        let facts = test_bed.scan(
            ExprContext::HavingClause,
            &[("s", "sum(number)"), ("a", "s + 1")],
            "a > 0",
        );

        assert_eq!(facts.len(), 1);
        assert_eq!(
            facts[0].source,
            AggregatePrepassSource::AliasExpansion("a".to_string())
        );
    }

    #[test]
    fn aggregate_prepass_fact_flags_ignore_deeper_alias_expansion_features() {
        let test_bed = AggregatePrepassTestBed::new();
        let facts = test_bed.scan(
            ExprContext::HavingClause,
            &[("w", "row_number() OVER (ORDER BY number)")],
            "sum(w) > 0",
        );

        assert_eq!(facts.len(), 1);
        assert!(!facts[0].contains_window);
    }

    #[test]
    fn aggregate_prepass_alias_catalog_tracks_transitive_aggregate_and_window_aliases() {
        let test_bed = AggregatePrepassTestBed::new();
        let aliases = test_bed.catalog(&[
            ("s", "sum(number)"),
            ("a", "s + 1"),
            ("rn", "row_number() OVER (ORDER BY number)"),
            ("w", "rn + 1"),
        ]);

        let aggregate_usage = aliases.aggregate_alias_feature(&["a".to_string()]);
        assert!(aggregate_usage.ref_aggregate);
        assert!(!aggregate_usage.ref_window);

        let window_usage = aliases.aggregate_alias_feature(&["w".to_string()]);
        assert!(window_usage.ref_window);
        assert!(!window_usage.ref_aggregate);
    }
}
