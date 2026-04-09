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
use std::collections::BTreeSet;
use std::collections::HashSet;

use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::OrderByExpr;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SelectTarget;
use databend_common_base::runtime::block_on;
use databend_common_exception::Result;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_functions::is_builtin_function;
use databend_common_meta_app::principal::UDFDefinition;
use databend_common_users::UserApiProvider;
use derive_visitor::Drive;
use derive_visitor::Visitor;

use super::ExprContext;
use crate::BindContext;
use crate::NameResolutionContext;
use crate::binder::Binder;
use crate::binder::select::SelectList;
use crate::normalize_identifier;
use crate::plans::ScalarExpr;

#[derive(Debug, Clone, PartialEq, Eq, enum_as_inner::EnumAsInner)]
pub(super) enum AggregatePrepassSource {
    DirectClause,
    AliasExpansion(String),
}

fn is_aggregate_target(
    name_resolution_ctx: &NameResolutionContext,
    udaf_names: &HashSet<String>,
    func: &ASTFunctionCall,
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

trait AstProbe {
    fn enter_expr(&mut self, _expr: &Expr, _query_depth: usize) {}

    fn enter_column_ref(&mut self, _column: &ColumnRef, _query_depth: usize) {}

    fn enter_query(&mut self, _query: &Query, _query_depth: usize) {}
}

impl<L: AstProbe, R: AstProbe> AstProbe for (L, R) {
    fn enter_expr(&mut self, expr: &Expr, query_depth: usize) {
        self.0.enter_expr(expr, query_depth);
        self.1.enter_expr(expr, query_depth);
    }

    fn enter_column_ref(&mut self, column: &ColumnRef, query_depth: usize) {
        self.0.enter_column_ref(column, query_depth);
        self.1.enter_column_ref(column, query_depth);
    }

    fn enter_query(&mut self, query: &Query, query_depth: usize) {
        self.0.enter_query(query, query_depth);
        self.1.enter_query(query, query_depth);
    }
}

#[derive(Visitor)]
#[visitor(Expr(enter), ColumnRef(enter), Query)]
struct ProbeWalker<'a> {
    probe: &'a mut dyn AstProbe,
    query_depth: usize,
}

impl ProbeWalker<'_> {
    fn enter_expr(&mut self, expr: &Expr) {
        self.probe.enter_expr(expr, self.query_depth);
    }

    fn enter_column_ref(&mut self, column: &ColumnRef) {
        self.probe.enter_column_ref(column, self.query_depth);
    }

    fn enter_query(&mut self, query: &Query) {
        self.probe.enter_query(query, self.query_depth);
        self.query_depth += 1;
    }

    fn exit_query(&mut self, _query: &Query) {
        self.query_depth -= 1;
    }
}

fn walk_expr_with_probe(expr: &Expr, probe: &mut dyn AstProbe) {
    let mut walker = ProbeWalker {
        probe,
        query_depth: 0,
    };
    expr.drive(&mut walker);
}

struct ExprFlagsProbe<'a> {
    name_resolution_ctx: &'a NameResolutionContext,
    udaf_names: &'a HashSet<String>,
    result: ExprFlags,
}

impl AstProbe for ExprFlagsProbe<'_> {
    fn enter_expr(&mut self, expr: &Expr, query_depth: usize) {
        if query_depth > 0 {
            return;
        }

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

    fn enter_query(&mut self, _query: &Query, _query_depth: usize) {
        self.result.contains_subquery = true;
    }
}

struct ReferencedAliasProbe<'a> {
    name_resolution_ctx: &'a NameResolutionContext,
    aliases: &'a HashSet<&'a str>,
    referenced_aliases: BTreeSet<String>,
}

impl AstProbe for ReferencedAliasProbe<'_> {
    fn enter_column_ref(&mut self, column: &ColumnRef, query_depth: usize) {
        if query_depth > 0 {
            return;
        }

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

impl AstProbe for FunctionNameProbe<'_> {
    fn enter_expr(&mut self, expr: &Expr, query_depth: usize) {
        if query_depth > 0 {
            return;
        }

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
pub(super) struct AggregatePrepassExprInfo {
    pub ast: Expr,
    pub contains_aggregate: bool,
    pub contains_window: bool,
    #[allow(dead_code)]
    pub contains_subquery: bool,
    pub referenced_aliases: Vec<String>,
}

impl AggregatePrepassExprInfo {
    pub(super) fn analyze(
        name_resolution_ctx: &NameResolutionContext,
        udaf_names: &HashSet<String>,
        aliases: &HashSet<&str>,
        expr: &Expr,
    ) -> Self {
        let mut probe = (
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
        walk_expr_with_probe(expr, &mut probe);

        Self {
            ast: expr.clone(),
            contains_aggregate: probe.0.result.contains_aggregate,
            contains_window: probe.0.result.contains_window,
            contains_subquery: probe.0.result.contains_subquery,
            referenced_aliases: probe.1.referenced_aliases.into_iter().collect(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub(super) struct AggregatePrepassAliasCatalog {
    items: Vec<AggregatePrepassExprInfo>,
    by_name: BTreeMap<String, Vec<usize>>,
}

impl AggregatePrepassAliasCatalog {
    pub(super) fn new(
        name_resolution_ctx: &NameResolutionContext,
        udaf_names: &HashSet<String>,
        aliases: Vec<(String, Expr)>,
    ) -> Self {
        let mut by_name: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for (index, (name, _)) in aliases.iter().enumerate() {
            by_name.entry(name.clone()).or_default().push(index);
        }

        let alias_names = by_name.keys().map(String::as_str).collect();
        let items = aliases
            .into_iter()
            .map(|(_, ast)| {
                AggregatePrepassExprInfo::analyze(
                    name_resolution_ctx,
                    udaf_names,
                    &alias_names,
                    &ast,
                )
            })
            .collect();

        Self { items, by_name }
    }

    fn get_unique(&self, name: &str) -> Option<&AggregatePrepassExprInfo> {
        if let [index] = self.by_name.get(name)?.as_slice() {
            Some(&self.items[*index])
        } else {
            None
        }
    }

    #[cfg(test)]
    fn items(&self) -> &[AggregatePrepassExprInfo] {
        &self.items
    }

    pub(super) fn alias_names(&self) -> HashSet<&str> {
        self.by_name.keys().map(String::as_str).collect()
    }

    pub(super) fn references_aliases_matching<F>(&self, names: &[String], predicate: &F) -> bool
    where F: Fn(&AggregatePrepassExprInfo) -> bool {
        names
            .iter()
            .any(|name| self.alias_reaches(name, predicate, &mut BTreeSet::new()))
    }

    fn alias_reaches<F>(&self, name: &str, predicate: &F, visiting: &mut BTreeSet<String>) -> bool
    where F: Fn(&AggregatePrepassExprInfo) -> bool {
        let Some(alias) = self.get_unique(name) else {
            return false;
        };

        if predicate(alias) {
            return true;
        }

        if !visiting.insert(name.to_string()) {
            return false;
        }

        let reached = alias
            .referenced_aliases
            .iter()
            .any(|dep| self.alias_reaches(dep, predicate, visiting));
        visiting.remove(name);
        reached
    }
}

#[derive(Visitor)]
#[visitor(Expr, ColumnRef(enter), Query)]
struct Scanner<'a> {
    expr_context: ExprContext,
    name_resolution_ctx: &'a NameResolutionContext,
    udaf_names: &'a HashSet<String>,
    ast_aliases: &'a AggregatePrepassAliasCatalog,
    query_depth: usize,
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
        ast_aliases: &AggregatePrepassAliasCatalog,
        expr: &Expr,
    ) -> Vec<AggregatePrepassFact> {
        let mut scanner = Scanner {
            expr_context,
            name_resolution_ctx,
            udaf_names,
            ast_aliases,
            query_depth: 0,
            window_depth: 0,
            expanding_aliases: HashSet::new(),
            expansion_stack: Vec::new(),
            facts: Vec::new(),
        };
        expr.drive(&mut scanner);
        scanner.facts
    }

    fn enter_expr(&mut self, expr: &Expr) {
        if is_window_expr(expr) {
            self.window_depth += 1;
        }

        if self.window_depth > 0 || self.query_depth > 0 {
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

    fn exit_expr(&mut self, expr: &Expr) {
        if is_window_expr(expr) {
            self.window_depth -= 1;
        }
    }

    fn enter_column_ref(&mut self, column: &ColumnRef) {
        if self.query_depth > 0 || self.window_depth > 0 {
            return;
        }

        let Some((alias, alias_expr)) =
            Self::find_aggregate_prepass_alias(self.name_resolution_ctx, column, self.ast_aliases)
        else {
            return;
        };

        if self.expanding_aliases.insert(alias.clone()) {
            self.expansion_stack.push(alias.clone());
            alias_expr.drive(self);
            self.expansion_stack.pop();
            self.expanding_aliases.remove(&alias);
        }
    }

    fn enter_query(&mut self, _query: &Query) {
        self.query_depth += 1;
    }

    fn exit_query(&mut self, _query: &Query) {
        self.query_depth -= 1;
    }

    fn build_fact(&self, expr: &Expr) -> Option<AggregatePrepassFact> {
        let mut probe = ExprFlagsProbe {
            name_resolution_ctx: self.name_resolution_ctx,
            udaf_names: self.udaf_names,
            result: ExprFlags::default(),
        };
        walk_expr_with_probe(expr, &mut probe);
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
        ast_aliases: &'a AggregatePrepassAliasCatalog,
    ) -> Option<(String, &'a Expr)> {
        let alias = resolve_unqualified_alias_name(name_resolution_ctx, column)?;
        let ast = &ast_aliases.get_unique(&alias)?.ast;
        Some((alias, ast))
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
    pub(super) fn collect_aggregate_prepass_aliases<'a>(
        &self,
        udaf_names: &HashSet<String>,
        select_list: &'a SelectList<'a>,
    ) -> AggregatePrepassAliasCatalog {
        let aliases = select_list
            .items
            .iter()
            .filter_map(|item| match item.select_target {
                SelectTarget::AliasedExpr { expr, .. } => {
                    Some((item.alias.clone(), expr.as_ref().clone()))
                }
                _ => None,
            })
            .collect();
        AggregatePrepassAliasCatalog::new(&self.name_resolution_ctx, udaf_names, aliases)
    }

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
        let mut probe = FunctionNameProbe {
            name_resolution_ctx: &self.name_resolution_ctx,
            names: BTreeSet::new(),
        };

        for expr in select_list
            .items
            .iter()
            .filter_map(|item| {
                if let SelectTarget::AliasedExpr { box expr, .. } = item.select_target {
                    Some(expr)
                } else {
                    None
                }
            })
            .chain(having)
            .chain(qualify)
            .chain(order_by.iter().map(|order| &order.expr))
        {
            walk_expr_with_probe(expr, &mut probe);
        }

        self.resolve_udaf_names(bind_context, probe.names)
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

    pub(super) fn derive_aggregate_prepass_facts<'a>(
        &self,
        udaf_names: &HashSet<String>,
        aliases: &AggregatePrepassAliasCatalog,
        ast_iter: impl Iterator<Item = (&'a Expr, ExprContext)>,
    ) -> AggregatePrepassFacts {
        ast_iter
            .flat_map(|(ast_expr, expr_context)| {
                Scanner::scan(
                    expr_context,
                    &self.name_resolution_ctx,
                    udaf_names,
                    aliases,
                    ast_expr,
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
    use databend_common_ast::parser::Dialect;
    use databend_common_ast::parser::parse_expr;
    use databend_common_ast::parser::tokenize_sql;

    use super::*;

    fn parse_ast_expr(text: &str) -> Expr {
        let tokens = tokenize_sql(text).unwrap();
        parse_expr(&tokens, Dialect::PostgreSQL).unwrap()
    }

    #[test]
    fn aggregate_prepass_alias_catalog_tracks_features_and_refs() {
        let name_resolution_ctx = NameResolutionContext::default();
        let udaf_names = HashSet::new();
        let aliases = AggregatePrepassAliasCatalog::new(&name_resolution_ctx, &udaf_names, vec![
            ("s".to_string(), parse_ast_expr("sum(number)")),
            (
                "rn".to_string(),
                parse_ast_expr("row_number() OVER (ORDER BY s)"),
            ),
            (
                "sub".to_string(),
                parse_ast_expr("(SELECT max(number) FROM t)"),
            ),
        ]);

        let items = aliases.items();
        assert_eq!(items.len(), 3);

        assert!(items[0].contains_aggregate);
        assert!(!items[0].contains_window);
        assert!(!items[0].contains_subquery);
        assert!(items[0].referenced_aliases.is_empty());

        assert!(!items[1].contains_aggregate);
        assert!(items[1].contains_window);
        assert!(!items[1].contains_subquery);
        assert_eq!(items[1].referenced_aliases, vec!["s".to_string()]);

        assert!(!items[2].contains_aggregate);
        assert!(!items[2].contains_window);
        assert!(items[2].contains_subquery);
        assert!(items[2].referenced_aliases.is_empty());
    }

    #[test]
    fn aggregate_prepass_facts_track_alias_expansion_source() {
        let name_resolution_ctx = NameResolutionContext::default();
        let udaf_names = HashSet::new();
        let aliases = AggregatePrepassAliasCatalog::new(&name_resolution_ctx, &udaf_names, vec![(
            "s".to_string(),
            parse_ast_expr("sum(number)"),
        )]);

        let facts = Scanner::scan(
            ExprContext::HavingClause,
            &name_resolution_ctx,
            &udaf_names,
            &aliases,
            &parse_ast_expr("s > 0"),
        );

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
        let name_resolution_ctx = NameResolutionContext::default();
        let udaf_names = HashSet::new();
        let aliases = AggregatePrepassAliasCatalog::new(&name_resolution_ctx, &udaf_names, vec![
            ("s".to_string(), parse_ast_expr("sum(number)")),
            ("s".to_string(), parse_ast_expr("max(number)")),
        ]);

        let facts = Scanner::scan(
            ExprContext::OrderByClause,
            &name_resolution_ctx,
            &udaf_names,
            &aliases,
            &parse_ast_expr("s > 0"),
        );

        assert!(facts.is_empty());
    }

    #[test]
    fn aggregate_prepass_facts_deduplicate_identical_candidates() {
        let name_resolution_ctx = NameResolutionContext::default();
        let udaf_names = HashSet::new();
        let aliases = AggregatePrepassAliasCatalog::new(&name_resolution_ctx, &udaf_names, vec![(
            "s".to_string(),
            parse_ast_expr("sum(number)"),
        )]);

        let mut facts = AggregatePrepassFacts::default();
        for fact in Scanner::scan(
            ExprContext::HavingClause,
            &name_resolution_ctx,
            &udaf_names,
            &aliases,
            &parse_ast_expr("sum(number) > 0"),
        ) {
            facts.insert_prioritized_unique(fact);
        }
        for fact in Scanner::scan(
            ExprContext::HavingClause,
            &name_resolution_ctx,
            &udaf_names,
            &aliases,
            &parse_ast_expr("sum(number) > 0"),
        ) {
            facts.insert_prioritized_unique(fact);
        }

        assert_eq!(facts.items.len(), 1);
    }

    #[test]
    fn aggregate_prepass_nested_alias_expansion_keeps_alias_source() {
        let name_resolution_ctx = NameResolutionContext::default();
        let udaf_names = HashSet::new();
        let aliases = AggregatePrepassAliasCatalog::new(&name_resolution_ctx, &udaf_names, vec![
            ("s".to_string(), parse_ast_expr("sum(number)")),
            ("a".to_string(), parse_ast_expr("s + 1")),
        ]);

        let facts = Scanner::scan(
            ExprContext::HavingClause,
            &name_resolution_ctx,
            &udaf_names,
            &aliases,
            &parse_ast_expr("a > 0"),
        );

        assert_eq!(facts.len(), 1);
        assert_eq!(
            facts[0].source,
            AggregatePrepassSource::AliasExpansion("a".to_string())
        );
    }

    #[test]
    fn aggregate_prepass_fact_flags_ignore_deeper_alias_expansion_features() {
        let name_resolution_ctx = NameResolutionContext::default();
        let udaf_names = HashSet::new();
        let aliases = AggregatePrepassAliasCatalog::new(&name_resolution_ctx, &udaf_names, vec![(
            "w".to_string(),
            parse_ast_expr("row_number() OVER (ORDER BY number)"),
        )]);

        let facts = Scanner::scan(
            ExprContext::HavingClause,
            &name_resolution_ctx,
            &udaf_names,
            &aliases,
            &parse_ast_expr("sum(w) > 0"),
        );

        assert_eq!(facts.len(), 1);
        assert!(!facts[0].contains_window);
    }

    #[test]
    fn aggregate_prepass_alias_catalog_tracks_transitive_aggregate_and_window_aliases() {
        let name_resolution_ctx = NameResolutionContext::default();
        let udaf_names = HashSet::new();
        let aliases = AggregatePrepassAliasCatalog::new(&name_resolution_ctx, &udaf_names, vec![
            ("s".to_string(), parse_ast_expr("sum(number)")),
            ("a".to_string(), parse_ast_expr("s + 1")),
            (
                "rn".to_string(),
                parse_ast_expr("row_number() OVER (ORDER BY number)"),
            ),
            ("w".to_string(), parse_ast_expr("rn + 1")),
        ]);

        assert!(
            &aliases.references_aliases_matching(&["a".to_string()], &|alias| {
                alias.contains_aggregate
            })
        );
        assert!(
            !aliases.references_aliases_matching(&["a".to_string()], &|alias| {
                alias.contains_window
            })
        );
        assert!(
            aliases.references_aliases_matching(&["w".to_string()], &|alias| {
                alias.contains_window
            })
        );
        assert!(
            !aliases.references_aliases_matching(&["w".to_string()], &|alias| {
                alias.contains_aggregate
            })
        );
    }
}
