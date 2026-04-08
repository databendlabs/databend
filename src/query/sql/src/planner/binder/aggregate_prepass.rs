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

#[derive(Debug, Clone, Copy, Default)]
struct AggregatePrepassExprFeatures {
    pub contains_aggregate: bool,
    pub contains_window: bool,
    pub contains_subquery: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, enum_as_inner::EnumAsInner)]
pub(super) enum AggregatePrepassSource {
    DirectClause,
    AliasExpansion(String),
}

#[derive(Debug, Clone, Default)]
pub(super) struct AggregatePrepassFunctionCatalog {
    udaf_names: HashSet<String>,
}

impl AggregatePrepassFunctionCatalog {
    fn is_aggregate_target(
        &self,
        name_resolution_ctx: &NameResolutionContext,
        func: &ASTFunctionCall,
    ) -> bool {
        if func.window.is_some() {
            return false;
        }

        let func_name = normalize_identifier(&func.name, name_resolution_ctx).name;
        AggregateFunctionFactory::instance().contains(func_name.as_str())
            || func_name.eq_ignore_ascii_case("grouping")
            || self.udaf_names.contains(func_name.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct AggregatePrepassFact {
    pub expr_context: ExprContext,
    pub source: AggregatePrepassSource,
    pub expr: Expr,
    pub contains_window: bool,
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
    pub contains_subquery: bool,
    pub referenced_aliases: Vec<String>,
}

impl AggregatePrepassExprInfo {
    pub(super) fn analyze(
        name_resolution_ctx: &NameResolutionContext,
        functions: &AggregatePrepassFunctionCatalog,
        aliases: &HashSet<&str>,
        expr: &Expr,
    ) -> Self {
        let mut feature_visitor = AggregatePrepassExprFeatureVisitor {
            name_resolution_ctx,
            functions,
            query_depth: 0,
            features: Default::default(),
        };
        expr.drive(&mut feature_visitor);

        let mut alias_visitor = ReferencedAliasVisitor {
            name_resolution_ctx,
            aliases,
            query_depth: 0,
            names: BTreeSet::new(),
        };
        expr.drive(&mut alias_visitor);

        let AggregatePrepassExprFeatures {
            contains_aggregate,
            contains_window,
            contains_subquery,
        } = feature_visitor.features;

        Self {
            ast: expr.clone(),
            contains_aggregate,
            contains_window,
            contains_subquery,
            referenced_aliases: alias_visitor.names.into_iter().collect(),
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
        functions: &AggregatePrepassFunctionCatalog,
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
                    functions,
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
#[visitor(Expr(enter), Query)]
struct AggregatePrepassExprFeatureVisitor<'a> {
    name_resolution_ctx: &'a NameResolutionContext,
    functions: &'a AggregatePrepassFunctionCatalog,
    query_depth: usize,
    features: AggregatePrepassExprFeatures,
}

impl AggregatePrepassExprFeatureVisitor<'_> {
    fn enter_expr(&mut self, expr: &Expr) {
        if self.query_depth > 0 {
            return;
        }

        match expr {
            Expr::CountAll { window: None, .. } => self.features.contains_aggregate = true,
            Expr::FunctionCall { func, .. }
                if self
                    .functions
                    .is_aggregate_target(self.name_resolution_ctx, func) =>
            {
                self.features.contains_aggregate = true
            }
            _ if AggregatePrepassScanner::is_window_expr(expr) => {
                self.features.contains_window = true;
            }
            _ => {}
        }
    }

    fn enter_query(&mut self, _query: &Query) {
        self.features.contains_subquery = true;
        self.query_depth += 1;
    }

    fn exit_query(&mut self, _query: &Query) {
        self.query_depth -= 1;
    }
}

#[derive(Visitor)]
#[visitor(Expr(enter), Query)]
struct AggregatePrepassFunctionNameCollector<'a> {
    name_resolution_ctx: &'a NameResolutionContext,
    query_depth: usize,
    names: BTreeSet<String>,
}

impl<'a> AggregatePrepassFunctionNameCollector<'a> {
    fn new(name_resolution_ctx: &'a NameResolutionContext) -> Self {
        Self {
            name_resolution_ctx,
            query_depth: 0,
            names: BTreeSet::new(),
        }
    }

    fn enter_expr(&mut self, expr: &Expr) {
        if self.query_depth > 0 {
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

    fn enter_query(&mut self, _query: &Query) {
        self.query_depth += 1;
    }

    fn exit_query(&mut self, _query: &Query) {
        self.query_depth -= 1;
    }
}

#[derive(Visitor)]
#[visitor(ColumnRef(enter), Query)]
struct ReferencedAliasVisitor<'a> {
    name_resolution_ctx: &'a NameResolutionContext,
    aliases: &'a HashSet<&'a str>,
    query_depth: usize,
    names: BTreeSet<String>,
}

impl ReferencedAliasVisitor<'_> {
    fn enter_column_ref(&mut self, column: &ColumnRef) {
        if self.query_depth > 0 {
            return;
        }

        let Some(alias) = resolve_unqualified_alias_name(self.name_resolution_ctx, column) else {
            return;
        };

        if self.aliases.contains(alias.as_str()) {
            self.names.insert(alias);
        }
    }

    fn enter_query(&mut self, _query: &Query) {
        self.query_depth += 1;
    }

    fn exit_query(&mut self, _query: &Query) {
        self.query_depth -= 1;
    }
}

#[derive(Visitor)]
#[visitor(Expr, ColumnRef(enter), Query)]
struct AggregatePrepassScanner<'a> {
    expr_context: ExprContext,
    name_resolution_ctx: &'a NameResolutionContext,
    functions: &'a AggregatePrepassFunctionCatalog,
    ast_aliases: &'a AggregatePrepassAliasCatalog,
    query_depth: usize,
    window_depth: usize,
    expanding_aliases: HashSet<String>,
    expansion_stack: Vec<String>,
    facts: Vec<AggregatePrepassFact>,
}

impl AggregatePrepassScanner<'_> {
    fn scan(
        expr_context: ExprContext,
        name_resolution_ctx: &NameResolutionContext,
        functions: &AggregatePrepassFunctionCatalog,
        ast_aliases: &AggregatePrepassAliasCatalog,
        expr: &Expr,
    ) -> Vec<AggregatePrepassFact> {
        let mut scanner = AggregatePrepassScanner {
            expr_context,
            name_resolution_ctx,
            functions,
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
        if Self::is_window_expr(expr) {
            self.window_depth += 1;
        }

        if self.window_depth > 0 || self.query_depth > 0 {
            return;
        }

        if let Some(fact) = match expr {
            Expr::CountAll { window: None, .. } => self.build_fact(expr),
            Expr::FunctionCall { func, .. }
                if self
                    .functions
                    .is_aggregate_target(self.name_resolution_ctx, func) =>
            {
                self.build_fact(expr)
            }
            _ => None,
        } {
            self.facts.push(fact);
        }
    }

    fn exit_expr(&mut self, expr: &Expr) {
        if Self::is_window_expr(expr) {
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
        let analysis = AggregatePrepassExprInfo::analyze(
            self.name_resolution_ctx,
            self.functions,
            &self.ast_aliases.alias_names(),
            expr,
        );
        if analysis.contains_subquery {
            return None;
        }

        Some(AggregatePrepassFact {
            expr_context: self.expr_context,
            source: self.current_source(),
            expr: analysis.ast,
            contains_window: analysis.contains_window,
        })
    }

    fn current_source(&self) -> AggregatePrepassSource {
        match self.expansion_stack.first() {
            Some(alias) => AggregatePrepassSource::AliasExpansion(alias.clone()),
            None => AggregatePrepassSource::DirectClause,
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
        functions: &AggregatePrepassFunctionCatalog,
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
        AggregatePrepassAliasCatalog::new(&self.name_resolution_ctx, functions, aliases)
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

    pub(super) fn build_aggregate_prepass_function_catalog(
        &self,
        bind_context: &BindContext,
        select_list: &SelectList<'_>,
        having: Option<&Expr>,
        qualify: Option<&Expr>,
        order_by: &[databend_common_ast::ast::OrderByExpr],
    ) -> Result<AggregatePrepassFunctionCatalog> {
        let function_names =
            self.collect_aggregate_prepass_function_names(select_list, having, qualify, order_by);
        let udaf_names = self.resolve_aggregate_prepass_udaf_names(bind_context, function_names)?;

        Ok(AggregatePrepassFunctionCatalog { udaf_names })
    }

    fn collect_aggregate_prepass_function_names(
        &self,
        select_list: &SelectList<'_>,
        having: Option<&Expr>,
        qualify: Option<&Expr>,
        order_by: &[databend_common_ast::ast::OrderByExpr],
    ) -> BTreeSet<String> {
        let mut visitor = AggregatePrepassFunctionNameCollector::new(&self.name_resolution_ctx);

        for item in select_list.items.iter() {
            if let SelectTarget::AliasedExpr { expr, .. } = item.select_target {
                expr.drive(&mut visitor);
            }
        }
        if let Some(expr) = having {
            expr.drive(&mut visitor);
        }
        if let Some(expr) = qualify {
            expr.drive(&mut visitor);
        }
        for order in order_by {
            order.expr.drive(&mut visitor);
        }

        visitor.names
    }

    fn resolve_aggregate_prepass_udaf_names(
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
        functions: &AggregatePrepassFunctionCatalog,
        aliases: &AggregatePrepassAliasCatalog,
        ast_iter: impl Iterator<Item = (&'a Expr, ExprContext)>,
    ) -> AggregatePrepassFacts {
        ast_iter
            .flat_map(|(ast_expr, expr_context)| {
                AggregatePrepassScanner::scan(
                    expr_context,
                    &self.name_resolution_ctx,
                    functions,
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
        let functions = AggregatePrepassFunctionCatalog::default();
        let aliases = AggregatePrepassAliasCatalog::new(&name_resolution_ctx, &functions, vec![
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
        let functions = AggregatePrepassFunctionCatalog::default();
        let aliases = AggregatePrepassAliasCatalog::new(&name_resolution_ctx, &functions, vec![(
            "s".to_string(),
            parse_ast_expr("sum(number)"),
        )]);

        let facts = AggregatePrepassScanner::scan(
            ExprContext::HavingClause,
            &name_resolution_ctx,
            &functions,
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
        let functions = AggregatePrepassFunctionCatalog::default();
        let aliases = AggregatePrepassAliasCatalog::new(&name_resolution_ctx, &functions, vec![
            ("s".to_string(), parse_ast_expr("sum(number)")),
            ("s".to_string(), parse_ast_expr("max(number)")),
        ]);

        let facts = AggregatePrepassScanner::scan(
            ExprContext::OrderByClause,
            &name_resolution_ctx,
            &functions,
            &aliases,
            &parse_ast_expr("s > 0"),
        );

        assert!(facts.is_empty());
    }

    #[test]
    fn aggregate_prepass_facts_deduplicate_identical_candidates() {
        let name_resolution_ctx = NameResolutionContext::default();
        let functions = AggregatePrepassFunctionCatalog::default();
        let aliases = AggregatePrepassAliasCatalog::new(&name_resolution_ctx, &functions, vec![(
            "s".to_string(),
            parse_ast_expr("sum(number)"),
        )]);

        let mut facts = AggregatePrepassFacts::default();
        for fact in AggregatePrepassScanner::scan(
            ExprContext::HavingClause,
            &name_resolution_ctx,
            &functions,
            &aliases,
            &parse_ast_expr("sum(number) > 0"),
        ) {
            facts.insert_prioritized_unique(fact);
        }
        for fact in AggregatePrepassScanner::scan(
            ExprContext::HavingClause,
            &name_resolution_ctx,
            &functions,
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
        let functions = AggregatePrepassFunctionCatalog::default();
        let aliases = AggregatePrepassAliasCatalog::new(&name_resolution_ctx, &functions, vec![
            ("s".to_string(), parse_ast_expr("sum(number)")),
            ("a".to_string(), parse_ast_expr("s + 1")),
        ]);

        let facts = AggregatePrepassScanner::scan(
            ExprContext::HavingClause,
            &name_resolution_ctx,
            &functions,
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
    fn aggregate_prepass_alias_catalog_tracks_transitive_aggregate_and_window_aliases() {
        let name_resolution_ctx = NameResolutionContext::default();
        let functions = AggregatePrepassFunctionCatalog::default();
        let aliases = AggregatePrepassAliasCatalog::new(&name_resolution_ctx, &functions, vec![
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
