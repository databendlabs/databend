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
pub(super) struct AggregatePrepassExprFeatures {
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
    pub contains_subquery: bool,
    pub referenced_aliases: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct AggregatePrepassFacts {
    aliases: AggregatePrepassAliasCatalog,
    facts: Vec<AggregatePrepassFact>,
}

#[derive(Debug, Clone)]
pub(super) struct AggregatePrepassAliasInfo {
    #[cfg_attr(not(test), expect(dead_code))]
    pub name: String,
    pub ast: Expr,
    pub contains_aggregate: bool,
    pub contains_window: bool,
    #[cfg_attr(not(test), expect(dead_code))]
    pub contains_subquery: bool,
    pub referenced_aliases: Vec<String>,
}

#[derive(Debug, Default, Clone)]
pub(super) struct AggregatePrepassAliasCatalog {
    items: Vec<AggregatePrepassAliasInfo>,
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

        let items = aliases
            .into_iter()
            .map(|(name, ast)| {
                let AggregatePrepassExprFeatures {
                    contains_aggregate,
                    contains_window,
                    contains_subquery,
                } = Binder::aggregate_prepass_expr_features(name_resolution_ctx, functions, &ast);
                AggregatePrepassAliasInfo {
                    referenced_aliases: Self::collect_referenced_aliases(
                        name_resolution_ctx,
                        &by_name,
                        &ast,
                    ),
                    contains_aggregate,
                    contains_window,
                    contains_subquery,
                    name,
                    ast,
                }
            })
            .collect();

        Self { items, by_name }
    }

    fn get_unique(&self, name: &str) -> Option<&AggregatePrepassAliasInfo> {
        if let [index] = self.by_name.get(name)?.as_slice() {
            Some(&self.items[*index])
        } else {
            None
        }
    }

    #[cfg(test)]
    fn items(&self) -> &[AggregatePrepassAliasInfo] {
        &self.items
    }

    pub(super) fn referenced_aliases(
        &self,
        name_resolution_ctx: &NameResolutionContext,
        expr: &Expr,
    ) -> Vec<String> {
        Self::collect_referenced_aliases(name_resolution_ctx, &self.by_name, expr)
    }

    pub(super) fn references_window_aliases(&self, names: &[String]) -> bool {
        names.iter().any(|name| {
            self.alias_reaches(name, &|alias| alias.contains_window, &mut BTreeSet::new())
        })
    }

    pub(super) fn references_aggregate_aliases(&self, names: &[String]) -> bool {
        names.iter().any(|name| {
            self.alias_reaches(
                name,
                &|alias| alias.contains_aggregate,
                &mut BTreeSet::new(),
            )
        })
    }

    fn alias_reaches<F>(&self, name: &str, predicate: &F, visiting: &mut BTreeSet<String>) -> bool
    where F: Fn(&AggregatePrepassAliasInfo) -> bool {
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

    fn collect_referenced_aliases(
        name_resolution_ctx: &NameResolutionContext,
        aliases: &BTreeMap<String, Vec<usize>>,
        expr: &Expr,
    ) -> Vec<String> {
        let mut visitor = ReferencedAliasVisitor {
            name_resolution_ctx,
            aliases,
            query_depth: 0,
            names: BTreeSet::new(),
        };
        expr.drive(&mut visitor);
        visitor.names.into_iter().collect()
    }
}

#[derive(Visitor)]
#[visitor(Expr(enter), Query(enter))]
struct AggregatePrepassExprFeatureVisitor<'a> {
    name_resolution_ctx: &'a NameResolutionContext,
    functions: &'a AggregatePrepassFunctionCatalog,
    features: AggregatePrepassExprFeatures,
}

impl AggregatePrepassExprFeatureVisitor<'_> {
    fn enter_expr(&mut self, expr: &Expr) {
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
    aliases: &'a BTreeMap<String, Vec<usize>>,
    query_depth: usize,
    names: BTreeSet<String>,
}

impl ReferencedAliasVisitor<'_> {
    fn enter_column_ref(&mut self, column: &ColumnRef) {
        if self.query_depth > 0 {
            return;
        }

        if column.database.is_some() || column.table.is_some() {
            return;
        }

        let ColumnID::Name(ident) = &column.column else {
            return;
        };

        let alias = normalize_identifier(ident, self.name_resolution_ctx).name;
        if self.aliases.contains_key(&alias) {
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
    fragments: Vec<AggregatePrepassFact>,
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
            fragments: Vec::new(),
        };
        expr.drive(&mut scanner);
        scanner.fragments
    }

    fn enter_expr(&mut self, expr: &Expr) {
        if Self::is_window_expr(expr) {
            self.window_depth += 1;
        }

        if self.window_depth > 0 || self.query_depth > 0 {
            return;
        }

        match expr {
            Expr::CountAll { window: None, .. } => self.record_fragment(expr),
            Expr::FunctionCall { func, .. }
                if self
                    .functions
                    .is_aggregate_target(self.name_resolution_ctx, func) =>
            {
                self.record_fragment(expr)
            }
            _ => {}
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

    fn record_fragment(&mut self, expr: &Expr) {
        let features =
            Binder::aggregate_prepass_expr_features(self.name_resolution_ctx, self.functions, expr);
        if features.contains_subquery {
            return;
        };

        self.fragments.push(AggregatePrepassFact {
            expr_context: self.expr_context,
            source: match self.expansion_stack.as_array() {
                Some([alias]) => AggregatePrepassSource::AliasExpansion(alias.clone()),
                None => AggregatePrepassSource::DirectClause,
            },
            expr: expr.clone(),
            contains_window: features.contains_window,
            contains_subquery: features.contains_subquery,
            referenced_aliases: AggregatePrepassAliasCatalog::collect_referenced_aliases(
                self.name_resolution_ctx,
                &self.ast_aliases.by_name,
                expr,
            ),
        });
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
        if column.database.is_some() || column.table.is_some() {
            return None;
        }

        let ColumnID::Name(ident) = &column.column else {
            return None;
        };

        let alias = normalize_identifier(ident, name_resolution_ctx).name;
        let ast = &ast_aliases.get_unique(&alias)?.ast;
        Some((alias, ast))
    }
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

    pub(super) fn register_aggregate_prepass_facts(
        &mut self,
        bind_context: &mut BindContext,
        aliases: &[(String, ScalarExpr)],
        facts: &AggregatePrepassFacts,
    ) -> Result<()> {
        let mut direct = facts
            .facts
            .iter()
            .filter(|fact| fact.source.is_direct_clause())
            .collect::<Vec<_>>();
        direct.dedup();
        for fact in direct {
            self.bind_and_rewrite_aggregate_expr(
                bind_context,
                aliases,
                fact.expr_context,
                &fact.expr,
            )?;
        }

        let mut alias = facts
            .facts
            .iter()
            .filter(|fact| fact.source.is_alias_expansion())
            .collect::<Vec<_>>();
        alias.dedup();
        for fact in alias {
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

        let mut udaf_names = HashSet::new();
        let tenant = self.ctx.get_tenant();
        let provider = UserApiProvider::instance();
        for name in visitor.names {
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

        Ok(AggregatePrepassFunctionCatalog { udaf_names })
    }

    pub(super) fn aggregate_prepass_expr_features(
        name_resolution_ctx: &NameResolutionContext,
        functions: &AggregatePrepassFunctionCatalog,
        expr: &Expr,
    ) -> AggregatePrepassExprFeatures {
        let mut detector = AggregatePrepassExprFeatureVisitor {
            name_resolution_ctx,
            functions,
            features: Default::default(),
        };
        expr.drive(&mut detector);
        detector.features
    }

    pub(super) fn derive_aggregate_prepass_facts<'a>(
        &self,
        functions: &AggregatePrepassFunctionCatalog,
        aliases: &AggregatePrepassAliasCatalog,
        ast_iter: impl Iterator<Item = (&'a Expr, ExprContext)>,
    ) -> AggregatePrepassFacts {
        ast_iter
            .map(|(ast_expr, expr_context)| AggregatePrepassFacts {
                aliases: aliases.clone(),
                facts: AggregatePrepassScanner::scan(
                    expr_context,
                    &self.name_resolution_ctx,
                    functions,
                    aliases,
                    ast_expr,
                ),
            })
            .fold(AggregatePrepassFacts::default(), |mut acc, item| {
                if acc.aliases.items.is_empty() {
                    acc.aliases = item.aliases;
                }
                acc.facts.extend(item.facts);
                acc
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

    impl AggregatePrepassFacts {
        fn new(aliases: AggregatePrepassAliasCatalog) -> Self {
            Self {
                aliases,
                facts: Vec::new(),
            }
        }

        fn iter(&self) -> impl Iterator<Item = &AggregatePrepassFact> {
            self.facts.iter()
        }

        fn push_fact(&mut self, fact: AggregatePrepassFact) {
            self.facts.push(fact);
        }

        fn unique_facts(&self) -> Vec<&AggregatePrepassFact> {
            let mut unique = Vec::new();
            for fact in &self.facts {
                if unique.contains(&fact) {
                    continue;
                }
                unique.push(fact);
            }
            unique
        }

        fn ordered_unique_facts(&self) -> Vec<&AggregatePrepassFact> {
            let unique = self.unique_facts();
            let mut ordered = Vec::with_capacity(unique.len());
            ordered.extend(
                unique
                    .iter()
                    .copied()
                    .filter(|fact| matches!(fact.source, AggregatePrepassSource::DirectClause)),
            );
            ordered.extend(
                unique.iter().copied().filter(|fact| {
                    matches!(fact.source, AggregatePrepassSource::AliasExpansion(_))
                }),
            );
            ordered
        }
    }

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

        assert_eq!(items[0].name, "s");
        assert!(items[0].contains_aggregate);
        assert!(!items[0].contains_window);
        assert!(!items[0].contains_subquery);
        assert!(items[0].referenced_aliases.is_empty());

        assert_eq!(items[1].name, "rn");
        assert!(!items[1].contains_aggregate);
        assert!(items[1].contains_window);
        assert!(!items[1].contains_subquery);
        assert_eq!(items[1].referenced_aliases, vec!["s".to_string()]);

        assert_eq!(items[2].name, "sub");
        assert!(items[2].contains_aggregate);
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
        assert!(!facts[0].contains_subquery);
        assert!(facts[0].referenced_aliases.is_empty());
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

        let mut facts = AggregatePrepassFacts::new(aliases.clone());
        for fact in AggregatePrepassScanner::scan(
            ExprContext::HavingClause,
            &name_resolution_ctx,
            &functions,
            &aliases,
            &parse_ast_expr("sum(number) > 0"),
        ) {
            facts.push_fact(fact);
        }
        for fact in AggregatePrepassScanner::scan(
            ExprContext::HavingClause,
            &name_resolution_ctx,
            &functions,
            &aliases,
            &parse_ast_expr("sum(number) > 0"),
        ) {
            facts.push_fact(fact);
        }

        assert_eq!(facts.iter().count(), 2);
        assert_eq!(facts.unique_facts().len(), 1);
    }

    #[test]
    fn aggregate_prepass_facts_prioritize_direct_candidates() {
        let aliases = AggregatePrepassAliasCatalog::default();
        let expr = parse_ast_expr("sum(number)");
        let mut facts = AggregatePrepassFacts::new(aliases);
        facts.push_fact(AggregatePrepassFact {
            expr_context: ExprContext::HavingClause,
            source: AggregatePrepassSource::AliasExpansion("s".to_string()),
            expr: expr.clone(),
            contains_window: false,
            contains_subquery: false,
            referenced_aliases: Vec::new(),
        });
        facts.push_fact(AggregatePrepassFact {
            expr_context: ExprContext::HavingClause,
            source: AggregatePrepassSource::DirectClause,
            expr,
            contains_window: false,
            contains_subquery: false,
            referenced_aliases: Vec::new(),
        });

        let ordered = facts.ordered_unique_facts();
        assert_eq!(ordered.len(), 2);
        assert!(matches!(
            ordered[0].source,
            AggregatePrepassSource::DirectClause
        ));
        assert!(matches!(
            ordered[1].source,
            AggregatePrepassSource::AliasExpansion(_)
        ));
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

        assert!(aliases.references_aggregate_aliases(&["a".to_string()]));
        assert!(aliases.references_window_aliases(&["w".to_string()]));
        assert!(!aliases.references_aggregate_aliases(&["w".to_string()]));
        assert!(!aliases.references_window_aliases(&["a".to_string()]));
    }
}
