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

use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::quote::ident_opt_quote;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_cluster_key_exprs;
use databend_common_ast::parser::parse_comma_separated_idents;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_sql::IdentifierNormalizer;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::normalize_identifier;
use derive_visitor::DriveMut;
use derive_visitor::VisitorMut;

use crate::interpreters::ShowCreateQuerySettings;

#[derive(VisitorMut)]
#[visitor(ColumnRef(enter))]
struct ColumnRefCollector {
    columns: HashSet<String>,
}

impl ColumnRefCollector {
    fn new() -> Self {
        Self {
            columns: HashSet::new(),
        }
    }

    fn enter_column_ref(&mut self, column_ref: &mut ColumnRef) {
        if let ColumnID::Name(ident) = &column_ref.column {
            self.columns.insert(ident.name.clone());
        }
    }
}

pub fn cluster_key_referenced_columns(cluster_key: &str) -> Result<HashSet<String>> {
    let exprs = parse_cluster_key_exprs(cluster_key)?;
    let mut collector = ColumnRefCollector::new();
    for mut expr in exprs {
        expr.drive_mut(&mut collector);
    }
    Ok(collector.columns)
}

#[derive(VisitorMut)]
#[visitor(ColumnRef(enter))]
struct ColumnRenamer<'a> {
    old: &'a str,
    new: &'a str,
    new_quote: Option<char>,
    changed: bool,
}

impl<'a> ColumnRenamer<'a> {
    fn enter_column_ref(&mut self, column_ref: &mut ColumnRef) {
        if column_ref.column.name() != self.old {
            return;
        }
        if let ColumnID::Name(ident) = &mut column_ref.column {
            ident.name = self.new.to_string();
            ident.quote = self.new_quote;
            self.changed = true;
        }
    }
}

pub fn rename_column_in_cluster_key(
    ctx: &dyn TableContext,
    cluster_key: &str,
    old_column: &str,
    new_column: &str,
) -> Result<Option<String>> {
    // `cluster_key` is persisted in table metadata and may have been stored with different
    // quoting rules than the current session dialect. Use a dialect that accepts both.
    let sql_dialect = Dialect::default();
    let name_resolution_ctx = NameResolutionContext::try_from(ctx.get_settings().as_ref())?;
    let new_quote = ident_opt_quote(
        new_column,
        false,
        name_resolution_ctx.quoted_ident_case_sensitive,
        sql_dialect,
    );

    let mut exprs = parse_cluster_key_exprs(cluster_key)?;
    let mut renamer = ColumnRenamer {
        old: old_column,
        new: new_column,
        new_quote,
        changed: false,
    };

    for expr in exprs.iter_mut() {
        expr.drive_mut(&mut renamer);
    }

    if !renamer.changed {
        return Ok(None);
    }

    let mut normalizer = IdentifierNormalizer::new(&name_resolution_ctx);
    let cluster_keys = exprs
        .iter_mut()
        .map(|e| {
            e.drive_mut(&mut normalizer);
            format!("{:#}", e)
        })
        .collect::<Vec<_>>();
    Ok(Some(format!("({})", cluster_keys.join(", "))))
}

pub fn rename_column_in_comma_separated_ident(
    ctx: &dyn TableContext,
    definition: &mut String,
    old_column: &str,
    new_column: &str,
) -> Result<()> {
    let trimmed = definition.trim();
    if trimmed.is_empty() {
        return Ok(());
    }

    let sql_dialect = Dialect::default();
    let tokens = tokenize_sql(trimmed)?;
    let mut idents = parse_comma_separated_idents(&tokens, sql_dialect)?;

    let name_resolution_ctx = NameResolutionContext::try_from(ctx.get_settings().as_ref())?;
    let new_quote = ident_opt_quote(
        new_column,
        false,
        name_resolution_ctx.quoted_ident_case_sensitive,
        sql_dialect,
    );

    let mut changed = false;
    for ident in idents.iter_mut() {
        let normalized = normalize_identifier(ident, &name_resolution_ctx);
        if normalized.name == old_column {
            ident.name = new_column.to_string();
            ident.quote = new_quote;
            changed = true;
        }
    }

    if changed {
        *definition = idents
            .into_iter()
            .map(|ident| ident.to_string())
            .collect::<Vec<_>>()
            .join(",");
    }
    Ok(())
}

#[derive(VisitorMut)]
#[visitor(ColumnRef(enter))]
struct ClusterKeyColumnQuoteRewriter {
    force_quoted_ident: bool,
    quoted_ident_case_sensitive: bool,
    sql_dialect: Dialect,
}

impl ClusterKeyColumnQuoteRewriter {
    fn enter_column_ref(&mut self, column_ref: &mut ColumnRef) {
        if let ColumnID::Name(ident) = &mut column_ref.column {
            ident.quote = ident_opt_quote(
                &ident.name,
                self.force_quoted_ident,
                self.quoted_ident_case_sensitive,
                self.sql_dialect,
            );
        }
    }
}

pub fn format_cluster_key_for_show_create(
    cluster_key: &str,
    settings: &ShowCreateQuerySettings,
) -> Result<String> {
    // `cluster_key` is stored verbatim in table metadata and may contain identifier quotes
    // from another dialect. Re-render it using the current session dialect so that the
    // `SHOW CREATE TABLE` output is runnable under this dialect.
    let mut exprs = parse_cluster_key_exprs(cluster_key)?;
    let mut rewriter = ClusterKeyColumnQuoteRewriter {
        force_quoted_ident: settings.force_quoted_ident,
        quoted_ident_case_sensitive: settings.quoted_ident_case_sensitive,
        sql_dialect: settings.sql_dialect,
    };
    for expr in exprs.iter_mut() {
        expr.drive_mut(&mut rewriter);
    }

    let exprs = exprs
        .into_iter()
        .map(|expr| format!("{:#}", expr))
        .collect::<Vec<_>>();
    Ok(format!("({})", exprs.join(", ")))
}

#[cfg(test)]
mod tests {
    use databend_common_ast::ast::quote::ident_opt_quote;
    use databend_common_ast::parser::Dialect;
    use databend_common_catalog::table_context::TableContext;
    use databend_common_exception::Result;

    use super::rename_column_in_cluster_key;
    use super::rename_column_in_comma_separated_ident;
    use crate::test_kits::TestFixture;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_rename_column_in_cluster_key_preserve_quote() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;

        // Match the default behavior described in the bug report.
        let settings = ctx.get_settings();
        settings.set_setting("quoted_ident_case_sensitive".to_string(), "1".to_string())?;

        let updated = rename_column_in_cluster_key(ctx.as_ref(), "(col2)", "col2", "NewCol")?;
        assert_eq!(updated, Some("(\"NewCol\")".to_string()));

        let updated = rename_column_in_cluster_key(ctx.as_ref(), "(col2)", "col2", "newcol")?;
        assert_eq!(updated, Some("(newcol)".to_string()));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_rename_column_in_option_keeps_required_quote() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;

        let quoted_ident_case_sensitive = true;
        let force_quoted_ident = false;
        let quote_needed = ident_opt_quote(
            "NewCol",
            force_quoted_ident,
            quoted_ident_case_sensitive,
            Dialect::default(),
        )
        .is_some();
        assert!(quote_needed);

        let mut definition = "col1,col2".to_string();
        rename_column_in_comma_separated_ident(ctx.as_ref(), &mut definition, "col2", "NewCol")?;
        assert_eq!(definition, "col1,\"NewCol\"".to_string());
        Ok(())
    }
}
