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

use databend_common_ast::Span;
use databend_common_ast::ast::SampleConfig;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::TableRef;
use databend_common_ast::ast::TemporalClause;
use databend_common_ast::ast::WithOptions;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::table::TimeNavigation;
use databend_common_catalog::table_with_options::check_with_opt_valid;
use databend_common_catalog::table_with_options::get_with_opt_consume;
use databend_common_catalog::table_with_options::get_with_opt_max_batch_size;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storages_basic::view_table::QUERY;
use databend_storages_common_table_meta::table::get_change_type;

use crate::BindContext;
use crate::binder::Binder;
use crate::binder::util::TableIdentifier;
use crate::optimizer::ir::SExpr;
impl Binder {
    /// Bind a base table.
    /// A base table is a table that is not a view or CTE.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bind_table(
        &mut self,
        bind_context: &mut BindContext,
        span: &Span,
        table_ref: &TableRef,
        alias: &Option<TableAlias>,
        temporal: &Option<TemporalClause>,
        with_options: &Option<WithOptions>,
        sample: &Option<SampleConfig>,
    ) -> Result<(SExpr, BindContext)> {
        let TableRef {
            catalog,
            database,
            table,
            branch,
        } = table_ref;
        let table_identifier = TableIdentifier::new(self, catalog, database, table, branch, alias);
        let catalog = table_identifier.catalog_name();
        let database = table_identifier.database_name();
        let table_name = table_identifier.table_name();
        let branch_name = table_identifier.branch_name();
        let table_name_alias = table_identifier.table_name_alias();

        if let Some(cte_name) = &bind_context.cte_context.cte_name {
            if cte_name == &table_name {
                return Err(ErrorCode::SemanticError(format!(
                    "The cte {table_name} is not recursive, but it references itself.",
                ))
                .set_span(*span));
            }
        }

        let (consume, max_batch_size, with_opts_str) = if let Some(with_options) = with_options {
            check_with_opt_valid(with_options)?;
            let consume = get_with_opt_consume(with_options)?;
            let max_batch_size = get_with_opt_max_batch_size(with_options)?;
            let with_opts_str = with_options.to_change_query_with_clause();
            (consume, max_batch_size, with_opts_str)
        } else {
            (false, None, String::new())
        };

        // Check and bind common table expression
        let mut cte_suffix_name = None;
        let cte_map = bind_context.cte_context.cte_map.clone();
        if let Some(cte_info) = cte_map.get(&table_name) {
            if let Some(materialized_cte_info) = &cte_info.materialized_cte_info {
                return self.bind_cte_consumer(
                    bind_context,
                    &table_name,
                    alias,
                    cte_info,
                    &materialized_cte_info.bound_context.columns,
                );
            } else if cte_info.user_specified_materialized {
                cte_suffix_name = Some(self.ctx.get_id().replace("-", ""));
            } else {
                if self
                    .metadata
                    .read()
                    .get_table_index(Some(&database), &table_name)
                    .is_some()
                {
                    return Err(ErrorCode::SyntaxException(format!(
                        "Table name `{}` is misleading, please distinguish it.",
                        table_name
                    ))
                    .set_span(*span));
                }
                return if cte_info.recursive {
                    if self
                        .bind_recursive_cte
                        .as_ref()
                        .map(|name| name == &table_name)
                        .unwrap_or(false)
                    {
                        self.bind_r_cte_scan(bind_context, cte_info, &table_name, alias)
                    } else {
                        self.bind_r_cte(*span, bind_context, cte_info, &table_name, alias)
                    }
                } else {
                    self.bind_cte(*span, bind_context, &table_name, alias, cte_info)
                };
            }
        }

        let navigation = self.resolve_temporal_clause(bind_context, temporal)?;

        // Resolve table with catalog
        let table_meta = {
            let table_name = if let Some(cte_suffix_name) = cte_suffix_name.as_ref() {
                format!("{}${}", &table_name, cte_suffix_name)
            } else {
                table_name.clone()
            };
            match self.resolve_data_source(
                &self.ctx,
                catalog.as_str(),
                database.as_str(),
                table_name.as_str(),
                branch_name.as_deref(),
                navigation.as_ref(),
                max_batch_size,
            ) {
                Ok(table) => table,
                Err(e) => {
                    let mut parent = bind_context.parent.as_mut();
                    loop {
                        if parent.is_none() {
                            break;
                        }
                        let bind_context = parent.unwrap().as_mut();
                        let cte_map = bind_context.cte_context.cte_map.clone();
                        if let Some(cte_info) = cte_map.get(&table_name) {
                            return self.bind_cte(
                                *span,
                                bind_context,
                                &table_name,
                                alias,
                                cte_info,
                            );
                        }
                        parent = bind_context.parent.as_mut();
                    }
                    return Err(table_identifier.not_found_suggest_error(e));
                }
            }
        };

        if consume && !table_meta.is_stream() {
            return Err(ErrorCode::StorageUnsupported(
                "WITH CONSUME only support in STREAM",
            ));
        }

        if navigation.is_some_and(|n| matches!(n, TimeNavigation::Changes { .. }))
            || table_meta.is_stream()
        {
            let change_type = get_change_type(&table_name_alias);
            if change_type.is_some() {
                let table_index = self.metadata.write().add_table(
                    catalog,
                    database.clone(),
                    table_meta.clone(),
                    branch_name,
                    table_name_alias,
                    bind_context.view_info.is_some(),
                    bind_context.planning_agg_index,
                    false,
                    cte_suffix_name,
                );
                let (s_expr, mut bind_context) = self.bind_base_table(
                    bind_context,
                    database.as_str(),
                    table_index,
                    change_type,
                    sample,
                )?;

                if let Some(alias) = alias {
                    bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                }
                return Ok((s_expr, bind_context));
            }

            let query =
                databend_common_base::runtime::block_on(table_meta.generate_changes_query(
                    self.ctx.clone(),
                    database.as_str(),
                    table_name.as_str(),
                    &with_opts_str,
                ))?;

            if table_meta.is_stream() {
                self.ctx
                    .add_streams_ref(&catalog, &database, &table_name, consume);
            }
            let mut new_bind_context = BindContext::with_parent(bind_context.clone())?;
            let tokens = tokenize_sql(query.as_str())?;
            let (stmt, _) = parse_sql(&tokens, self.dialect)?;
            let Statement::Query(query) = &stmt else {
                unreachable!()
            };
            let (s_expr, mut new_bind_context) = self.bind_query(&mut new_bind_context, query)?;
            bind_context
                .cte_context
                .set_cte_context(new_bind_context.cte_context.clone());

            let cols = table_meta
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>();
            for (index, column_name) in cols.iter().enumerate() {
                new_bind_context.columns[index].column_name = column_name.clone();
            }

            if let Some(alias) = alias {
                new_bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
            } else {
                for column in new_bind_context.columns.iter_mut() {
                    column.database_name = None;
                    column.table_name = Some(table_name.clone());
                }
            }

            new_bind_context.parent = Some(Box::new(bind_context.clone()));
            return Ok((s_expr, new_bind_context));
        }

        match table_meta.engine() {
            "VIEW" => {
                // TODO(leiysky): this check is error-prone,
                // we should find a better way to do this.
                Self::check_view_dep(bind_context, &database, &table_name)?;
                let query = table_meta
                    .options()
                    .get(QUERY)
                    .ok_or_else(|| ErrorCode::Internal("Invalid VIEW object"))?;
                let tokens = tokenize_sql(query.as_str())?;
                let (stmt, _) = parse_sql(&tokens, self.dialect)?;
                // For view, we need use a new context to bind it.
                let mut new_bind_context = BindContext::with_parent(bind_context.clone())?;
                new_bind_context.view_info = Some((database.clone(), table_name));
                if let Statement::Query(query) = &stmt {
                    self.metadata.write().add_table(
                        catalog,
                        database.clone(),
                        table_meta,
                        branch_name,
                        table_name_alias,
                        false,
                        false,
                        false,
                        None,
                    );
                    let (s_expr, mut new_bind_context) =
                        self.bind_query(&mut new_bind_context, query)?;
                    if let Some(alias) = alias {
                        // view maybe has alias, e.g. select v1.col1 from v as v1;
                        new_bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                    } else {
                        // e.g. select v0.c0 from v0;
                        for column in new_bind_context.columns.iter_mut() {
                            column.database_name = Some(database.clone());
                            column.table_name = Some(self.normalize_identifier(table).name);
                        }
                    }
                    new_bind_context.parent = Some(Box::new(bind_context.clone()));
                    Ok((s_expr, new_bind_context))
                } else {
                    Err(
                        ErrorCode::Internal(format!("Invalid VIEW object: {}", table_meta.name()))
                            .set_span(*span),
                    )
                }
            }
            _ => {
                let table_index = self.metadata.write().add_table(
                    catalog.clone(),
                    database.clone(),
                    table_meta.clone(),
                    branch_name,
                    table_name_alias,
                    bind_context.view_info.is_some(),
                    bind_context.planning_agg_index,
                    false,
                    cte_suffix_name,
                );

                let (s_expr, mut bind_context) = self.bind_base_table(
                    bind_context,
                    database.as_str(),
                    table_index,
                    None,
                    sample,
                )?;
                if let Some(alias) = alias {
                    bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                }

                Ok((s_expr, bind_context))
            }
        }
    }

    pub(crate) fn check_view_dep(
        bind_context: &BindContext,
        database: &str,
        view_name: &str,
    ) -> Result<()> {
        match &bind_context.parent {
            Some(parent) => match &parent.view_info {
                Some((db, v)) => {
                    if db == database && v == view_name {
                        Err(ErrorCode::Internal(format!(
                            "View dependency loop detected (view: {}.{})",
                            database, view_name
                        )))
                    } else {
                        Self::check_view_dep(parent, database, view_name)
                    }
                }
                _ => Ok(()),
            },
            _ => Ok(()),
        }
    }
}
