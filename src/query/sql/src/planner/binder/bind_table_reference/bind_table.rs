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

use std::collections::HashMap;

use databend_common_ast::Span;
use databend_common_ast::ast::SampleConfig;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::TableRef;
use databend_common_ast::ast::TemporalClause;
use databend_common_ast::ast::WithOptions;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::TimeNavigation;
use databend_common_catalog::table_with_options::check_with_opt_valid;
use databend_common_catalog::table_with_options::get_with_opt_consume;
use databend_common_catalog::table_with_options::get_with_opt_max_batch_size;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_meta_app::schema::MATERIALIZED_VIEW_ENGINE;
use databend_common_storages_basic::view_table::QUERY;
use databend_storages_common_table_meta::table::get_change_type;

use crate::BindContext;
use crate::ColumnEntry;
use crate::LineageSourceRelation;
use crate::MaterializedCteLineageSource;
use crate::Metadata;
use crate::Symbol;
use crate::ViewLineageSourceColumn;
use crate::Visibility;
use crate::binder::Binder;
use crate::binder::ViewIdent;
use crate::binder::lineage_enabled;
use crate::binder::util::TableIdentifier;
use crate::optimizer::ir::SExpr;
impl Binder {
    fn reject_branch_qualified_cte_reference(
        span: Span,
        cte_name: &str,
        branch_name: Option<&str>,
    ) -> Result<()> {
        if let Some(branch_name) = branch_name {
            return Err(ErrorCode::SemanticError(format!(
                "CTE `{cte_name}` does not support branch-qualified reference `{cte_name}/{branch_name}`"
            ))
            .set_span(span));
        }
        Ok(())
    }

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
        let mut materialized_cte_lineage = None;
        let cte_map = bind_context.cte_context.cte_map.clone();
        if let Some(cte_info) = cte_map.get(&table_name) {
            Self::reject_branch_qualified_cte_reference(
                *span,
                &table_name,
                branch_name.as_deref(),
            )?;
            if let Some(materialized_cte_info) = &cte_info.materialized_cte_info {
                return self.bind_cte_consumer(
                    bind_context,
                    &table_name,
                    alias,
                    cte_info,
                    &materialized_cte_info.bound_context.columns,
                );
            } else if cte_info.user_specified_materialized {
                if lineage_enabled() {
                    // The main query scans a temporary table, so retain a separately bound
                    // producer definition that lineage extraction can follow by output position.
                    materialized_cte_lineage = Some(self.bind_cte_definition(
                        &table_name,
                        cte_map.as_ref(),
                        &cte_info.query,
                    )?);
                }
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
        if let Some(branch_name) = branch_name.as_ref() {
            // Branch-qualified reads are feature/license gated during table resolution in
            // QueryContext::get_table_from_shared() before any branch table is loaded. Keep the
            // binder-side branch handling here focused on syntax/semantic validation.
            // Branch reads are supported in FROM, but TAG navigation stays bound to the base table
            // namespace (`db.table AT (TAG => ...)`). Reject the mixed form early in binder.
            if matches!(
                navigation.as_ref(),
                Some(TimeNavigation::TimeTravel {
                    point: NavigationPoint::TableTag(_),
                    ..
                }) | Some(TimeNavigation::Changes {
                    at: NavigationPoint::TableTag(_),
                    ..
                }) | Some(TimeNavigation::Changes {
                    end: Some(NavigationPoint::TableTag(_)),
                    ..
                })
            ) {
                return Err(ErrorCode::Unimplemented(format!(
                    "Unsupported TAG navigation on branch reference `{catalog}.{database}.{table_name}/{branch_name}`"
                ))
                .set_span(*span));
            }
        }

        // Resolve table with catalog, allowing internal rewrites to bind an exact table instance.
        let table_meta = if let Some(table) =
            self.pre_resolved_tables
                .get(&(catalog.clone(), database.clone(), table_name.clone()))
        {
            table.clone()
        } else {
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
                            Self::reject_branch_qualified_cte_reference(
                                *span,
                                &table_name,
                                branch_name.as_deref(),
                            )?;
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
            || table_meta.has_changes_source()
        {
            let change_type = get_change_type(&table_name_alias);
            if change_type.is_some() {
                let stream_lineage_source = stream_lineage_source_relation(&table_meta);
                let table_index = {
                    let mut metadata = self.metadata.write();
                    let table_index = metadata.add_table(
                        catalog,
                        database.clone(),
                        table_name.clone(),
                        table_meta.clone(),
                        branch_name,
                        table_name_alias,
                        !bind_context.binding_views.is_empty(),
                        bind_context.planning_agg_index,
                        false,
                    );
                    if let Some(stream_lineage_source) = stream_lineage_source {
                        metadata.set_stream_lineage_source(table_index, stream_lineage_source);
                    }
                    table_index
                };
                let (s_expr, mut bind_context) = self.bind_base_table(
                    bind_context,
                    database.as_str(),
                    table_index,
                    change_type,
                    sample,
                    true,
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
                let view_ident = ViewIdent {
                    catalog: catalog.clone(),
                    database: database.clone(),
                    name: table_name.clone(),
                };
                bind_context.check_view_loop(&view_ident)?;
                let query = table_meta
                    .options()
                    .get(QUERY)
                    .ok_or_else(|| ErrorCode::Internal("Invalid VIEW object"))?;
                let tokens = tokenize_sql(query.as_str())?;
                let (stmt, _) = parse_sql(&tokens, self.dialect)?;
                // For view, we need use a new context to bind it.
                let mut new_bind_context = BindContext::with_parent(bind_context.clone())?;
                new_bind_context.binding_views.insert(view_ident);
                if let Statement::Query(query) = &stmt {
                    self.metadata.write().add_table(
                        catalog.clone(),
                        database.clone(),
                        table_name.clone(),
                        table_meta.clone(),
                        branch_name.clone(),
                        table_name_alias.clone(),
                        false,
                        false,
                        false,
                    );
                    let (s_expr, mut new_bind_context) =
                        self.bind_query(&mut new_bind_context, query)?;
                    if lineage_enabled() {
                        // Record the view's output identity before an outer table alias can
                        // rename those columns in the current query scope.
                        self.add_view_lineage_source_columns(
                            &new_bind_context,
                            catalog.as_str(),
                            database.as_str(),
                            table_name.as_str(),
                            &table_meta,
                        );
                    }
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
                    // Restore binding_views to the outer scope's value so the
                    // current view does not leak into sibling/parent contexts.
                    new_bind_context.binding_views = bind_context.binding_views.clone();
                    new_bind_context.parent = Some(Box::new(bind_context.clone()));
                    Ok((s_expr, new_bind_context))
                } else {
                    Err(
                        ErrorCode::Internal(format!("Invalid VIEW object: {}", table_meta.name()))
                            .set_span(*span),
                    )
                }
            }
            MATERIALIZED_VIEW_ENGINE => self.bind_materialized_view(
                bind_context,
                &catalog,
                &database,
                &self.normalize_identifier(table).name,
                table_name_alias,
                table_meta,
                alias,
                sample,
            ),
            _ => {
                let table_index = self.metadata.write().add_table(
                    catalog,
                    database.clone(),
                    table_name.clone(),
                    table_meta,
                    branch_name,
                    table_name_alias,
                    !bind_context.binding_views.is_empty(),
                    bind_context.planning_agg_index,
                    false,
                );

                let (s_expr, mut bind_context) = self.bind_base_table(
                    bind_context,
                    database.as_str(),
                    table_index,
                    None,
                    sample,
                    true,
                )?;
                if let Some((definition, producer_context)) = materialized_cte_lineage {
                    let producer_columns = producer_context
                        .columns
                        .iter()
                        .filter(|column| column.visibility == Visibility::Visible)
                        .collect::<Vec<_>>();
                    let consumer_column_count = bind_context
                        .columns
                        .iter()
                        .filter(|column| column.visibility == Visibility::Visible)
                        .count();
                    if consumer_column_count != producer_columns.len() {
                        return Err(ErrorCode::Internal(format!(
                            "Materialized CTE '{}' has {} producer columns but {} temporary-table columns",
                            table_name,
                            producer_columns.len(),
                            consumer_column_count
                        )));
                    }
                    let column_mapping = {
                        let metadata = self.metadata.read();
                        bind_context
                            .columns
                            .iter()
                            .filter_map(|consumer| {
                                let output_position =
                                    materialized_cte_output_position(&metadata, consumer.index)?;
                                let producer = producer_columns.get(output_position)?;
                                Some((
                                    consumer.index,
                                    materialized_cte_producer_column(
                                        &metadata,
                                        consumer.index,
                                        producer.index,
                                    ),
                                ))
                            })
                            .collect::<HashMap<_, _>>()
                    };
                    self.metadata.write().add_materialized_cte_lineage_source(
                        table_index,
                        MaterializedCteLineageSource {
                            definition,
                            column_mapping,
                        },
                    );
                }
                if let Some(alias) = alias {
                    bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                }

                Ok((s_expr, bind_context))
            }
        }
    }

    fn add_view_lineage_source_columns(
        &mut self,
        bind_context: &BindContext,
        catalog: &str,
        database: &str,
        view_name: &str,
        view: &std::sync::Arc<dyn databend_common_catalog::table::Table>,
    ) {
        let relation = LineageSourceRelation {
            catalog: catalog.to_string(),
            database: database.to_string(),
            name: view_name.to_string(),
            id: view.get_table_info().ident.table_id,
        };
        let mut metadata = self.metadata.write();
        for (idx, column) in bind_context.columns.iter().enumerate() {
            metadata.add_view_lineage_source_column(column.index, ViewLineageSourceColumn {
                relation: relation.clone(),
                // View TableMeta has no persisted schema. The bound view query (including an
                // explicit view column list) is the source of truth for output column names.
                name: column.column_name.clone(),
                id: idx as ColumnId,
            });
        }
    }
}

fn materialized_cte_producer_column(
    metadata: &Metadata,
    consumer_index: Symbol,
    producer_index: Symbol,
) -> Symbol {
    let ColumnEntry::BaseTableColumn(consumer) = metadata.column(consumer_index) else {
        return producer_index;
    };
    let Some(consumer_path) = consumer.path_indices.as_deref() else {
        return producer_index;
    };
    let Some(nested_path) = consumer_path.get(1..) else {
        return producer_index;
    };

    let ColumnEntry::BaseTableColumn(producer) = metadata.column(producer_index) else {
        return producer_index;
    };
    let mut producer_path = if let Some(path) = &producer.path_indices {
        path.clone()
    } else if let Some(position) = materialized_cte_output_position(metadata, producer_index) {
        vec![position]
    } else {
        return producer_index;
    };
    producer_path.extend_from_slice(nested_path);

    metadata
        .columns_by_table_index(producer.table_index)
        .find_map(|column| match column {
            ColumnEntry::BaseTableColumn(column)
                if column.path_indices.as_deref() == Some(producer_path.as_slice()) =>
            {
                Some(column.column_index)
            }
            _ => None,
        })
        .unwrap_or(producer_index)
}

fn materialized_cte_output_position(metadata: &Metadata, column_index: Symbol) -> Option<usize> {
    let ColumnEntry::BaseTableColumn(column) = metadata.column(column_index) else {
        return None;
    };
    if let Some(path) = &column.path_indices {
        return path.first().copied();
    }
    if let Some(position) = column.column_position {
        return position.checked_sub(1);
    }

    metadata
        .columns_by_table_index(column.table_index)
        .filter(|column| {
            matches!(
                column,
                ColumnEntry::BaseTableColumn(column) if column.path_indices.is_none()
            )
        })
        .position(|column| column.index() == column_index)
}

fn stream_lineage_source_relation(
    table: &std::sync::Arc<dyn databend_common_catalog::table::Table>,
) -> Option<LineageSourceRelation> {
    table.stream_source_table_info().and_then(|table_info| {
        let database = table_info.database_name().ok()?.to_string();
        Some(LineageSourceRelation {
            catalog: table_info.catalog().to_string(),
            database,
            name: table_info.name.clone(),
            id: table_info.ident.table_id,
        })
    })
}
