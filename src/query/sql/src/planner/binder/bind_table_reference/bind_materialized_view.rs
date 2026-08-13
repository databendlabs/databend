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

use std::sync::Arc;

use databend_common_ast::ast::Query;
use databend_common_ast::ast::SampleConfig;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::TableReference;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::Visitor;
use databend_common_ast::visit::VisitorMut;
use databend_common_ast::visit::Walk;
use databend_common_ast::visit::WalkMut;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TimeNavigation;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_storages_common_table_meta::table::ChangeType;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_SOURCE_TABLE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_VER;
use databend_storages_common_table_meta::table::get_change_type;
use log::info;

use crate::BindContext;
use crate::Metadata;
use crate::ScalarExpr;
use crate::Visibility;
use crate::binder::Binder;
use crate::binder::ScalarBinder;
use crate::binder::ddl::materialized_view::find_materialized_view_aggregate;
use crate::optimizer::ir::SExpr;
use crate::parse_materialized_view_query;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::ScalarItem;
use crate::validate_materialized_view_source;

#[derive(Default)]
struct MaterializedViewChangeScanFinder {
    change_types: Vec<ChangeType>,
}

struct MaterializedViewChangeScanSampler<'a> {
    table_name: &'a str,
    sample: &'a Option<SampleConfig>,
}

impl VisitorMut for MaterializedViewChangeScanSampler<'_> {
    type Error = !;

    fn visit_table_reference(
        &mut self,
        table_ref: &mut TableReference,
    ) -> std::result::Result<VisitControl, Self::Error> {
        if let TableReference::Table { table, sample, .. } = table_ref
            && table.table.name == self.table_name
        {
            *sample = self.sample.clone();
        }
        Ok(VisitControl::Continue)
    }
}

impl Visitor for MaterializedViewChangeScanFinder {
    type Error = !;

    fn visit_table_reference(
        &mut self,
        table_ref: &TableReference,
    ) -> std::result::Result<VisitControl, Self::Error> {
        if let TableReference::Table { alias, .. } = table_ref {
            let alias_name = alias.as_ref().map(|alias| alias.name.name.clone());
            if let Some(change_type) = get_change_type(&alias_name) {
                self.change_types.push(change_type);
            }
        }
        Ok(VisitControl::Continue)
    }
}

impl Binder {
    /// Whether the MV storage checkpoint matches the current source data endpoint.
    fn is_materialized_view_fresh(
        mv_table: &dyn Table,
        source_snapshot_location: Option<&String>,
    ) -> bool {
        let options = mv_table.options();
        // The sequence option distinguishes an established empty checkpoint from CREATE's
        // unconsumed state. Its value is a CHANGE_TRACKING offset, not part of read freshness:
        // metadata-only source changes may advance the table sequence without changing data.
        options.contains_key(OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ)
            && options.get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION)
                == source_snapshot_location
    }

    // Stale MV read: bind the persisted logical definition against the live source table.
    fn bind_materialized_view_fallback(
        &mut self,
        bind_context: &mut BindContext,
        mv_definition: &MVDefinition,
        database: &str,
        table_name: &str,
        alias: &Option<TableAlias>,
    ) -> Result<(SExpr, BindContext)> {
        let query = parse_materialized_view_query(
            &mv_definition.original_query,
            "invalid materialized view logical query",
        )?;

        // Bind with the current binder so the source table enters this query's metadata.
        let mut definition_context = BindContext::with_parent(bind_context.clone())?;
        let (s_expr, mut logical_context) = self.bind_query(&mut definition_context, &query)?;
        if logical_context.columns.len() != mv_definition.logical_schema.num_fields() {
            return Err(ErrorCode::Internal(format!(
                "materialized view logical query has {} columns, expected {}",
                logical_context.columns.len(),
                mv_definition.logical_schema.num_fields()
            )));
        }
        for (column, field) in logical_context
            .columns
            .iter_mut()
            .zip(mv_definition.logical_schema.fields())
        {
            let expected_type = DataType::from(field.data_type());
            if column.data_type.as_ref() != &expected_type {
                return Err(ErrorCode::Internal(format!(
                    "materialized view logical column '{}' expects {}, query produces {}",
                    field.name(),
                    expected_type,
                    column.data_type
                )));
            }
            column.column_name = field.name().clone();
        }
        if let Some(alias) = alias {
            logical_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
        } else {
            for column in logical_context.columns.iter_mut() {
                column.database_name = Some(database.to_string());
                column.table_name = Some(table_name.to_string());
            }
        }
        Ok((s_expr, logical_context))
    }

    fn build_materialized_view_state_merge(
        &mut self,
        physical_context: &BindContext,
        child: SExpr,
    ) -> Result<(SExpr, BindContext)> {
        let physical_columns = physical_context
            .columns
            .iter()
            .filter(|column| column.visibility == Visibility::Visible)
            .cloned()
            .collect::<Vec<_>>();
        let mut aggregate_functions = Vec::new();
        let mut group_items = Vec::new();
        let mut output_columns = Vec::with_capacity(physical_columns.len());
        let mut saw_group_column = false;

        for column in physical_columns {
            let argument_type = *column.data_type.clone();
            if matches!(argument_type.remove_nullable(), DataType::AggregateState(_)) {
                if saw_group_column {
                    return Err(ErrorCode::InvalidMaterializedView(
                        "aggregate state columns must precede GROUP BY columns in materialized view storage",
                    ));
                }
                let DataType::AggregateState(state) = argument_type.remove_nullable() else {
                    unreachable!()
                };
                let merge_name = format!("{}_merge", state.function_name);
                let function = AggregateFunctionFactory::instance().get(
                    &merge_name,
                    vec![],
                    vec![argument_type.clone()],
                    vec![],
                )?;
                let return_type = function.return_type()?;
                let output = self
                    .create_derived_column_binding(column.column_name.clone(), return_type.clone());
                aggregate_functions.push(ScalarItem {
                    scalar: ScalarExpr::AggregateFunction(AggregateFunction {
                        span: None,
                        func_name: merge_name.clone(),
                        distinct: false,
                        params: vec![],
                        args: vec![ScalarExpr::BoundColumnRef(BoundColumnRef {
                            span: None,
                            column,
                        })],
                        return_type: Box::new(return_type),
                        sort_descs: vec![],
                        display_name: format!("{merge_name}({})", output.column_name),
                    }),
                    index: output.index,
                });
                output_columns.push(output);
            } else {
                saw_group_column = true;
                group_items.push(ScalarItem {
                    scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: column.clone(),
                    }),
                    index: column.index,
                });
                output_columns.push(column);
            }
        }

        let aggregate = Aggregate {
            mode: AggregateMode::Initial,
            group_items,
            aggregate_functions,
            from_distinct: false,
            rank_limit: None,
            grouping_sets: None,
        };
        let s_expr = SExpr::create_unary(Arc::new(aggregate.into()), Arc::new(child));
        let mut output_context = BindContext::new();
        output_context.parent = physical_context.parent.clone();
        output_context
            .cte_context
            .set_cte_context(physical_context.cte_context.clone());
        output_context.columns = output_columns;
        Ok((s_expr, output_context))
    }

    fn build_materialized_view_final_projection(
        &mut self,
        physical_context: &BindContext,
        logical_schema: &databend_common_expression::TableSchema,
        child: SExpr,
    ) -> Result<(SExpr, BindContext)> {
        let physical_columns = physical_context
            .columns
            .iter()
            .filter(|column| column.visibility == Visibility::Visible)
            .cloned()
            .collect::<Vec<_>>();
        let mut expression_context = BindContext::new();
        expression_context.columns = physical_columns;

        let mut items = Vec::with_capacity(logical_schema.num_fields());
        let mut output_columns = Vec::with_capacity(logical_schema.num_fields());
        for logical_field in logical_schema.fields() {
            // For MVDefinition.logical_schema only, default_expr stores the canonical finalized
            // projection expression rather than a table-column default value. A missing expression
            // is the compact representation of an identity projection from the same-named physical
            // column.
            let (scalar, scalar_type) = if let Some(final_expr) = logical_field.default_expr() {
                let tokens = tokenize_sql(final_expr)?;
                let ast = parse_expr(&tokens, Dialect::PostgreSQL)?;
                let mut scalar_binder = ScalarBinder::new(
                    &mut expression_context,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    self.metadata.clone(),
                    &[],
                );
                scalar_binder.forbid_udf();
                scalar_binder.bind(&ast)?
            } else {
                let column = expression_context
                    .columns
                    .iter()
                    .find(|column| column.column_name == *logical_field.name())
                    .cloned()
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "materialized view physical column '{}' does not exist; drop and recreate it",
                            logical_field.name()
                        ))
                    })?;
                let scalar_type = *column.data_type.clone();
                (
                    ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column }),
                    scalar_type,
                )
            };
            let logical_type = DataType::from(logical_field.data_type());
            if scalar_type != logical_type {
                return Err(ErrorCode::Internal(format!(
                    "materialized view logical column '{}' expects {}, final expression produces {}; drop and recreate it",
                    logical_field.name(),
                    logical_type,
                    scalar_type
                )));
            }
            let output =
                self.create_derived_column_binding(logical_field.name().to_string(), logical_type);
            items.push(ScalarItem {
                scalar,
                index: output.index,
            });
            output_columns.push(output);
        }

        let projection =
            SExpr::create_unary(Arc::new(EvalScalar { items }.into()), Arc::new(child));
        let mut output_context = BindContext::new();
        output_context.parent = physical_context.parent.clone();
        output_context
            .cte_context
            .set_cte_context(physical_context.cte_context.clone());
        output_context.columns = output_columns;
        Ok((projection, output_context))
    }

    fn apply_materialized_view_changes_query(
        query: &mut Query,
        changes_query: Query,
    ) -> Result<()> {
        let SetExpr::Select(select) = &mut query.body else {
            return Err(ErrorCode::Internal(
                "materialized view physical query must be a SELECT",
            ));
        };
        let [source] = select.from.as_mut_slice() else {
            return Err(ErrorCode::Internal(
                "materialized view hybrid read requires exactly one base table",
            ));
        };
        let alias = match source {
            TableReference::Table { alias, .. } => alias.clone(),
            _ => None,
        };
        *source = TableReference::Subquery {
            span: None,
            lateral: false,
            subquery: Box::new(changes_query),
            alias,
            pivot: None,
            unpivot: None,
        };

        Ok(())
    }

    fn is_materialized_view_hybrid_history_unavailable(error: &ErrorCode) -> bool {
        matches!(
            error.code(),
            ErrorCode::TABLE_HISTORICAL_DATA_NOT_FOUND
                | ErrorCode::STORAGE_NOT_FOUND
                | ErrorCode::ILLEGAL_STREAM
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn bind_materialized_view_hybrid(
        &mut self,
        bind_context: &mut BindContext,
        catalog_name: &str,
        database: &str,
        table_name: &str,
        table_name_alias: Option<String>,
        table_meta: Arc<dyn Table>,
        mv_definition: &MVDefinition,
        is_aggregating: bool,
        source_table: Arc<dyn Table>,
        source_database: &str,
        checkpoint_seq: u64,
        checkpoint_location: Option<String>,
        alias: &Option<TableAlias>,
        sample: &Option<SampleConfig>,
        cte_suffix_name: Option<String>,
    ) -> Result<Option<(SExpr, BindContext)>> {
        let internal_source_name = format!(
            "_mv_read_changes_{}_{}",
            table_meta.get_id(),
            checkpoint_seq
        );
        let mut checkpoint_info = TableInfo {
            ident: TableIdent::new(source_table.get_id(), checkpoint_seq),
            desc: format!("materialized view {} read checkpoint", table_meta.name()),
            name: internal_source_name.clone(),
            ..source_table.get_table_info().clone()
        };
        checkpoint_info.meta.options.insert(
            OPT_KEY_SOURCE_TABLE_ID.to_string(),
            source_table.get_id().to_string(),
        );
        checkpoint_info
            .meta
            .options
            .insert(OPT_KEY_TABLE_VER.to_string(), checkpoint_seq.to_string());
        match checkpoint_location {
            Some(location) => {
                checkpoint_info
                    .meta
                    .options
                    .insert(OPT_KEY_SNAPSHOT_LOCATION.to_string(), location);
            }
            None => {
                checkpoint_info
                    .meta
                    .options
                    .remove(OPT_KEY_SNAPSHOT_LOCATION);
            }
        }

        let changes_table = match databend_common_base::runtime::block_on(source_table.navigate_to(
            &self.ctx,
            &TimeNavigation::Changes {
                append_only: false,
                at: NavigationPoint::StreamInfo(checkpoint_info),
                end: None,
                desc: String::new(),
            },
        )) {
            Ok(table) => table,
            Err(error) if Self::is_materialized_view_hybrid_history_unavailable(&error) => {
                info!(
                    "materialized view {} cannot use hybrid read because its source checkpoint history is unavailable: {}; fallback to live compute",
                    table_meta.name(),
                    error
                );
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let changes_sql = match databend_common_base::runtime::block_on(
            changes_table.generate_changes_query(
                self.ctx.clone(),
                source_database,
                &internal_source_name,
                "",
            ),
        ) {
            Ok(sql) => sql,
            Err(error) if Self::is_materialized_view_hybrid_history_unavailable(&error) => {
                info!(
                    "materialized view {} cannot use hybrid read because its source changes are unavailable: {}; fallback to live compute",
                    table_meta.name(),
                    error
                );
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let mut changes_query = parse_materialized_view_query(
            &changes_sql,
            "invalid CHANGE_TRACKING query for materialized view hybrid read",
        )?;
        // SAMPLE belongs to the MV table reference. A hybrid MV is represented as storage UNION
        // live delta, so push the same scan sample into the generated delta query as well as the
        // persisted storage scan below. Otherwise an unbounded delta branch bypasses SAMPLE.
        changes_query
            .walk_mut(&mut MaterializedViewChangeScanSampler {
                table_name: &internal_source_name,
                sample,
            })
            .unwrap();
        let mut finder = MaterializedViewChangeScanFinder::default();
        changes_query.walk(&mut finder).unwrap();
        if finder.change_types.as_slice() != [ChangeType::Append] {
            return Ok(None);
        }

        self.pre_resolved_tables.insert(
            (
                catalog_name.to_string(),
                source_database.to_string(),
                internal_source_name,
            ),
            changes_table,
        );
        let mut delta_query = parse_materialized_view_query(
            &mv_definition.query,
            "invalid materialized view physical query",
        )?;
        Self::apply_materialized_view_changes_query(&mut delta_query, changes_query)?;
        let mut delta_context = BindContext::with_parent(bind_context.clone())?;
        let (delta_expr, delta_context) = self.bind_query(&mut delta_context, &delta_query)?;

        let table_index = self.metadata.write().add_table(
            catalog_name.to_string(),
            database.to_string(),
            table_meta,
            None,
            table_name_alias,
            !bind_context.binding_views.is_empty(),
            bind_context.planning_agg_index,
            false,
            cte_suffix_name,
        );
        let (storage_expr, storage_context) =
            self.bind_base_table(bind_context, database, table_index, None, sample, false)?;
        if storage_context.columns.len() != delta_context.columns.len() {
            return Err(ErrorCode::Internal(format!(
                "materialized view hybrid branches have {} and {} columns",
                storage_context.columns.len(),
                delta_context.columns.len()
            )));
        }
        let (union_expr, union_context) = self.bind_union(
            None,
            None,
            Some(bind_context),
            &storage_context,
            &delta_context,
            storage_expr,
            delta_expr,
            false,
            None,
        )?;
        let (union_expr, union_context) = if is_aggregating {
            self.build_materialized_view_state_merge(&union_context, union_expr)?
        } else {
            (union_expr, union_context)
        };
        let (union_expr, mut logical_context) = self.build_materialized_view_final_projection(
            &union_context,
            &mv_definition.logical_schema,
            union_expr,
        )?;
        if let Some(alias) = alias {
            logical_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
        } else {
            for column in logical_context.columns.iter_mut() {
                column.database_name = Some(database.to_string());
                column.table_name = Some(table_name.to_string());
            }
        }
        Ok(Some((union_expr, logical_context)))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn bind_materialized_view(
        &mut self,
        bind_context: &mut BindContext,
        catalog_name: &str,
        database: &str,
        table_name: &str,
        table_name_alias: Option<String>,
        table_meta: Arc<dyn Table>,
        alias: &Option<TableAlias>,
        sample: &Option<SampleConfig>,
        cte_suffix_name: Option<String>,
    ) -> Result<(SExpr, BindContext)> {
        let tenant = self.ctx.get_tenant();
        let source_table_id = table_meta
            .get_table_info()
            .meta
            .materialized_view_source_table_id()
            .map_err(ErrorCode::from)?;
        let mv_definition = databend_common_base::runtime::block_on(async {
            let catalog = self.ctx.get_catalog(catalog_name).await?;
            catalog
                .get_active_mv_definition(&tenant, source_table_id, table_meta.get_id())
                .await?
                .ok_or_else(|| {
                    ErrorCode::InvalidMaterializedView(format!(
                        "materialized view {} has an invalid source binding; recreate the materialized view",
                        table_meta.name()
                    ))
                })
        })?;

        // Bind the persisted physical definition to validate its source identity and determine
        // whether hybrid reads must merge aggregate states before the logical projection.
        let query = parse_materialized_view_query(
            &mv_definition.data.query,
            "invalid materialized view physical query",
        )?;
        let definition_metadata = Metadata::default_ref();
        let mut definition_binder = Binder::new(
            self.ctx.clone(),
            self.catalogs.clone(),
            self.name_resolution_ctx.clone(),
            definition_metadata.clone(),
        )
        .with_subquery_executor(self.subquery_executor.clone());
        let mut definition_context = BindContext::new();
        let (definition_expr, _) = definition_binder.bind_query(&mut definition_context, &query)?;
        validate_materialized_view_source(
            &definition_metadata,
            source_table_id,
            table_meta.name(),
        )?;
        let is_aggregating = find_materialized_view_aggregate(&definition_expr).is_some();

        let (source_table, source_database, source_seq, source_snapshot_location) =
            databend_common_base::runtime::block_on(async {
                let catalog = self.ctx.get_catalog(catalog_name).await?;
                let source_meta = catalog
                    .get_table_meta_by_id(source_table_id)
                    .await?
                    .ok_or_else(|| {
                        ErrorCode::InvalidMaterializedView(format!(
                            "materialized view {} source table changed: expected table id {} no longer exists",
                            table_meta.name(), source_table_id
                        ))
                    })?;
                let source_database_id = source_meta
                    .data
                    .options
                    .get(OPT_KEY_DATABASE_ID)
                    .ok_or_else(|| ErrorCode::Internal("source table database id is missing"))?
                    .parse::<u64>()?;
                let source_database = catalog.get_db_name_by_id(source_database_id).await?;
                let source_table_name = catalog
                    .get_table_name_by_id(source_table_id)
                    .await?
                    .ok_or_else(|| {
                        ErrorCode::InvalidMaterializedView(format!(
                            "materialized view {} source table changed: expected table id {} no longer exists",
                            table_meta.name(), source_table_id
                        ))
                    })?;
                let current_source = catalog
                    .get_table(&tenant, &source_database, &source_table_name)
                    .await?;
                let source_table = catalog.get_table_by_info(&TableInfo {
                    ident: TableIdent::new(source_table_id, source_meta.seq),
                    meta: source_meta.data.clone(),
                    ..current_source.get_table_info().clone()
                })?;
                let source_snapshot_location = source_meta
                    .data
                    .options
                    .get(OPT_KEY_SNAPSHOT_LOCATION)
                    .cloned();
                Ok::<_, ErrorCode>((
                    source_table,
                    source_database,
                    source_meta.seq,
                    source_snapshot_location,
                ))
            })?;
        // MV results depend on both persisted storage and the live source endpoint. Include both
        // identities in the result-cache key so a source append cannot reuse a previously fresh
        // storage-only result. Sequence values distinguish no-snapshot endpoints and metadata
        // revisions; snapshot locations identify data endpoints when present.
        self.ctx.result_cache_state().add_cache_key_extra(format!(
            "mv:{}:{}:{:?}|source:{}:{}:{:?}",
            table_meta.get_id(),
            table_meta.get_table_info().ident.seq,
            table_meta.options().get(OPT_KEY_SNAPSHOT_LOCATION),
            source_table_id,
            source_seq,
            source_snapshot_location
        ));
        if !Self::is_materialized_view_fresh(table_meta.as_ref(), source_snapshot_location.as_ref())
        {
            let checkpoint_seq = match table_meta
                .options()
                .get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ)
            {
                Some(seq) => seq.parse::<u64>().map_err(|error| {
                    ErrorCode::InvalidMaterializedView(format!(
                        "invalid materialized view source offset '{seq}': {error}"
                    ))
                })?,
                None => {
                    info!(
                        "materialized view {} has no source checkpoint; fallback to live compute",
                        table_meta.name()
                    );
                    return self.bind_materialized_view_fallback(
                        bind_context,
                        &mv_definition.data,
                        database,
                        table_name,
                        alias,
                    );
                }
            };
            if checkpoint_seq > source_seq {
                return Err(ErrorCode::InvalidMaterializedView(format!(
                    "materialized view {} offset {} is newer than source table version {}",
                    table_meta.name(),
                    checkpoint_seq,
                    source_seq
                )));
            }
            let checkpoint_location = table_meta
                .options()
                .get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION)
                .cloned();
            if let Some(result) = self.bind_materialized_view_hybrid(
                bind_context,
                catalog_name,
                database,
                table_name,
                table_name_alias.clone(),
                table_meta.clone(),
                &mv_definition.data,
                is_aggregating,
                source_table,
                &source_database,
                checkpoint_seq,
                checkpoint_location,
                alias,
                sample,
                cte_suffix_name.clone(),
            )? {
                info!(
                    "materialized view {} uses append-only hybrid read",
                    table_meta.name()
                );
                return Ok(result);
            }
            info!(
                "materialized view {} is stale (source_snapshot={:?}, base_snapshot={:?}); fallback to live compute",
                table_meta.name(),
                table_meta
                    .options()
                    .get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION),
                source_snapshot_location
            );
            return self.bind_materialized_view_fallback(
                bind_context,
                &mv_definition.data,
                database,
                table_name,
                alias,
            );
        }

        let table_index = self.metadata.write().add_table(
            catalog_name.to_string(),
            database.to_string(),
            table_meta,
            None,
            table_name_alias,
            !bind_context.binding_views.is_empty(),
            bind_context.planning_agg_index,
            false,
            cte_suffix_name,
        );

        let (s_expr, physical_context) =
            self.bind_base_table(bind_context, database, table_index, None, sample, false)?;
        let (s_expr, physical_context) = if is_aggregating {
            self.build_materialized_view_state_merge(&physical_context, s_expr)?
        } else {
            (s_expr, physical_context)
        };
        let (s_expr, mut logical_context) = self.build_materialized_view_final_projection(
            &physical_context,
            &mv_definition.data.logical_schema,
            s_expr,
        )?;
        if let Some(alias) = alias {
            logical_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
        } else {
            for column in logical_context.columns.iter_mut() {
                column.database_name = Some(database.to_string());
                column.table_name = Some(table_name.to_string());
            }
        }
        Ok((s_expr, logical_context))
    }
}
