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
use std::default::Default;
use std::sync::Arc;

use chrono::TimeZone;
use chrono::Utc;
use dashmap::DashMap;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Indirection;
use databend_common_ast::ast::SampleConfig;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::SetOperator;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::TemporalClause;
use databend_common_ast::ast::TimeTravelPoint;
use databend_common_ast::Span;
use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TimeNavigation;
use databend_common_catalog::table_context::AbortChecker;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::is_stream_column;
use databend_common_expression::type_check::check_number;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::ListIndexesReq;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::MetaId;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;
use databend_storages_common_table_meta::table::ChangeType;
use log::info;

use crate::binder::Binder;
use crate::binder::ColumnBindingBuilder;
use crate::binder::CteInfo;
use crate::binder::ExprContext;
use crate::binder::Visibility;
use crate::optimizer::ir::SExpr;
use crate::planner::semantic::normalize_identifier;
use crate::planner::semantic::TypeChecker;
use crate::plans::DummyTableScan;
use crate::plans::RecursiveCteScan;
use crate::plans::RelOperator;
use crate::plans::Scan;
use crate::plans::Statistics;
use crate::BaseTableColumn;
use crate::BindContext;
use crate::ColumnEntry;
use crate::IndexType;

impl Binder {
    pub fn bind_dummy_table(
        &mut self,
        bind_context: &BindContext,
        select_list: &Vec<SelectTarget>,
    ) -> Result<(SExpr, BindContext)> {
        for select_target in select_list {
            if let SelectTarget::StarColumns {
                qualified: names, ..
            } = select_target
            {
                for indirect in names {
                    if let Indirection::Star(span) = indirect {
                        return Err(ErrorCode::SemanticError(
                            "'SELECT *' is used without specifying any tables in the FROM clause."
                                .to_string(),
                        )
                        .set_span(*span));
                    }
                }
            }
        }
        let bind_context = BindContext::with_parent(bind_context.clone())?;
        Ok((
            SExpr::create_leaf(Arc::new(DummyTableScan.into())),
            bind_context,
        ))
    }

    #[async_backtrace::framed]
    pub(crate) async fn bind_stage_table(
        &mut self,
        table_ctx: Arc<dyn TableContext>,
        bind_context: &BindContext,
        stage_info: StageInfo,
        files_info: StageFilesInfo,
        alias: &Option<TableAlias>,
        files_to_copy: Option<Vec<StageFileInfo>>,
        case_sensitive: bool,
    ) -> Result<(SExpr, BindContext)> {
        let start = std::time::Instant::now();
        let max_column_position = self.metadata.read().get_max_column_position();
        let table = table_ctx
            .create_stage_table(
                stage_info,
                files_info,
                files_to_copy,
                max_column_position,
                case_sensitive,
            )
            .await?;

        let table_alias_name = if let Some(table_alias) = alias {
            Some(normalize_identifier(&table_alias.name, &self.name_resolution_ctx).name)
        } else {
            None
        };

        let table_index = self.metadata.write().add_table(
            CATALOG_DEFAULT.to_string(),
            "system".to_string(),
            table.clone(),
            table_alias_name,
            false,
            false,
            true,
            None,
        );

        let (s_expr, mut bind_context) =
            self.bind_base_table(bind_context, "system", table_index, None, &None)?;
        if let Some(alias) = alias {
            bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
        }

        info!("bind_stage_table cost: {:?}", start.elapsed());
        Ok((s_expr, bind_context))
    }

    pub(crate) fn bind_cte(
        &mut self,
        span: Span,
        bind_context: &mut BindContext,
        table_name: &str,
        alias: &Option<TableAlias>,
        cte_info: &CteInfo,
    ) -> Result<(SExpr, BindContext)> {
        if let Some(cte_name) = &bind_context.cte_context.cte_name {
            if cte_name == table_name {
                return Err(ErrorCode::SemanticError(format!(
                    "The cte {table_name} is not recursive, but it references itself.",
                ))
                .set_span(span));
            }
        }

        let mut new_bind_context = BindContext {
            parent: Some(Box::new(bind_context.clone())),
            bound_internal_columns: BTreeMap::new(),
            columns: vec![],
            aggregate_info: Default::default(),
            windows: Default::default(),
            srf_info: Default::default(),
            cte_context: bind_context.cte_context.clone(),
            in_grouping: false,
            view_info: None,
            have_async_func: false,
            have_udf_script: false,
            have_udf_server: false,
            inverted_index_map: Box::default(),
            virtual_column_context: Default::default(),
            expr_context: ExprContext::default(),
            planning_agg_index: false,
            window_definitions: DashMap::new(),
        };

        new_bind_context.cte_context.cte_name = Some(table_name.to_string());

        let (s_expr, mut res_bind_context) =
            self.bind_query(&mut new_bind_context, &cte_info.query)?;
        let mut cols_alias = cte_info.columns_alias.clone();
        if let Some(alias) = alias {
            for (idx, col_alias) in alias.columns.iter().enumerate() {
                if idx < cte_info.columns_alias.len() {
                    cols_alias[idx] = col_alias.name.clone();
                } else {
                    cols_alias.push(col_alias.name.clone());
                }
            }
        }
        let alias_table_name = alias
            .as_ref()
            .map(|alias| normalize_identifier(&alias.name, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| table_name.to_string());
        for column in res_bind_context.columns.iter_mut() {
            column.database_name = None;
            column.table_name = Some(alias_table_name.clone());
        }

        if cols_alias.len() > res_bind_context.columns.len() {
            return Err(ErrorCode::SemanticError(format!(
                "The CTE '{}' has {} columns, but {} aliases were provided. Ensure the number of aliases matches the number of columns in the CTE.",
                table_name,
                res_bind_context.columns.len(),
                cols_alias.len()
            ))
                .set_span(span));
        }
        for (index, column_name) in cols_alias.iter().enumerate() {
            res_bind_context.columns[index].column_name = column_name.clone();
        }
        Ok((s_expr, res_bind_context))
    }

    pub(crate) fn bind_r_cte_scan(
        &mut self,
        bind_context: &mut BindContext,
        cte_info: &CteInfo,
        cte_name: &str,
        alias: &Option<TableAlias>,
    ) -> Result<(SExpr, BindContext)> {
        let mut new_bind_ctx = BindContext::with_parent(bind_context.clone())?;
        let mut metadata = self.metadata.write();
        let mut columns = cte_info.columns.clone();
        for (index, column_name) in cte_info.columns_alias.iter().enumerate() {
            columns[index].column_name = column_name.clone();
        }
        for col in columns.iter() {
            // Expand a number type to a higher precision to avoid overflow
            // (Because the output type of recursive cte is the left of the union)
            let expand_data_type = match *col.data_type {
                DataType::Number(NumberDataType::UInt8)
                | DataType::Number(NumberDataType::UInt16)
                | DataType::Number(NumberDataType::UInt32) => {
                    Box::new(DataType::Number(NumberDataType::UInt64))
                }
                DataType::Number(NumberDataType::Int8)
                | DataType::Number(NumberDataType::Int16)
                | DataType::Number(NumberDataType::Int32) => {
                    Box::new(DataType::Number(NumberDataType::Int64))
                }
                _ => col.data_type.clone(),
            };
            let idx = metadata.add_derived_column(
                col.column_name.clone(),
                *expand_data_type.clone(),
                None,
            );
            new_bind_ctx.columns.push(
                ColumnBindingBuilder::new(
                    col.column_name.clone(),
                    idx,
                    expand_data_type,
                    Visibility::Visible,
                )
                .table_name(col.table_name.clone())
                .build(),
            )
        }
        let mut fields = Vec::with_capacity(cte_info.columns.len());
        for col in new_bind_ctx.columns.iter() {
            fields.push(DataField::new(
                col.index.to_string().as_str(),
                *col.data_type.clone(),
            ));
        }
        // Update the cte_info of the recursive cte
        bind_context
            .cte_context
            .cte_map
            .entry(cte_name.to_string())
            .and_modify(|cte_info| {
                cte_info.columns = new_bind_ctx.columns.clone();
            });
        if let Some(alias) = alias {
            new_bind_ctx.apply_table_alias(alias, &self.name_resolution_ctx)?;
        }

        let table_alias_name = alias
            .as_ref()
            .map(|table_alias| self.normalize_identifier(&table_alias.name).name);
        let table_name = if let Some(table_alias_name) = table_alias_name {
            table_alias_name
        } else {
            cte_name.to_string()
        };

        Ok((
            SExpr::create_leaf(Arc::new(RelOperator::RecursiveCteScan(RecursiveCteScan {
                fields,
                table_name,
            }))),
            new_bind_ctx,
        ))
    }

    pub(crate) fn bind_r_cte(
        &mut self,
        span: Span,
        bind_context: &mut BindContext,
        cte_info: &CteInfo,
        cte_name: &str,
        alias: &Option<TableAlias>,
    ) -> Result<(SExpr, BindContext)> {
        match &cte_info.query.body {
            SetExpr::SetOperation(set_expr) => {
                if set_expr.op != SetOperator::Union {
                    return Err(ErrorCode::SyntaxException(
                        "Recursive CTE must contain a UNION(ALL) query".to_string(),
                    ));
                }
                self.set_bind_recursive_cte(true);
                let (union_s_expr, mut new_bind_ctx) = self.bind_set_operator(
                    bind_context,
                    &set_expr.left,
                    &set_expr.right,
                    &set_expr.op,
                    &set_expr.all,
                    Some(cte_name.to_string()),
                )?;
                self.set_bind_recursive_cte(false);
                if let Some(alias) = alias {
                    new_bind_ctx.apply_table_alias(alias, &self.name_resolution_ctx)?;
                } else {
                    for (index, column_name) in cte_info.columns_alias.iter().enumerate() {
                        new_bind_ctx.columns[index].column_name = column_name.clone();
                    }
                }
                Ok((union_s_expr, new_bind_ctx.clone()))
            }
            _ => self.bind_cte(span, bind_context, cte_name, alias, cte_info),
        }
    }

    pub(crate) fn bind_base_table(
        &mut self,
        bind_context: &BindContext,
        database_name: &str,
        table_index: IndexType,
        change_type: Option<ChangeType>,
        sample: &Option<SampleConfig>,
    ) -> Result<(SExpr, BindContext)> {
        let mut bind_context = BindContext::with_parent(bind_context.clone())?;

        let table = self.metadata.read().table(table_index).clone();
        let table_name = table.name();
        let columns = self.metadata.read().columns_by_table_index(table_index);
        let scan_id = self.metadata.write().next_scan_id();
        let mut base_column_scan_id = HashMap::new();
        for column in columns.iter() {
            match column {
                ColumnEntry::BaseTableColumn(BaseTableColumn {
                    column_name,
                    column_index,
                    path_indices,
                    data_type,
                    table_index,
                    column_position,
                    virtual_expr,
                    ..
                }) => {
                    let column_binding = ColumnBindingBuilder::new(
                        column_name.clone(),
                        *column_index,
                        Box::new(DataType::from(data_type)),
                        if path_indices.is_some() || is_stream_column(column_name) {
                            Visibility::InVisible
                        } else {
                            Visibility::Visible
                        },
                    )
                    .table_name(Some(table_name.to_string()))
                    .database_name(Some(database_name.to_string()))
                    .table_index(Some(*table_index))
                    .column_position(*column_position)
                    .virtual_expr(virtual_expr.clone())
                    .build();
                    bind_context.add_column_binding(column_binding);
                    base_column_scan_id.insert(*column_index, scan_id);
                }
                other => {
                    return Err(ErrorCode::Internal(format!(
                        "Invalid column entry '{:?}' encountered while binding the base table '{}'. Ensure that the table definition and column references are correct.",
                        other.name(),
                        table_name
                    )));
                }
            }
        }
        self.metadata
            .write()
            .add_base_column_scan_id(base_column_scan_id);

        Ok((
            SExpr::create_leaf(Arc::new(
                Scan {
                    table_index,
                    columns: columns.into_iter().map(|col| col.index()).collect(),
                    statistics: Arc::new(Statistics::default()),
                    change_type,
                    sample: sample.clone(),
                    scan_id,
                    ..Default::default()
                }
                .into(),
            )),
            bind_context,
        ))
    }

    pub fn resolve_data_source(
        &self,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        navigation: Option<&TimeNavigation>,
        max_batch_size: Option<u64>,
        abort_checker: AbortChecker,
    ) -> Result<Arc<dyn Table>> {
        databend_common_base::runtime::block_on(async move {
            // Resolve table with ctx
            // for example: select * from t1 join (select * from t1 as t2 where a > 1 and a < 13);
            // we will invoke here twice for t1, so in the past, we use catalog every time to get the
            // newest snapshot, we can't get consistent snapshot
            let mut table_meta = self
                .ctx
                .get_table_with_batch(catalog_name, database_name, table_name, max_batch_size)
                .await?;

            if let Some(desc) = navigation {
                table_meta = table_meta.navigate_to(desc, abort_checker).await?;
            }
            Ok(table_meta)
        })
    }

    pub(crate) fn resolve_temporal_clause(
        &self,
        bind_context: &mut BindContext,
        temporal: &Option<TemporalClause>,
    ) -> Result<Option<TimeNavigation>> {
        match temporal {
            Some(TemporalClause::TimeTravel(point)) => {
                let point = self.resolve_data_travel_point(bind_context, point)?;
                Ok(Some(TimeNavigation::TimeTravel(point)))
            }
            Some(TemporalClause::Changes(interval)) => {
                let end = match &interval.end_point {
                    Some(tp) => Some(self.resolve_data_travel_point(bind_context, tp)?),
                    None => None,
                };
                let at = self.resolve_data_travel_point(bind_context, &interval.at_point)?;
                Ok(Some(TimeNavigation::Changes {
                    append_only: interval.append_only,
                    at,
                    end,
                    desc: format!("{interval}"),
                }))
            }
            None => Ok(None),
        }
    }

    pub(crate) fn resolve_data_travel_point(
        &self,
        bind_context: &mut BindContext,
        travel_point: &TimeTravelPoint,
    ) -> Result<NavigationPoint> {
        match travel_point {
            TimeTravelPoint::Snapshot(s) => Ok(NavigationPoint::SnapshotID(s.to_owned())),
            TimeTravelPoint::Timestamp(expr) => {
                let mut type_checker = TypeChecker::try_create(
                    bind_context,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    self.metadata.clone(),
                    &[],
                    false,
                )?;
                let box (scalar, _) = type_checker.resolve(expr)?;
                let scalar_expr = scalar.as_expr()?;

                let (new_expr, _) = ConstantFolder::fold(
                    &scalar_expr,
                    &self.ctx.get_function_context()?,
                    &BUILTIN_FUNCTIONS,
                );

                match new_expr {
                    databend_common_expression::Expr::Constant(Constant {
                        scalar,
                        data_type: DataType::Timestamp,
                        ..
                    }) => {
                        let value = scalar.as_timestamp().unwrap();
                        Ok(NavigationPoint::TimePoint(
                            Utc.timestamp_nanos(*value * 1000),
                        ))
                    }

                    _ => Err(ErrorCode::InvalidArgument(format!(
                        "TimeTravelPoint for 'Timestamp' must resolve to a constant timestamp value. \
                        Provided expression '{}' is not a constant timestamp. \
                        Ensure the expression is a constant and of type timestamp",
                        expr
                    ))),
                }
            }
            TimeTravelPoint::Offset(expr) => {
                let mut type_checker = TypeChecker::try_create(
                    bind_context,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    self.metadata.clone(),
                    &[],
                    false,
                )?;
                let box (scalar, _) = type_checker.resolve(expr)?;
                let scalar_expr = scalar.as_expr()?;

                let (new_expr, _) = ConstantFolder::fold(
                    &scalar_expr,
                    &self.ctx.get_function_context()?,
                    &BUILTIN_FUNCTIONS,
                );

                let v: i64 = check_number(
                    None,
                    &FunctionContext::default(),
                    &new_expr,
                    &BUILTIN_FUNCTIONS,
                )?;
                if v > 0 {
                    return Err(ErrorCode::InvalidArgument(format!(
                        "TimeTravelPoint for 'Offset' must resolve to a constant negative integer. \
                        Provided expression '{}' does not meet this requirement. \
                        Ensure the expression is a constant and negative integer",
                        expr
                    )));
                }
                let micros = Utc::now().timestamp_micros() + v * 1_000_000;
                Ok(NavigationPoint::TimePoint(
                    Utc.timestamp_nanos(micros * 1000),
                ))
            }
            TimeTravelPoint::Stream {
                catalog,
                database,
                name,
            } => self.resolve_stream_data_travel_point(catalog, database, name),
        }
    }

    fn resolve_stream_data_travel_point(
        &self,
        catalog: &Option<Identifier>,
        database: &Option<Identifier>,
        name: &Identifier,
    ) -> Result<NavigationPoint> {
        let (catalog, database, name) =
            self.normalize_object_identifier_triple(catalog, database, name);
        databend_common_base::runtime::block_on(async move {
            let stream = self.ctx.get_table(&catalog, &database, &name).await?;
            if !stream.is_stream() {
                return Err(ErrorCode::TableEngineNotSupported(format!(
                    "{database}.{name} is not STREAM",
                )));
            }
            let info = stream.get_table_info().clone();
            Ok(NavigationPoint::StreamInfo(info))
        })
    }

    #[async_backtrace::framed]
    pub(crate) async fn resolve_table_indexes(
        &self,
        tenant: &Tenant,
        catalog_name: &str,
        table_id: MetaId,
    ) -> Result<Vec<(u64, String, IndexMeta)>> {
        let catalog = self
            .catalogs
            .get_catalog(tenant.tenant_name(), catalog_name, self.ctx.session_state())
            .await?;
        let index_metas = catalog
            .list_indexes(ListIndexesReq::new(tenant, Some(table_id)))
            .await?;

        Ok(index_metas)
    }
}
