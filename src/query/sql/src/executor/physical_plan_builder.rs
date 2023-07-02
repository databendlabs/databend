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

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_catalog::catalog::CatalogManager;
use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_catalog::plan::AggIndexInfo;
use common_catalog::plan::PrewhereInfo;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::plan::VirtualColumnInfo;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check;
use common_expression::type_check::check_function;
use common_expression::type_check::common_super_type;
use common_expression::types::DataType;
use common_expression::ConstantFolder;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRefExt;
use common_expression::FunctionContext;
use common_expression::RawExpr;
use common_expression::RemoteExpr;
use common_expression::TableSchema;
use common_expression::ROW_ID_COL_NAME;
use common_functions::BUILTIN_FUNCTIONS;
use itertools::Itertools;

use super::cast_expr_to_non_null_boolean;
use super::AggregateExpand;
use super::AggregateFinal;
use super::AggregateFunctionDesc;
use super::AggregateFunctionSignature;
use super::AggregatePartial;
use super::EvalScalar;
use super::Exchange as PhysicalExchange;
use super::Filter;
use super::Limit;
use super::NthValueFunctionDesc;
use super::ProjectSet;
use super::RowFetch;
use super::Sort;
use super::TableScan;
use super::WindowFunction;
use crate::binder::wrap_cast;
use crate::binder::INTERNAL_COLUMN_FACTORY;
use crate::executor::explain::PlanStatsInfo;
use crate::executor::physical_join;
use crate::executor::table_read_plan::ToReadDataSourcePlan;
use crate::executor::FragmentKind;
use crate::executor::LagLeadDefault;
use crate::executor::LagLeadFunctionDesc;
use crate::executor::NtileFunctionDesc;
use crate::executor::PhysicalJoinType;
use crate::executor::PhysicalPlan;
use crate::executor::RuntimeFilterSource;
use crate::executor::SortDesc;
use crate::executor::UnionAll;
use crate::executor::Window;
use crate::optimizer::ColumnSet;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::planner;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::Exchange;
use crate::plans::FunctionCall;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::Scan;
use crate::plans::Window as LogicalWindow;
use crate::plans::WindowFuncFrameBound;
use crate::plans::WindowFuncType;
use crate::BaseTableColumn;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::DerivedColumn;
use crate::IndexType;
use crate::Metadata;
use crate::MetadataRef;
use crate::TableInternalColumn;
use crate::TypeCheck;
use crate::VirtualColumn;
use crate::Visibility;
use crate::DUMMY_COLUMN_INDEX;
use crate::DUMMY_TABLE_INDEX;

pub struct PhysicalPlanBuilder {
    metadata: MetadataRef,
    ctx: Arc<dyn TableContext>,
    pub(crate) func_ctx: FunctionContext,

    next_plan_id: u32,
    dry_run: bool,
}

impl PhysicalPlanBuilder {
    pub fn new(metadata: MetadataRef, ctx: Arc<dyn TableContext>, dry_run: bool) -> Self {
        let func_ctx = ctx.get_function_context().unwrap();
        Self {
            metadata,
            ctx,
            next_plan_id: 0,
            func_ctx,
            dry_run,
        }
    }

    pub(crate) fn next_plan_id(&mut self) -> u32 {
        let id = self.next_plan_id;
        self.next_plan_id += 1;
        id
    }

    fn build_projection<'a>(
        metadata: &Metadata,
        schema: &TableSchema,
        columns: impl Iterator<Item = &'a IndexType>,
        has_inner_column: bool,
        ignore_internal_column: bool,
        add_virtual_source_column: bool,
        ignore_lazy_column: bool,
    ) -> Projection {
        if !has_inner_column {
            let mut col_indices = Vec::new();
            let mut virtual_col_indices = HashSet::new();
            for index in columns {
                if ignore_lazy_column && metadata.is_lazy_column(*index) {
                    continue;
                }
                let name = match metadata.column(*index) {
                    ColumnEntry::BaseTableColumn(BaseTableColumn { column_name, .. }) => {
                        column_name
                    }
                    ColumnEntry::DerivedColumn(DerivedColumn { alias, .. }) => alias,
                    ColumnEntry::InternalColumn(TableInternalColumn {
                        internal_column, ..
                    }) => {
                        if ignore_internal_column {
                            continue;
                        }
                        internal_column.column_name()
                    }
                    ColumnEntry::VirtualColumn(VirtualColumn {
                        source_column_name, ..
                    }) => {
                        if add_virtual_source_column {
                            virtual_col_indices
                                .insert(schema.index_of(source_column_name).unwrap());
                        }
                        continue;
                    }
                };
                col_indices.push(schema.index_of(name).unwrap());
            }
            if !virtual_col_indices.is_empty() {
                for index in virtual_col_indices {
                    if !col_indices.contains(&index) {
                        col_indices.push(index);
                    }
                }
            }
            col_indices.sort();
            Projection::Columns(col_indices)
        } else {
            let mut col_indices = BTreeMap::new();
            for index in columns {
                if ignore_lazy_column && metadata.is_lazy_column(*index) {
                    continue;
                }
                let column = metadata.column(*index);
                match column {
                    ColumnEntry::BaseTableColumn(BaseTableColumn {
                        column_name,
                        path_indices,
                        ..
                    }) => match path_indices {
                        Some(path_indices) => {
                            col_indices.insert(column.index(), path_indices.to_vec());
                        }
                        None => {
                            let idx = schema.index_of(column_name).unwrap();
                            col_indices.insert(column.index(), vec![idx]);
                        }
                    },
                    ColumnEntry::DerivedColumn(DerivedColumn { alias, .. }) => {
                        let idx = schema.index_of(alias).unwrap();
                        col_indices.insert(column.index(), vec![idx]);
                    }
                    ColumnEntry::InternalColumn(TableInternalColumn { column_index, .. }) => {
                        if !ignore_internal_column {
                            col_indices.insert(*column_index, vec![*column_index]);
                        }
                    }
                    ColumnEntry::VirtualColumn(VirtualColumn {
                        source_column_name, ..
                    }) => {
                        if add_virtual_source_column {
                            let idx = schema.index_of(source_column_name).unwrap();
                            col_indices.insert(idx, vec![idx]);
                        }
                    }
                }
            }
            Projection::InnerColumns(col_indices)
        }
    }

    fn build_limit(
        &mut self,
        input_plan: PhysicalPlan,
        limit: &planner::plans::Limit,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let next_plan_id = self.next_plan_id();
        let metadata = self.metadata.read().clone();
        if metadata.lazy_columns().is_empty() {
            return Ok(PhysicalPlan::Limit(Limit {
                plan_id: next_plan_id,
                input: Box::new(input_plan),
                limit: limit.limit,
                offset: limit.offset,
                stat_info: Some(stat_info),
            }));
        }

        // If `lazy_columns` is not empty, build a `RowFetch` plan on top of the `Limit` plan.

        let input_schema = input_plan.output_schema()?;

        // Lazy materialization is enabled.
        let row_id_col_index = metadata
            .columns()
            .iter()
            .position(|col| col.name() == ROW_ID_COL_NAME)
            .ok_or_else(|| ErrorCode::Internal("Internal column _row_id is not found"))?;
        let row_id_col_offset = input_schema.index_of(&row_id_col_index.to_string())?;

        // There may be more than one `LIMIT` plan, we don't need to fetch the same columns multiple times.
        // See the case in tests/sqllogictests/suites/crdb/limit:
        // SELECT * FROM (SELECT * FROM t_47283 ORDER BY k LIMIT 4) WHERE a > 5 LIMIT 1
        let lazy_columns = metadata
            .lazy_columns()
            .iter()
            .sorted() // Needs sort because we need to make the order deterministic.
            .filter(|index| !input_schema.has_field(&index.to_string())) // If the column is already in the input schema, we don't need to fetch it.
            .cloned()
            .collect::<Vec<_>>();

        if lazy_columns.is_empty() {
            // If there is no lazy column, we don't need to build a `RowFetch` plan.
            return Ok(PhysicalPlan::Limit(Limit {
                plan_id: next_plan_id,
                input: Box::new(input_plan),
                limit: limit.limit,
                offset: limit.offset,
                stat_info: Some(stat_info),
            }));
        }

        let mut has_inner_column = false;
        let fetched_fields = lazy_columns
            .iter()
            .map(|index| {
                let col = metadata.column(*index);
                if let ColumnEntry::BaseTableColumn(c) = col {
                    if c.path_indices.is_some() {
                        has_inner_column = true;
                    }
                }
                DataField::new(&index.to_string(), col.data_type())
            })
            .collect();

        let source = input_plan.try_find_single_data_source();
        debug_assert!(source.is_some());
        let source_info = source.cloned().unwrap();
        let table_schema = source_info.source_info.schema();
        let cols_to_fetch = Self::build_projection(
            &metadata,
            &table_schema,
            lazy_columns.iter(),
            has_inner_column,
            true,
            true,
            false,
        );

        Ok(PhysicalPlan::RowFetch(RowFetch {
            plan_id: self.next_plan_id(),
            input: Box::new(PhysicalPlan::Limit(Limit {
                plan_id: next_plan_id,
                input: Box::new(input_plan),
                limit: limit.limit,
                offset: limit.offset,
                stat_info: Some(stat_info.clone()),
            })),
            source: Box::new(source_info),
            row_id_col_offset,
            cols_to_fetch,
            fetched_fields,
            stat_info: Some(stat_info),
        }))
    }

    #[async_backtrace::framed]
    async fn build_scan(&mut self, scan: &Scan, stat_info: PlanStatsInfo) -> Result<PhysicalPlan> {
        let mut has_inner_column = false;
        let mut has_virtual_column = false;
        let mut name_mapping = BTreeMap::new();
        let mut project_internal_columns = BTreeMap::new();
        let metadata = self.metadata.read().clone();

        for index in scan.columns.iter() {
            if metadata.is_lazy_column(*index) {
                continue;
            }
            let column = metadata.column(*index);
            if let ColumnEntry::BaseTableColumn(BaseTableColumn { path_indices, .. }) = column {
                if path_indices.is_some() {
                    has_inner_column = true;
                }
            } else if let ColumnEntry::InternalColumn(TableInternalColumn {
                internal_column, ..
            }) = column
            {
                project_internal_columns.insert(*index, internal_column.to_owned());
            } else if let ColumnEntry::VirtualColumn(_) = column {
                has_virtual_column = true;
            }

            if let Some(prewhere) = &scan.prewhere {
                // if there is a prewhere optimization,
                // we can prune `PhysicalScan`'s output schema.
                if prewhere.output_columns.contains(index) {
                    name_mapping.insert(column.name().to_string(), *index);
                }
            } else {
                name_mapping.insert(column.name().to_string(), *index);
            }
        }

        if !metadata.lazy_columns().is_empty() {
            // Lazy materialization is enabled.
            if let Entry::Vacant(entry) = name_mapping.entry(ROW_ID_COL_NAME.to_string()) {
                let internal_column = INTERNAL_COLUMN_FACTORY
                    .get_internal_column(ROW_ID_COL_NAME)
                    .unwrap();
                let index = self
                    .metadata
                    .write()
                    .add_internal_column(scan.table_index, internal_column.clone());
                entry.insert(index);
                project_internal_columns.insert(index, internal_column);
            }
        }

        let table_entry = metadata.table(scan.table_index);
        let table = table_entry.table();

        if !table.result_can_be_cached() {
            self.ctx.set_cacheable(false);
        }

        let mut table_schema = table.schema();
        if !project_internal_columns.is_empty() {
            let mut schema = table_schema.as_ref().clone();
            for internal_column in project_internal_columns.values() {
                schema.add_internal_field(
                    internal_column.column_name(),
                    internal_column.table_data_type(),
                    internal_column.column_id(),
                );
            }
            table_schema = Arc::new(schema);
        }

        let push_downs =
            self.push_downs(scan, &table_schema, has_inner_column, has_virtual_column)?;

        let mut source = table
            .read_plan_with_catalog(
                self.ctx.clone(),
                table_entry.catalog().to_string(),
                Some(push_downs),
                if project_internal_columns.is_empty() {
                    None
                } else {
                    Some(project_internal_columns.clone())
                },
                self.dry_run,
            )
            .await?;

        if let Some(agg_index) = &scan.agg_index {
            let schema = source.schema();
            let push_down = source.push_downs.as_mut().unwrap();
            let output_fields = TableScan::output_fields(schema, &name_mapping)?;
            Self::push_down_agg_index(push_down, agg_index, &output_fields)?;
        }
        let internal_column = if project_internal_columns.is_empty() {
            None
        } else {
            Some(project_internal_columns)
        };
        Ok(PhysicalPlan::TableScan(TableScan {
            plan_id: self.next_plan_id(),
            name_mapping,
            source: Box::new(source),
            table_index: scan.table_index,
            stat_info: Some(stat_info),
            internal_column,
        }))
    }

    fn push_down_agg_index(
        push_down: &mut PushDownInfo,
        agg: &planner::plans::AggIndexInfo,
        output_fields: &[DataField],
    ) -> Result<()> {
        let predicate = agg.predicates.iter().cloned().reduce(|lhs, rhs| {
            ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "and".to_string(),
                params: vec![],
                arguments: vec![lhs, rhs],
            })
        });
        let filter = predicate
            .map(|pred| -> Result<_> {
                Ok(cast_expr_to_non_null_boolean(
                    pred.as_expr()?.project_column_ref(|col| col.index),
                )?
                .as_remote_expr())
            })
            .transpose()?;
        let selection = agg
            .selection
            .iter()
            .map(|sel| {
                let offset = output_fields
                    .iter()
                    .position(|f| sel.index.to_string() == f.name().as_str());
                Ok((
                    sel.scalar
                        .as_expr()?
                        .project_column_ref(|col| col.index)
                        .as_remote_expr(),
                    offset,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let agg_info = AggIndexInfo {
            index_id: agg.index_id,
            filter,
            selection,
            schema: agg.schema.clone(),
            actual_table_field_len: output_fields.len(),
        };
        push_down.agg_index = Some(agg_info);

        Ok(())
    }

    #[async_recursion::async_recursion]
    #[async_backtrace::framed]
    pub async fn build(&mut self, s_expr: &SExpr) -> Result<PhysicalPlan> {
        // Build stat info
        let stat_info = self.build_plan_stat_info(s_expr)?;

        match s_expr.plan() {
            RelOperator::Scan(scan) => self.build_scan(scan, stat_info).await,
            RelOperator::DummyTableScan(_) => {
                let catalogs = CatalogManager::instance();
                let table = catalogs
                    .get_catalog(CATALOG_DEFAULT)?
                    .get_table(self.ctx.get_tenant().as_str(), "system", "one")
                    .await?;

                if !table.result_can_be_cached() {
                    self.ctx.set_cacheable(false);
                }

                let source = table
                    .read_plan_with_catalog(
                        self.ctx.clone(),
                        CATALOG_DEFAULT.to_string(),
                        None,
                        None,
                        self.dry_run,
                    )
                    .await?;
                Ok(PhysicalPlan::TableScan(TableScan {
                    plan_id: self.next_plan_id(),
                    name_mapping: BTreeMap::from([("dummy".to_string(), DUMMY_COLUMN_INDEX)]),
                    source: Box::new(source),
                    table_index: DUMMY_TABLE_INDEX,
                    stat_info: Some(PlanStatsInfo {
                        estimated_rows: 1.0,
                    }),
                    internal_column: None,
                }))
            }
            RelOperator::Join(join) => {
                // Choose physical join type by join conditions
                let physical_join = physical_join(join, s_expr)?;
                match physical_join {
                    PhysicalJoinType::Hash => self.build_hash_join(join, s_expr, stat_info).await,
                    PhysicalJoinType::RangeJoin(range, other) => {
                        self.build_range_join(range, other, s_expr).await
                    }
                }
            }

            RelOperator::EvalScalar(eval_scalar) => {
                let input = self.build(s_expr.child(0)?).await?;
                self.build_eval_scalar(input, eval_scalar, stat_info)
            }

            RelOperator::Filter(filter) => {
                let input = Box::new(self.build(s_expr.child(0)?).await?);
                let input_schema = input.output_schema()?;
                Ok(PhysicalPlan::Filter(Filter {
                    plan_id: self.next_plan_id(),
                    input,
                    predicates: filter
                        .predicates
                        .iter()
                        .map(|scalar| {
                            let expr = scalar
                                .resolve_and_check(input_schema.as_ref())?
                                .project_column_ref(|index| {
                                    input_schema.index_of(&index.to_string()).unwrap()
                                });
                            let expr = cast_expr_to_non_null_boolean(expr)?;
                            let (expr, _) =
                                ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                            Ok(expr.as_remote_expr())
                        })
                        .collect::<Result<_>>()?,

                    stat_info: Some(stat_info),
                }))
            }

            RelOperator::Aggregate(agg) => {
                let input = self.build(s_expr.child(0)?).await?;
                let input_schema = input.output_schema()?;
                let group_items = agg.group_items.iter().map(|v| v.index).collect::<Vec<_>>();

                let result = match &agg.mode {
                    AggregateMode::Partial => {
                        let agg_funcs: Vec<AggregateFunctionDesc> = agg.aggregate_functions.iter().map(|v| {
                            if let ScalarExpr::AggregateFunction(agg) = &v.scalar {
                                Ok(AggregateFunctionDesc {
                                    sig: AggregateFunctionSignature {
                                        name: agg.func_name.clone(),
                                        args: agg.args.iter().map(|s| {
                                            if let ScalarExpr::BoundColumnRef(col) = s {
                                                Ok(input_schema.field_with_name(&col.column.index.to_string())?.data_type().clone())
                                            } else {
                                                Err(ErrorCode::Internal(
                                                    "Aggregate function argument must be a BoundColumnRef".to_string()
                                                ))
                                            }
                                        }).collect::<Result<_>>()?,
                                        params: agg.params.clone(),
                                    },
                                    output_column: v.index,
                                    args: agg.args.iter().map(|arg| {
                                        if let ScalarExpr::BoundColumnRef(col) = arg {
                                            input_schema.index_of(&col.column.index.to_string())
                                        } else {
                                            Err(ErrorCode::Internal(
                                                "Aggregate function argument must be a BoundColumnRef".to_string()
                                            ))
                                        }
                                    }).collect::<Result<_>>()?,
                                    arg_indices: agg.args.iter().map(|arg| {
                                        if let ScalarExpr::BoundColumnRef(col) = arg {
                                            Ok(col.column.index)
                                        } else {
                                            Err(ErrorCode::Internal(
                                                "Aggregate function argument must be a BoundColumnRef".to_string()
                                            ))
                                        }
                                    }).collect::<Result<_>>()?,
                                })
                            } else {
                                Err(ErrorCode::Internal("Expected aggregate function".to_string()))
                            }
                        }).collect::<Result<_>>()?;

                        let settings = self.ctx.get_settings();
                        let group_by_shuffle_mode = settings.get_group_by_shuffle_mode()?;

                        match input {
                            PhysicalPlan::Exchange(PhysicalExchange { input, kind, .. })
                                if group_by_shuffle_mode == "before_merge" =>
                            {
                                let aggregate_partial = if !agg.grouping_sets.is_empty() {
                                    let expand = AggregateExpand {
                                        plan_id: self.next_plan_id(),
                                        input,
                                        group_bys: group_items.clone(),
                                        grouping_id_index: agg.grouping_id_index,
                                        grouping_sets: agg.grouping_sets.clone(),
                                        stat_info: Some(stat_info.clone()),
                                    };
                                    AggregatePartial {
                                        plan_id: self.next_plan_id(),
                                        input: Box::new(PhysicalPlan::AggregateExpand(expand)),
                                        agg_funcs,
                                        group_by: group_items,
                                        stat_info: Some(stat_info),
                                    }
                                } else {
                                    AggregatePartial {
                                        plan_id: self.next_plan_id(),
                                        input,
                                        agg_funcs,
                                        group_by: group_items,
                                        stat_info: Some(stat_info),
                                    }
                                };

                                let settings = self.ctx.get_settings();
                                let efficiently_memory =
                                    settings.get_efficiently_memory_group_by()?;

                                let group_by_key_index =
                                    aggregate_partial.output_schema()?.num_fields() - 1;
                                let group_by_key_data_type =
                                    DataBlock::choose_hash_method_with_types(
                                        &agg.group_items
                                            .iter()
                                            .map(|v| v.scalar.data_type())
                                            .collect::<Result<Vec<_>>>()?,
                                        efficiently_memory,
                                    )?
                                    .data_type();

                                PhysicalPlan::Exchange(PhysicalExchange {
                                    plan_id: self.next_plan_id(),
                                    kind,
                                    input: Box::new(PhysicalPlan::AggregatePartial(
                                        aggregate_partial,
                                    )),
                                    keys: vec![RemoteExpr::ColumnRef {
                                        span: None,
                                        id: group_by_key_index,
                                        data_type: group_by_key_data_type,
                                        display_name: "_group_by_key".to_string(),
                                    }],
                                })
                            }
                            _ => {
                                if !agg.grouping_sets.is_empty() {
                                    let expand = AggregateExpand {
                                        plan_id: self.next_plan_id(),
                                        input: Box::new(input),
                                        group_bys: group_items.clone(),
                                        grouping_id_index: agg.grouping_id_index,
                                        grouping_sets: agg.grouping_sets.clone(),
                                        stat_info: Some(stat_info.clone()),
                                    };
                                    PhysicalPlan::AggregatePartial(AggregatePartial {
                                        plan_id: self.next_plan_id(),
                                        agg_funcs,
                                        group_by: group_items,
                                        input: Box::new(PhysicalPlan::AggregateExpand(expand)),
                                        stat_info: Some(stat_info),
                                    })
                                } else {
                                    PhysicalPlan::AggregatePartial(AggregatePartial {
                                        plan_id: self.next_plan_id(),
                                        agg_funcs,
                                        group_by: group_items,
                                        input: Box::new(input),
                                        stat_info: Some(stat_info),
                                    })
                                }
                            }
                        }
                    }

                    // Hack to get before group by schema, we should refactor this
                    AggregateMode::Final => {
                        let input_schema = match input {
                            PhysicalPlan::AggregatePartial(ref agg) => agg.input.output_schema()?,

                            PhysicalPlan::Exchange(PhysicalExchange {
                                input: box PhysicalPlan::AggregatePartial(ref agg),
                                ..
                            }) => agg.input.output_schema()?,

                            _ => {
                                return Err(ErrorCode::Internal(format!(
                                    "invalid input physical plan: {}",
                                    input.name(),
                                )));
                            }
                        };

                        let agg_funcs: Vec<AggregateFunctionDesc> = agg.aggregate_functions.iter().map(|v| {
                            if let ScalarExpr::AggregateFunction(agg) = &v.scalar {
                                Ok(AggregateFunctionDesc {
                                    sig: AggregateFunctionSignature {
                                        name: agg.func_name.clone(),
                                        args: agg.args.iter().map(|s| {
                                            if let ScalarExpr::BoundColumnRef(col) = s {
                                                Ok(input_schema.field_with_name(&col.column.index.to_string())?.data_type().clone())
                                            } else {
                                                Err(ErrorCode::Internal(
                                                    "Aggregate function argument must be a BoundColumnRef".to_string()
                                                ))
                                            }
                                        }).collect::<Result<_>>()?,
                                        params: agg.params.clone(),
                                    },
                                    output_column: v.index,
                                    args: agg.args.iter().map(|arg| {
                                        if let ScalarExpr::BoundColumnRef(col) = arg {
                                            input_schema.index_of(&col.column.index.to_string())
                                        } else {
                                            Err(ErrorCode::Internal(
                                                "Aggregate function argument must be a BoundColumnRef".to_string()
                                            ))
                                        }
                                    }).collect::<Result<_>>()?,
                                    arg_indices: agg.args.iter().map(|arg| {
                                        if let ScalarExpr::BoundColumnRef(col) = arg {
                                            Ok(col.column.index)
                                        } else {
                                            Err(ErrorCode::Internal(
                                                "Aggregate function argument must be a BoundColumnRef".to_string()
                                            ))
                                        }
                                    }).collect::<Result<_>>()?,
                                })
                            } else {
                                Err(ErrorCode::Internal("Expected aggregate function".to_string()))
                            }
                        }).collect::<Result<_>>()?;

                        match input {
                            PhysicalPlan::AggregatePartial(ref partial) => {
                                let before_group_by_schema = partial.input.output_schema()?;
                                let limit = agg.limit;
                                PhysicalPlan::AggregateFinal(AggregateFinal {
                                    plan_id: self.next_plan_id(),
                                    input: Box::new(input),
                                    group_by: group_items,
                                    agg_funcs,
                                    before_group_by_schema,

                                    stat_info: Some(stat_info),
                                    limit,
                                })
                            }

                            PhysicalPlan::Exchange(PhysicalExchange {
                                input: box PhysicalPlan::AggregatePartial(ref partial),
                                ..
                            }) => {
                                let before_group_by_schema = partial.input.output_schema()?;
                                let limit = agg.limit;

                                PhysicalPlan::AggregateFinal(AggregateFinal {
                                    plan_id: self.next_plan_id(),
                                    input: Box::new(input),
                                    group_by: group_items,
                                    agg_funcs,
                                    before_group_by_schema,

                                    stat_info: Some(stat_info),
                                    limit,
                                })
                            }

                            _ => {
                                return Err(ErrorCode::Internal(format!(
                                    "invalid input physical plan: {}",
                                    input.name(),
                                )));
                            }
                        }
                    }
                    AggregateMode::Initial => {
                        return Err(ErrorCode::Internal("Invalid aggregate mode: Initial"));
                    }
                };

                Ok(result)
            }
            RelOperator::Window(w) => self.build_physical_window(s_expr, &stat_info, w).await,
            RelOperator::Sort(sort) => Ok(PhysicalPlan::Sort(Sort {
                plan_id: self.next_plan_id(),
                input: Box::new(self.build(s_expr.child(0)?).await?),
                order_by: sort
                    .items
                    .iter()
                    .map(|v| SortDesc {
                        asc: v.asc,
                        nulls_first: v.nulls_first,
                        order_by: v.index,
                    })
                    .collect(),
                limit: sort.limit,
                after_exchange: sort.after_exchange,
                stat_info: Some(stat_info),
            })),

            RelOperator::Limit(limit) => {
                let input_plan = self.build(s_expr.child(0)?).await?;
                self.build_limit(input_plan, limit, stat_info)
            }

            RelOperator::Exchange(exchange) => {
                let input = Box::new(self.build(s_expr.child(0)?).await?);
                let input_schema = input.output_schema()?;
                let mut keys = vec![];
                let kind = match exchange {
                    Exchange::Random => FragmentKind::Init,
                    Exchange::Hash(scalars) => {
                        for scalar in scalars {
                            let expr = scalar
                                .resolve_and_check(input_schema.as_ref())?
                                .project_column_ref(|index| {
                                    input_schema.index_of(&index.to_string()).unwrap()
                                });
                            let (expr, _) =
                                ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                            keys.push(expr.as_remote_expr());
                        }
                        FragmentKind::Normal
                    }
                    Exchange::Broadcast => FragmentKind::Expansive,
                    Exchange::Merge => FragmentKind::Merge,
                };
                Ok(PhysicalPlan::Exchange(PhysicalExchange {
                    plan_id: self.next_plan_id(),
                    input,
                    kind,
                    keys,
                }))
            }

            RelOperator::UnionAll(op) => {
                let left_plan = self.build(s_expr.child(0)?).await?;
                let right_plan = self.build(s_expr.child(1)?).await?;
                let left_schema = left_plan.output_schema()?;
                let right_schema = right_plan.output_schema()?;

                let common_types = op.pairs.iter().map(|(l, r)| {
                    let left_field = left_schema.field_with_name(&l.to_string()).unwrap();
                    let right_field = right_schema.field_with_name(&r.to_string()).unwrap();

                    let common_type = common_super_type(
                        left_field.data_type().clone(),
                        right_field.data_type().clone(),
                        &BUILTIN_FUNCTIONS.default_cast_rules,
                    );
                    common_type.ok_or_else(|| {
                        ErrorCode::SemanticError(format!(
                            "SetOperation's types cannot be matched, left column {:?}, type: {:?}, right column {:?}, type: {:?}",
                            left_field.name(),
                            left_field.data_type(),
                            right_field.name(),
                            right_field.data_type()
                        ))
                    })
                }).collect::<Result<Vec<_>>>()?;

                async fn cast_plan(
                    plan_builder: &mut PhysicalPlanBuilder,
                    plan: PhysicalPlan,
                    plan_schema: &DataSchema,
                    indexes: &[IndexType],
                    common_types: &[DataType],
                    stat_info: PlanStatsInfo,
                ) -> Result<PhysicalPlan> {
                    debug_assert!(indexes.len() == common_types.len());
                    let scalar_items = indexes
                        .iter()
                        .map(|index| plan_schema.field_with_name(&index.to_string()).unwrap())
                        .zip(common_types)
                        .filter(|(f, common_ty)| f.data_type() != *common_ty)
                        .map(|(f, common_ty)| {
                            let cast_expr = wrap_cast(
                                &ScalarExpr::BoundColumnRef(BoundColumnRef {
                                    span: None,
                                    column: ColumnBinding {
                                        database_name: None,
                                        table_name: None,
                                        column_position: None,
                                        table_index: None,
                                        column_name: f.name().clone(),
                                        index: f.name().parse().unwrap(),
                                        data_type: Box::new(f.data_type().clone()),
                                        visibility: Visibility::Visible,
                                        virtual_computed_expr: None,
                                    },
                                }),
                                common_ty,
                            );
                            ScalarItem {
                                scalar: cast_expr,
                                index: f.name().parse().unwrap(),
                            }
                        })
                        .collect::<Vec<_>>();

                    let new_plan = if scalar_items.is_empty() {
                        plan
                    } else {
                        plan_builder.build_eval_scalar(
                            plan,
                            &crate::plans::EvalScalar {
                                items: scalar_items,
                            },
                            stat_info,
                        )?
                    };

                    Ok(new_plan)
                }

                let left_indexes = op.pairs.iter().map(|(l, _)| *l).collect::<Vec<_>>();
                let right_indexes = op.pairs.iter().map(|(_, r)| *r).collect::<Vec<_>>();
                let left_plan = cast_plan(
                    self,
                    left_plan,
                    left_schema.as_ref(),
                    &left_indexes,
                    &common_types,
                    stat_info.clone(),
                )
                .await?;
                let right_plan = cast_plan(
                    self,
                    right_plan,
                    right_schema.as_ref(),
                    &right_indexes,
                    &common_types,
                    stat_info.clone(),
                )
                .await?;

                let pairs = op
                    .pairs
                    .iter()
                    .map(|(l, r)| (l.to_string(), r.to_string()))
                    .collect::<Vec<_>>();
                let fields = left_indexes
                    .iter()
                    .zip(&common_types)
                    .map(|(index, ty)| DataField::new(&index.to_string(), ty.clone()))
                    .collect::<Vec<_>>();

                Ok(PhysicalPlan::UnionAll(UnionAll {
                    plan_id: self.next_plan_id(),
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    pairs,
                    schema: DataSchemaRefExt::create(fields),

                    stat_info: Some(stat_info),
                }))
            }

            RelOperator::RuntimeFilterSource(op) => {
                let left_side = Box::new(self.build(s_expr.child(0)?).await?);
                let left_schema = left_side.output_schema()?;
                let right_side = Box::new(self.build(s_expr.child(1)?).await?);
                let right_schema = right_side.output_schema()?;
                let mut left_runtime_filters = BTreeMap::new();
                let mut right_runtime_filters = BTreeMap::new();
                for (left, right) in op
                    .left_runtime_filters
                    .iter()
                    .zip(op.right_runtime_filters.iter())
                {
                    let left_expr = left
                        .1
                        .resolve_and_check(left_schema.as_ref())?
                        .project_column_ref(|index| {
                            left_schema.index_of(&index.to_string()).unwrap()
                        });
                    let right_expr = right
                        .1
                        .resolve_and_check(right_schema.as_ref())?
                        .project_column_ref(|index| {
                            right_schema.index_of(&index.to_string()).unwrap()
                        });

                    let common_ty = common_super_type(left_expr.data_type().clone(), right_expr.data_type().clone(), &BUILTIN_FUNCTIONS.default_cast_rules)
                        .ok_or_else(|| ErrorCode::SemanticError(format!("RuntimeFilter's types cannot be matched, left column {:?}, type: {:?}, right column {:?}, type: {:?}", left.0, left_expr.data_type(), right.0, right_expr.data_type())))?;

                    let left_expr = type_check::check_cast(
                        left_expr.span(),
                        false,
                        left_expr,
                        &common_ty,
                        &BUILTIN_FUNCTIONS,
                    )?;
                    let right_expr = type_check::check_cast(
                        right_expr.span(),
                        false,
                        right_expr,
                        &common_ty,
                        &BUILTIN_FUNCTIONS,
                    )?;

                    let (left_expr, _) =
                        ConstantFolder::fold(&left_expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                    let (right_expr, _) =
                        ConstantFolder::fold(&right_expr, &self.func_ctx, &BUILTIN_FUNCTIONS);

                    left_runtime_filters.insert(left.0.clone(), left_expr.as_remote_expr());
                    right_runtime_filters.insert(right.0.clone(), right_expr.as_remote_expr());
                }
                Ok(PhysicalPlan::RuntimeFilterSource(RuntimeFilterSource {
                    plan_id: self.next_plan_id(),
                    left_side,
                    right_side,
                    left_runtime_filters,
                    right_runtime_filters,
                }))
            }

            RelOperator::ProjectSet(project_set) => {
                let input = self.build(s_expr.child(0)?).await?;
                let input_schema = input.output_schema()?;
                let srf_exprs = project_set
                    .srfs
                    .iter()
                    .map(|item| {
                        let expr = item
                            .scalar
                            .resolve_and_check(input_schema.as_ref())?
                            .project_column_ref(|index| {
                                input_schema.index_of(&index.to_string()).unwrap()
                            });
                        let (expr, _) =
                            ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                        Ok((expr.as_remote_expr(), item.index))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let mut unused_indices = HashSet::new();
                if let Some(ref unused_columns) = project_set.unused_columns {
                    for column in unused_columns {
                        if let Ok(index) = input_schema.index_of(&column.to_string()) {
                            unused_indices.insert(index);
                        }
                    }
                }

                Ok(PhysicalPlan::ProjectSet(ProjectSet {
                    plan_id: self.next_plan_id(),
                    input: Box::new(input),
                    srf_exprs,
                    unused_indices,
                    stat_info: Some(stat_info),
                }))
            }

            _ => Err(ErrorCode::Internal(format!(
                "Unsupported physical plan: {:?}",
                s_expr.plan()
            ))),
        }
    }

    #[async_recursion::async_recursion]
    #[async_backtrace::framed]
    async fn build_physical_window(
        &mut self,
        s_expr: &SExpr,
        stat_info: &PlanStatsInfo,
        w: &LogicalWindow,
    ) -> Result<PhysicalPlan> {
        let input = self.build(s_expr.child(0)?).await?;
        let input_schema = input.output_schema()?;

        let mut w = w.clone();

        // Unify the data type for range frame.
        if w.frame.units.is_range() && w.order_by.len() == 1 {
            let order_by = &mut w.order_by[0].order_by_item.scalar;

            let mut start = match &mut w.frame.start_bound {
                WindowFuncFrameBound::Preceding(scalar)
                | WindowFuncFrameBound::Following(scalar) => scalar.as_mut(),
                _ => None,
            };
            let mut end = match &mut w.frame.end_bound {
                WindowFuncFrameBound::Preceding(scalar)
                | WindowFuncFrameBound::Following(scalar) => scalar.as_mut(),
                _ => None,
            };

            let mut common_ty = order_by
                .resolve_and_check(&*input_schema)?
                .data_type()
                .clone();
            for scalar in start.iter_mut().chain(end.iter_mut()) {
                let ty = scalar.as_ref().infer_data_type();
                common_ty = common_super_type(
                    common_ty.clone(),
                    ty.clone(),
                    &BUILTIN_FUNCTIONS.default_cast_rules,
                )
                .ok_or_else(|| {
                    ErrorCode::IllegalDataType(format!(
                        "Cannot find common type for {:?} and {:?}",
                        &common_ty, &ty
                    ))
                })?;
            }

            *order_by = wrap_cast(order_by, &common_ty);
            for scalar in start.iter_mut().chain(end.iter_mut()) {
                let raw_expr = RawExpr::<usize>::Cast {
                    span: w.span,
                    is_try: false,
                    expr: Box::new(RawExpr::Constant {
                        span: w.span,
                        scalar: scalar.clone(),
                    }),
                    dest_type: common_ty.clone(),
                };
                let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)?;
                let (expr, _) =
                    ConstantFolder::fold(&expr, &FunctionContext::default(), &BUILTIN_FUNCTIONS);
                if let common_expression::Expr::Constant {
                    scalar: new_scalar, ..
                } = expr
                {
                    if new_scalar.is_positive() {
                        **scalar = new_scalar;
                        continue;
                    }
                }
                return Err(ErrorCode::SemanticError(
                    "Only positive numbers are allowed in RANGE offset".to_string(),
                )
                .set_span(w.span));
            }
        }

        // Generate a `EvalScalar` as the input of `Window`.
        let mut scalar_items: Vec<ScalarItem> = Vec::new();
        for arg in &w.arguments {
            scalar_items.push(arg.clone());
        }
        for part in &w.partition_by {
            scalar_items.push(part.clone());
        }
        for order in &w.order_by {
            scalar_items.push(order.order_by_item.clone())
        }
        let input = if !scalar_items.is_empty() {
            self.build_eval_scalar(
                input,
                &crate::planner::plans::EvalScalar {
                    items: scalar_items,
                },
                stat_info.clone(),
            )?
        } else {
            input
        };

        let order_by_items = w
            .order_by
            .iter()
            .map(|v| SortDesc {
                asc: v.asc.unwrap_or(true),
                nulls_first: v.nulls_first.unwrap_or(false),
                order_by: v.order_by_item.index,
            })
            .collect::<Vec<_>>();
        let partition_items = w.partition_by.iter().map(|v| v.index).collect::<Vec<_>>();

        let func = match &w.function {
            WindowFuncType::Aggregate(agg) => WindowFunction::Aggregate(AggregateFunctionDesc {
                sig: AggregateFunctionSignature {
                    name: agg.func_name.clone(),
                    args: agg
                        .args
                        .iter()
                        .map(|s| s.data_type())
                        .collect::<Result<_>>()?,
                    params: agg.params.clone(),
                },
                output_column: w.index,
                args: agg
                    .args
                    .iter()
                    .map(|arg| {
                        if let ScalarExpr::BoundColumnRef(col) = arg {
                            Ok(col.column.index)
                        } else {
                            Err(ErrorCode::Internal(
                                "Window's aggregate function argument must be a BoundColumnRef"
                                    .to_string(),
                            ))
                        }
                    })
                    .collect::<Result<_>>()?,
                arg_indices: agg
                    .args
                    .iter()
                    .map(|arg| {
                        if let ScalarExpr::BoundColumnRef(col) = arg {
                            Ok(col.column.index)
                        } else {
                            Err(ErrorCode::Internal(
                                "Aggregate function argument must be a BoundColumnRef".to_string(),
                            ))
                        }
                    })
                    .collect::<Result<_>>()?,
            }),
            WindowFuncType::LagLead(lag_lead) => {
                let new_default = match &lag_lead.default {
                    None => LagLeadDefault::Null,
                    Some(d) => match d {
                        box ScalarExpr::BoundColumnRef(col) => {
                            LagLeadDefault::Index(col.column.index)
                        }
                        _ => unreachable!(),
                    },
                };
                WindowFunction::LagLead(LagLeadFunctionDesc {
                    is_lag: lag_lead.is_lag,
                    offset: lag_lead.offset,
                    return_type: *lag_lead.return_type.clone(),
                    arg: if let ScalarExpr::BoundColumnRef(col) = *lag_lead.arg.clone() {
                        Ok(col.column.index)
                    } else {
                        Err(ErrorCode::Internal(
                            "Window's lag function argument must be a BoundColumnRef".to_string(),
                        ))
                    }?,
                    default: new_default,
                })
            }

            WindowFuncType::NthValue(func) => WindowFunction::NthValue(NthValueFunctionDesc {
                n: func.n,
                return_type: *func.return_type.clone(),
                arg: if let ScalarExpr::BoundColumnRef(col) = &*func.arg {
                    Ok(col.column.index)
                } else {
                    Err(ErrorCode::Internal(
                        "Window's nth_value function argument must be a BoundColumnRef".to_string(),
                    ))
                }?,
            }),
            WindowFuncType::Ntile(func) => WindowFunction::Ntile(NtileFunctionDesc {
                n: func.n,
                return_type: *func.return_type.clone(),
            }),
            WindowFuncType::RowNumber => WindowFunction::RowNumber,
            WindowFuncType::Rank => WindowFunction::Rank,
            WindowFuncType::DenseRank => WindowFunction::DenseRank,
            WindowFuncType::PercentRank => WindowFunction::PercentRank,
        };

        Ok(PhysicalPlan::Window(Window {
            plan_id: self.next_plan_id(),
            index: w.index,
            input: Box::new(input),
            func,
            partition_by: partition_items,
            order_by: order_by_items,
            window_frame: w.frame.clone(),
        }))
    }

    fn build_eval_scalar(
        &mut self,
        input: PhysicalPlan,
        eval_scalar: &planner::plans::EvalScalar,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let input_schema = input.output_schema()?;
        let exprs = eval_scalar
            .items
            .iter()
            .map(|item| {
                let expr = item
                    .scalar
                    .resolve_and_check(input_schema.as_ref())?
                    .project_column_ref(|index| input_schema.index_of(&index.to_string()).unwrap());
                let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                Ok((expr.as_remote_expr(), item.index))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(PhysicalPlan::EvalScalar(EvalScalar {
            plan_id: self.next_plan_id(),
            input: Box::new(input),
            exprs,
            stat_info: Some(stat_info),
        }))
    }

    fn build_virtual_columns(&self, columns: &ColumnSet) -> Option<Vec<VirtualColumnInfo>> {
        let mut virtual_column_infos = Vec::new();
        for index in columns.iter() {
            if let ColumnEntry::VirtualColumn(virtual_column) = self.metadata.read().column(*index)
            {
                let virtual_column_info = VirtualColumnInfo {
                    source_name: virtual_column.source_column_name.clone(),
                    name: virtual_column.column_name.clone(),
                    paths: virtual_column.paths.clone(),
                    data_type: Box::new(virtual_column.data_type.clone()),
                };
                virtual_column_infos.push(virtual_column_info);
            }
        }
        if virtual_column_infos.is_empty() {
            None
        } else {
            Some(virtual_column_infos)
        }
    }

    fn push_downs(
        &self,
        scan: &Scan,
        table_schema: &TableSchema,
        has_inner_column: bool,
        has_virtual_column: bool,
    ) -> Result<PushDownInfo> {
        let metadata = self.metadata.read().clone();
        let projection = Self::build_projection(
            &metadata,
            table_schema,
            scan.columns.iter(),
            has_inner_column,
            // for projection, we need to ignore read data from internal column,
            // or else in read_partition when search internal column from table schema will core.
            true,
            true,
            true,
        );

        let output_columns = if has_virtual_column {
            Some(Self::build_projection(
                &metadata,
                table_schema,
                scan.columns.iter(),
                has_inner_column,
                true,
                false,
                true,
            ))
        } else {
            None
        };

        let mut is_deterministic = true;
        let push_down_filter = scan
            .push_down_predicates
            .as_ref()
            .filter(|p| !p.is_empty())
            .map(
                |predicates: &Vec<ScalarExpr>| -> Result<RemoteExpr<String>> {
                    let predicates = predicates
                        .iter()
                        .map(|p| {
                            Ok(p.as_expr()?
                                .project_column_ref(|col| col.column_name.clone()))
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let expr = predicates
                        .into_iter()
                        .reduce(|lhs, rhs| {
                            check_function(
                                None,
                                "and_filters",
                                &[],
                                &[lhs, rhs],
                                &BUILTIN_FUNCTIONS,
                            )
                            .unwrap()
                        })
                        .unwrap();

                    let expr = cast_expr_to_non_null_boolean(expr)?;
                    let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);

                    is_deterministic = expr.is_deterministic(&BUILTIN_FUNCTIONS);

                    Ok(expr.as_remote_expr())
                },
            )
            .transpose()?;

        let prewhere_info = scan
            .prewhere
            .as_ref()
            .map(|prewhere| -> Result<PrewhereInfo> {
                let remain_columns = scan
                    .columns
                    .difference(&prewhere.prewhere_columns)
                    .copied()
                    .collect::<HashSet<usize>>();

                let output_columns = Self::build_projection(
                    &metadata,
                    table_schema,
                    prewhere.output_columns.iter(),
                    has_inner_column,
                    true,
                    false,
                    true,
                );
                let prewhere_columns = Self::build_projection(
                    &metadata,
                    table_schema,
                    prewhere.prewhere_columns.iter(),
                    has_inner_column,
                    true,
                    true,
                    true,
                );
                let remain_columns = Self::build_projection(
                    &metadata,
                    table_schema,
                    remain_columns.iter(),
                    has_inner_column,
                    true,
                    true,
                    true,
                );

                let predicate = prewhere
                    .predicates
                    .iter()
                    .cloned()
                    .reduce(|lhs, rhs| {
                        ScalarExpr::FunctionCall(FunctionCall {
                            span: None,
                            func_name: "and".to_string(),
                            params: vec![],
                            arguments: vec![lhs, rhs],
                        })
                    })
                    .expect("there should be at least one predicate in prewhere");
                let filter = cast_expr_to_non_null_boolean(
                    predicate
                        .as_expr()?
                        .project_column_ref(|col| col.column_name.clone()),
                )?
                .as_remote_expr();
                let virtual_columns = self.build_virtual_columns(&prewhere.prewhere_columns);

                Ok::<PrewhereInfo, ErrorCode>(PrewhereInfo {
                    output_columns,
                    prewhere_columns,
                    remain_columns,
                    filter,
                    virtual_columns,
                })
            })
            .transpose()?;

        let order_by = scan
            .order_by
            .clone()
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| {
                        let metadata = self.metadata.read();
                        let column = metadata.column(item.index);
                        let (name, data_type) = match column {
                            ColumnEntry::BaseTableColumn(BaseTableColumn {
                                column_name,
                                data_type,
                                ..
                            }) => (column_name.clone(), DataType::from(data_type)),
                            ColumnEntry::DerivedColumn(DerivedColumn {
                                alias, data_type, ..
                            }) => (alias.clone(), data_type.clone()),
                            ColumnEntry::InternalColumn(TableInternalColumn {
                                internal_column,
                                ..
                            }) => (
                                internal_column.column_name().to_owned(),
                                internal_column.data_type(),
                            ),
                            ColumnEntry::VirtualColumn(VirtualColumn {
                                column_name,
                                data_type,
                                ..
                            }) => (column_name.clone(), DataType::from(data_type)),
                        };

                        // sort item is already a column
                        let scalar = RemoteExpr::ColumnRef {
                            span: None,
                            id: name.clone(),
                            data_type,
                            display_name: name,
                        };

                        Ok((scalar, item.asc, item.nulls_first))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;

        let virtual_columns = self.build_virtual_columns(&scan.columns);

        Ok(PushDownInfo {
            projection: Some(projection),
            output_columns,
            filter: push_down_filter,
            is_deterministic,
            prewhere: prewhere_info,
            limit: scan.limit,
            order_by: order_by.unwrap_or_default(),
            virtual_columns,
            lazy_materialization: !metadata.lazy_columns().is_empty(),
            agg_index: None,
        })
    }

    pub(crate) fn build_plan_stat_info(&self, s_expr: &SExpr) -> Result<PlanStatsInfo> {
        let rel_expr = RelExpr::with_s_expr(s_expr);
        let stat_info = rel_expr.derive_cardinality()?;

        Ok(PlanStatsInfo {
            estimated_rows: stat_info.cardinality,
        })
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RangeJoinCondition {
    pub left_expr: RemoteExpr,
    pub right_expr: RemoteExpr,
    // "gt" | "lt" | "gte" | "lte"
    pub operator: String,
}
