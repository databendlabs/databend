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

use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::PrewhereInfo;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::VirtualColumnInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FieldIndex;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::ROW_ID_COL_NAME;
use databend_common_functions::BUILTIN_FUNCTIONS;
use itertools::Itertools;

use crate::binder::INTERNAL_COLUMN_FACTORY;
use crate::executor::cast_expr_to_non_null_boolean;
use crate::executor::explain::PlanStatsInfo;
use crate::executor::table_read_plan::ToReadDataSourcePlan;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::plans::FunctionCall;
use crate::BaseTableColumn;
use crate::ColumnEntry;
use crate::ColumnSet;
use crate::DerivedColumn;
use crate::IndexType;
use crate::Metadata;
use crate::ScalarExpr;
use crate::TableInternalColumn;
use crate::TypeCheck;
use crate::VirtualColumn;
use crate::DUMMY_COLUMN_INDEX;
use crate::DUMMY_TABLE_INDEX;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TableScan {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub name_mapping: BTreeMap<String, IndexType>,
    pub source: Box<DataSourcePlan>,
    pub internal_column: Option<BTreeMap<FieldIndex, InternalColumn>>,

    pub table_index: Option<IndexType>,
    pub stat_info: Option<PlanStatsInfo>,
}

impl TableScan {
    pub fn output_fields(
        schema: TableSchemaRef,
        name_mapping: &BTreeMap<String, IndexType>,
    ) -> Result<Vec<DataField>> {
        let mut fields = Vec::with_capacity(name_mapping.len());
        let mut name_and_ids = name_mapping
            .iter()
            .map(|(name, id)| {
                let index = schema.index_of(name)?;
                Ok((name, id, index))
            })
            .collect::<Result<Vec<_>>>()?;
        // Make the order of output fields the same as their indexes in te table schema.
        name_and_ids.sort_by_key(|(_, _, index)| *index);

        for (name, id, _) in name_and_ids {
            let orig_field = schema.field_with_name(name)?;
            let data_type = DataType::from(orig_field.data_type());
            fields.push(DataField::new(&id.to_string(), data_type));
        }
        Ok(fields)
    }

    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let fields = TableScan::output_fields(self.source.schema(), &self.name_mapping)?;
        Ok(DataSchemaRefExt::create(fields))
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_table_scan(
        &mut self,
        scan: &crate::plans::Scan,
        required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        // Some table may not have any column,
        // e.g. `system.sync_crash_me`
        let scan = if scan.columns.is_empty() {
            scan.clone()
        } else {
            let columns = scan.columns.clone();
            let mut prewhere = scan.prewhere.clone();
            let mut used: ColumnSet = required.intersection(&columns).cloned().collect();
            if let Some(ref mut pw) = prewhere {
                debug_assert!(
                    pw.prewhere_columns.is_subset(&columns),
                    "prewhere columns should be a subset of scan columns"
                );
                pw.output_columns = used.clone();
                // `prune_columns` is after `prewhere_optimize`,
                // so we need to add prewhere columns to scan columns.
                used = used.union(&pw.prewhere_columns).cloned().collect();
            }
            scan.prune_columns(used, prewhere)
        };

        // 2. Build physical plan.
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
            match column {
                ColumnEntry::BaseTableColumn(BaseTableColumn { path_indices, .. }) => {
                    if path_indices.is_some() {
                        has_inner_column = true;
                    }
                }
                ColumnEntry::InternalColumn(TableInternalColumn {
                    internal_column, ..
                }) => {
                    project_internal_columns.insert(*index, internal_column.to_owned());
                }
                ColumnEntry::VirtualColumn(_) => {
                    has_virtual_column = true;
                }
                _ => {}
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
                    .read()
                    .row_id_index_by_table_index(scan.table_index);
                debug_assert!(index.is_some());
                // Safe to unwrap: if lazy_columns is not empty, the `analyze_lazy_materialization` have been called
                // and the row_id index of the table_index has been generated.
                let index = index.unwrap();
                entry.insert(index);
                project_internal_columns.insert(index, internal_column);
            }
        }

        let table_entry = metadata.table(scan.table_index);
        let table = table_entry.table();

        if !table.result_can_be_cached() {
            self.ctx.set_cacheable(false);
        }

        let mut table_schema = table.schema_with_stream();
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
            self.push_downs(&scan, &table_schema, has_inner_column, has_virtual_column)?;

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
                scan.update_stream_columns,
                self.dry_run,
            )
            .await?;
        source.table_index = scan.table_index;
        if let Some(agg_index) = &scan.agg_index {
            let source_schema = source.schema();
            let push_down = source.push_downs.as_mut().unwrap();
            let output_fields = TableScan::output_fields(source_schema, &name_mapping)?;
            let agg_index = Self::build_agg_index(agg_index, &output_fields)?;
            push_down.agg_index = Some(agg_index);
        }
        let internal_column = if project_internal_columns.is_empty() {
            None
        } else {
            Some(project_internal_columns)
        };
        Ok(PhysicalPlan::TableScan(TableScan {
            plan_id: 0,
            name_mapping,
            source: Box::new(source),
            table_index: Some(scan.table_index),
            stat_info: Some(stat_info),
            internal_column,
        }))
    }

    pub(crate) async fn build_dummy_table_scan(&mut self) -> Result<PhysicalPlan> {
        let catalogs = CatalogManager::instance();
        let table = catalogs
            .get_default_catalog(self.ctx.txn_mgr())?
            .get_table(&self.ctx.get_tenant(), "system", "one")
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
                false,
                self.dry_run,
            )
            .await?;
        Ok(PhysicalPlan::TableScan(TableScan {
            plan_id: 0,
            name_mapping: BTreeMap::from([("dummy".to_string(), DUMMY_COLUMN_INDEX)]),
            source: Box::new(source),
            table_index: Some(DUMMY_TABLE_INDEX),
            stat_info: Some(PlanStatsInfo {
                estimated_rows: 1.0,
            }),
            internal_column: None,
        }))
    }

    fn push_downs(
        &self,
        scan: &crate::plans::Scan,
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
            .map(|predicates: &Vec<ScalarExpr>| -> Result<Filters> {
                let predicates = predicates
                    .iter()
                    .map(|p| {
                        Ok(p.as_raw_expr()
                            .type_check(&metadata)?
                            .project_column_ref(|col| col.column_name.clone()))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let expr = predicates
                    .into_iter()
                    .try_reduce(|lhs, rhs| {
                        check_function(None, "and_filters", &[], &[lhs, rhs], &BUILTIN_FUNCTIONS)
                    })?
                    .unwrap();

                let expr = cast_expr_to_non_null_boolean(expr)?;
                let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);

                is_deterministic = expr.is_deterministic(&BUILTIN_FUNCTIONS);

                let inverted_filter =
                    check_function(None, "not", &[], &[expr.clone()], &BUILTIN_FUNCTIONS)?;

                Ok(Filters {
                    filter: expr.as_remote_expr(),
                    inverted_filter: inverted_filter.as_remote_expr(),
                })
            })
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
                            func_name: "and_filters".to_string(),
                            params: vec![],
                            arguments: vec![lhs, rhs],
                        })
                    })
                    .expect("there should be at least one predicate in prewhere");

                let filter = cast_expr_to_non_null_boolean(
                    predicate
                        .as_raw_expr()
                        .type_check(&metadata)?
                        .project_column_ref(|col| col.column_name.clone()),
                )?;
                let filter = filter.as_remote_expr();
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
            filters: push_down_filter,
            is_deterministic,
            prewhere: prewhere_info,
            limit: scan.limit,
            order_by: order_by.unwrap_or_default(),
            virtual_columns,
            lazy_materialization: !metadata.lazy_columns().is_empty(),
            agg_index: None,
            change_type: scan.change_type.clone(),
            inverted_index: scan.inverted_index.clone(),
        })
    }

    fn build_virtual_columns(&self, indices: &ColumnSet) -> Option<Vec<VirtualColumnInfo>> {
        let mut column_and_indices = Vec::new();
        for index in indices.iter() {
            if let ColumnEntry::VirtualColumn(virtual_column) = self.metadata.read().column(*index)
            {
                let virtual_column_info = VirtualColumnInfo {
                    source_name: virtual_column.source_column_name.clone(),
                    name: virtual_column.column_name.clone(),
                    key_paths: virtual_column.key_paths.clone(),
                    data_type: Box::new(virtual_column.data_type.clone()),
                };
                column_and_indices.push((virtual_column_info, *index));
            }
        }
        if column_and_indices.is_empty() {
            return None;
        }
        // Make the order of virtual columns the same as their indexes.
        column_and_indices.sort_by_key(|(_, index)| *index);

        let virtual_column_infos = column_and_indices
            .into_iter()
            .map(|(column, _)| column)
            .collect::<Vec<_>>();
        Some(virtual_column_infos)
    }

    pub(crate) fn build_agg_index(
        agg: &crate::plans::AggIndexInfo,
        source_fields: &[DataField],
    ) -> Result<databend_common_catalog::plan::AggIndexInfo> {
        // Build projection
        let used_columns = agg.used_columns();
        let mut col_indices = Vec::with_capacity(used_columns.len());
        for index in used_columns.iter().sorted() {
            col_indices.push(agg.schema.index_of(&index.to_string())?);
        }
        let projection = Projection::Columns(col_indices);
        let output_schema = projection.project_schema(&agg.schema);

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
                Ok(
                    cast_expr_to_non_null_boolean(pred.as_expr()?.project_column_ref(|col| {
                        output_schema.index_of(&col.index.to_string()).unwrap()
                    }))?
                    .as_remote_expr(),
                )
            })
            .transpose()?;
        let selection = agg
            .selection
            .iter()
            .map(|sel| {
                let offset = source_fields
                    .iter()
                    .position(|f| sel.index.to_string() == f.name().as_str());
                Ok((
                    sel.scalar
                        .as_expr()?
                        .project_column_ref(|col| {
                            output_schema.index_of(&col.index.to_string()).unwrap()
                        })
                        .as_remote_expr(),
                    offset,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(databend_common_catalog::plan::AggIndexInfo {
            index_id: agg.index_id,
            filter,
            selection,
            schema: agg.schema.clone(),
            actual_table_field_len: source_fields.len(),
            is_agg: agg.is_agg,
            projection,
            num_agg_funcs: agg.num_agg_funcs,
        })
    }

    pub(crate) fn build_projection<'a>(
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
}
