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

use std::any::Any;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PrewhereInfo;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::VirtualColumnField;
use databend_common_catalog::plan::VirtualColumnInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FieldIndex;
use databend_common_expression::ROW_ID_COL_NAME;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_pipeline_transforms::columns::TransformAddInternalColumns;
use databend_common_sql::BaseTableColumn;
use databend_common_sql::ColumnEntry;
use databend_common_sql::ColumnSet;
use databend_common_sql::DUMMY_COLUMN_INDEX;
use databend_common_sql::DUMMY_TABLE_INDEX;
use databend_common_sql::DerivedColumn;
use databend_common_sql::IndexType;
use databend_common_sql::Metadata;
use databend_common_sql::ScalarExpr;
use databend_common_sql::TableInternalColumn;
use databend_common_sql::TypeCheck;
use databend_common_sql::VirtualColumn;
use databend_common_sql::binder::INTERNAL_COLUMN_FACTORY;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::executor::cast_expr_to_non_null_boolean;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_common_sql::plans::FunctionCall;
use databend_common_storages_fuse::FuseTable;
use rand::distributions::Bernoulli;
use rand::distributions::Distribution;
use rand::thread_rng;

use crate::physical_plans::AddStreamColumn;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::TableScanFormatter;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TableScan {
    pub meta: PhysicalPlanMeta,
    pub scan_id: usize,
    pub name_mapping: BTreeMap<String, IndexType>,
    pub source: Box<DataSourcePlan>,
    pub internal_column: Option<BTreeMap<FieldIndex, InternalColumn>>,

    pub table_index: Option<IndexType>,
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for TableScan {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    #[recursive::recursive]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        let schema = self.source.schema();
        let mut fields = Vec::with_capacity(self.name_mapping.len());
        let mut name_and_ids = self
            .name_mapping
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

        Ok(DataSchemaRefExt::create(fields))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(TableScanFormatter::create(self))
    }

    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        Some(&self.source)
    }

    fn get_all_data_source(&self, sources: &mut Vec<(u32, Box<DataSourcePlan>)>) {
        sources.push((self.get_id(), self.source.clone()));
    }

    fn set_pruning_stats(&mut self, stats: &mut HashMap<u32, PartStatistics>) {
        if let Some(stat) = stats.remove(&self.get_id()) {
            self.source.statistics = stat;
        }
    }

    fn is_warehouse_distributed_plan(&self) -> bool {
        self.source.parts.kind == PartitionsShuffleKind::BroadcastWarehouse
    }

    fn get_desc(&self) -> Result<String> {
        Ok(format!(
            "{}.{}",
            self.source.source_info.catalog_name(),
            self.source.source_info.desc()
        ))
    }

    fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        Ok(HashMap::from([
            (String::from("Full table name"), vec![format!(
                "{}.{}",
                self.source.source_info.catalog_name(),
                self.source.source_info.desc()
            )]),
            (
                format!(
                    "Columns ({} / {})",
                    self.output_schema()?.num_fields(),
                    std::cmp::max(
                        self.output_schema()?.num_fields(),
                        self.source.source_info.schema().num_fields(),
                    )
                ),
                self.name_mapping.keys().cloned().collect(),
            ),
            (String::from("Total partitions"), vec![
                self.source.statistics.partitions_total.to_string(),
            ]),
        ]))
    }

    fn derive(&self, children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert!(children.is_empty());
        PhysicalPlan::new(TableScan {
            meta: self.meta.clone(),
            scan_id: self.scan_id,
            name_mapping: self.name_mapping.clone(),
            source: self.source.clone(),
            internal_column: self.internal_column.clone(),
            table_index: self.table_index,
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let table = builder.ctx.build_table_from_source_plan(&self.source)?;
        builder.ctx.set_partitions(self.source.parts.clone())?;

        if builder.ctx.get_settings().get_enable_prune_pipeline()? {
            if let Some(prune_pipeline) = table.build_prune_pipeline(
                builder.ctx.clone(),
                &self.source,
                &mut builder.main_pipeline,
                self.get_id(),
            )? {
                builder.pipelines.push(prune_pipeline);
            }
        }

        table.read_data(
            builder.ctx.clone(),
            &self.source,
            &mut builder.main_pipeline,
            true,
        )?;

        // Fill internal columns if needed.
        if let Some(internal_columns) = &self.internal_column {
            builder
                .main_pipeline
                .add_transformer(|| TransformAddInternalColumns::new(internal_columns.clone()));
        }

        let schema = self.source.schema();
        let mut projection = self
            .name_mapping
            .keys()
            .map(|name| schema.index_of(name.as_str()))
            .collect::<Result<Vec<usize>>>()?;
        projection.sort();

        // if projection is sequential, no need to add projection
        if projection != (0..schema.fields().len()).collect::<Vec<usize>>() {
            let ops = vec![BlockOperator::Project { projection }];
            let num_input_columns = schema.num_fields();
            builder.main_pipeline.add_transformer(|| {
                CompoundBlockOperator::new(ops.clone(), builder.func_ctx.clone(), num_input_columns)
            });
        }

        Ok(())
    }
}

impl TableScan {
    pub fn create(
        scan_id: usize,
        name_mapping: BTreeMap<String, IndexType>,
        source: Box<DataSourcePlan>,
        table_index: Option<IndexType>,
        stat_info: Option<PlanStatsInfo>,
        internal_column: Option<BTreeMap<FieldIndex, InternalColumn>>,
    ) -> PhysicalPlan {
        let name = match &source.source_info {
            DataSourceInfo::TableSource(_) => "TableScan".to_string(),
            DataSourceInfo::StageSource(_) => "StageScan".to_string(),
            DataSourceInfo::ParquetSource(_) => "ParquetScan".to_string(),
            DataSourceInfo::ResultScanSource(_) => "ResultScan".to_string(),
            DataSourceInfo::ORCSource(_) => "OrcScan".to_string(),
        };

        PhysicalPlan::new(TableScan {
            meta: PhysicalPlanMeta::new(name),
            source,
            scan_id,
            name_mapping,
            table_index,
            stat_info,
            internal_column,
        })
    }

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
}

impl PhysicalPlanBuilder {
    pub async fn build_table_scan(
        &mut self,
        scan: &databend_common_sql::plans::Scan,
        required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        // Some table may not have any column,
        // e.g. `system.sync_crash_me`
        let scan = if scan.columns.is_empty() {
            scan.clone()
        } else {
            let mut columns = scan.columns.clone();

            let required_column_ids: Vec<_> = required.difference(&columns).cloned().collect();
            if !required_column_ids.is_empty() {
                // add virtual columns to table scan columns.
                let read_guard = self.metadata.read();
                let virtual_column_id_set = read_guard
                    .virtual_columns_by_table_index(scan.table_index)
                    .iter()
                    .map(|column| column.index())
                    .collect::<HashSet<_>>();
                for required_column_id in required_column_ids {
                    if virtual_column_id_set.contains(&required_column_id) {
                        columns.insert(required_column_id);
                    }
                }
            }

            let mut prewhere = scan.prewhere.clone();
            let mut used: ColumnSet = required.intersection(&columns).cloned().collect();

            let supported_lazy_materialize = {
                self.metadata
                    .read()
                    .table(scan.table_index)
                    .table()
                    .supported_lazy_materialize()
            };

            if scan.is_lazy_table && supported_lazy_materialize {
                let lazy_columns = columns.difference(&used).cloned().collect();
                let mut metadata = self.metadata.write();
                metadata.set_table_lazy_columns(scan.table_index, lazy_columns);
                for column_index in used.iter() {
                    metadata.add_retained_column(*column_index);
                }
            }
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
        let mut name_mapping = BTreeMap::new();
        let mut project_internal_columns = BTreeMap::new();
        let mut project_virtual_columns = BTreeMap::new();
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
                ColumnEntry::VirtualColumn(virtual_column) => {
                    project_virtual_columns.insert(*index, virtual_column.clone());
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

        if !name_mapping.contains_key(ROW_ID_COL_NAME) {
            let metadata = self.metadata.read();
            if let Some(index) = metadata.row_id_index_by_table_index(scan.table_index) {
                let internal_column = INTERNAL_COLUMN_FACTORY
                    .get_internal_column(ROW_ID_COL_NAME)
                    .unwrap();
                name_mapping.insert(ROW_ID_COL_NAME.to_string(), index);
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

        let push_downs = self.push_downs(
            &scan,
            &table_schema,
            project_virtual_columns,
            has_inner_column,
        )?;

        let mut source = table
            .read_plan(
                self.ctx.clone(),
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
        if let Some(sample) = scan.sample
            && !table.use_own_sample_block()
        {
            if let Some(block_sample_value) = sample.block_level {
                if block_sample_value > 100.0 {
                    return Err(ErrorCode::SyntaxException(format!(
                        "Sample value should be less than or equal to 100, but got {}",
                        block_sample_value
                    )));
                }
                let probability = block_sample_value / 100.0;
                let original_parts = source.parts.partitions.len();
                let mut sample_parts = Vec::with_capacity(original_parts);
                let mut rng = thread_rng();
                let bernoulli = Bernoulli::new(probability).unwrap();
                for part in source.parts.partitions.iter() {
                    if bernoulli.sample(&mut rng) {
                        sample_parts.push(part.clone());
                    }
                }
                source.parts.partitions = sample_parts;
            }
        }
        source.table_index = scan.table_index;
        source.scan_id = scan.scan_id;
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

        if scan.is_lazy_table {
            let mut metadata = self.metadata.write();
            metadata.set_table_source(scan.table_index, source.clone());
        }

        let mut plan = TableScan::create(
            scan.scan_id,
            name_mapping,
            Box::new(source),
            Some(scan.table_index),
            Some(stat_info),
            internal_column,
        );

        // Update stream columns if needed.
        if scan.update_stream_columns {
            plan = AddStreamColumn::create(
                &self.metadata,
                plan,
                scan.table_index,
                table.get_table_info().ident.seq,
            )?;
        }

        Ok(plan)
    }

    pub async fn build_dummy_table_scan(
        &mut self,
        dummy_scan: &databend_common_sql::plans::DummyTableScan,
    ) -> Result<PhysicalPlan> {
        let catalogs = CatalogManager::instance();
        let table = catalogs
            .get_default_catalog(self.ctx.session_state()?)?
            .get_table(&self.ctx.get_tenant(), "system", "one")
            .await?;

        // Add cache invalidation keys for DummyTableScan's source tables.
        //
        // When DummyTableScan is created by optimizations like count(*) folding, we need to
        // track which tables the result depends on for proper cache invalidation.
        //
        // Example problem without this fix:
        //   SELECT * FROM t1 WHERE a > (SELECT COUNT(*) FROM t2)
        //
        //   1. t1 has data, t2 is empty:
        //      - t1 (TableScan) adds t1's partition SHA
        //      - t2 (DummyTableScan, empty table) adds nothing if we skip empty tables
        //      - Cache writes: partitions_shas = [t1_sha]
        //   2. After inserting data into t2:
        //      - Cache check: [t1_sha] == [t1_sha] → cache hit → WRONG result!
        //
        // Solution: For FuseTables, use snapshot_location as the cache invalidation key.
        //
        // Why snapshot_location instead of partition SHA256?
        // - Simpler: No need to call read_partitions() (async I/O) and compute SHA
        // - Equivalent semantics: Any mutation (INSERT, UPDATE, DELETE, COMPACT, RECLUSTER)
        //   creates a new snapshot, so snapshot_location uniquely identifies table state
        // - snapshot_loc() is synchronous (reads from table metadata, no storage I/O)
        //   vs read_table_snapshot() which would need to fetch and deserialize the snapshot file
        let settings = self.ctx.get_settings();
        if settings.get_enable_query_result_cache()? && !dummy_scan.source_table_indexes.is_empty()
        {
            let metadata = self.metadata.read();
            for &idx in &dummy_scan.source_table_indexes {
                let source_table = metadata.table(idx).table();

                // Check if the source table supports result caching at all.
                if !source_table.result_can_be_cached() {
                    self.ctx.set_cacheable(false);
                    break;
                }

                // Record a cache invalidation ID for DummyTableScan's source tables.
                // Only FuseTable provides query_result_cache_id; for other table engines
                // we conservatively disable caching to avoid returning stale results.
                if let Ok(fuse_table) = FuseTable::try_from_table(source_table.as_ref()) {
                    self.ctx
                        .add_partitions_sha(fuse_table.query_result_cache_id());
                } else {
                    // Non-FuseTable (system table, memory table, etc.), disable caching.
                    self.ctx.set_cacheable(false);
                    break;
                }
            }
        }

        let source = table
            .read_plan(self.ctx.clone(), None, None, false, self.dry_run)
            .await?;

        Ok(TableScan::create(
            DUMMY_TABLE_INDEX,
            BTreeMap::from([("dummy".to_string(), DUMMY_COLUMN_INDEX)]),
            Box::new(source),
            Some(DUMMY_TABLE_INDEX),
            Some(PlanStatsInfo {
                estimated_rows: 1.0,
            }),
            None,
        ))
    }

    fn push_downs(
        &self,
        scan: &databend_common_sql::plans::Scan,
        table_schema: &TableSchema,
        virtual_columns: BTreeMap<IndexType, VirtualColumn>,
        has_inner_column: bool,
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
        );
        let has_virtual_column = !virtual_columns.is_empty();

        let output_columns = if has_virtual_column {
            Some(Self::build_projection(
                &metadata,
                table_schema,
                scan.columns.iter(),
                has_inner_column,
                true,
                false,
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
                        p.as_raw_expr()
                            .type_check(&metadata)?
                            .project_column_ref(|col| Ok(col.column_name.clone()))
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
                );
                let prewhere_columns = Self::build_projection(
                    &metadata,
                    table_schema,
                    prewhere.prewhere_columns.iter(),
                    has_inner_column,
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
                        .project_column_ref(|col| Ok(col.column_name.clone()))?,
                )?;
                let filter = filter.as_remote_expr();
                let virtual_column_ids =
                    self.build_prewhere_virtual_column_ids(&prewhere.prewhere_columns);

                Ok::<PrewhereInfo, ErrorCode>(PrewhereInfo {
                    output_columns,
                    prewhere_columns,
                    remain_columns,
                    filter,
                    virtual_column_ids,
                })
            })
            .transpose()?;

        let order_by = scan.order_by.clone().map(|items| {
            items
                .into_iter()
                .filter_map(|item| {
                    let metadata = self.metadata.read();
                    let column = metadata.column(item.index);
                    let (name, data_type) = match column {
                        ColumnEntry::BaseTableColumn(BaseTableColumn {
                            column_name,
                            data_type,
                            ..
                        }) => (column_name.clone(), DataType::from(data_type)),
                        ColumnEntry::InternalColumn(TableInternalColumn {
                            internal_column,
                            ..
                        }) => (
                            internal_column.column_name().to_owned(),
                            internal_column.data_type(),
                        ),
                        ColumnEntry::VirtualColumn(_) | ColumnEntry::DerivedColumn(_) => {
                            return None;
                        }
                    };

                    // sort item is already a column
                    let scalar = RemoteExpr::ColumnRef {
                        span: None,
                        id: name.clone(),
                        data_type,
                        display_name: name,
                    };

                    Some((scalar, item.asc, item.nulls_first))
                })
                .collect::<Vec<_>>()
        });

        let order_by = order_by.unwrap_or_default();
        let mut limit = scan.limit;
        if let Some(scan_order_by) = &scan.order_by {
            // If some order by columns can't be pushed down, then the limit can't be pushed down either,
            // as this may cause some blocks are pruned by the limit pruner.
            if scan_order_by.len() != order_by.len() {
                limit = None;
            }
        }

        let virtual_column = self.build_virtual_column(virtual_columns)?;

        Ok(PushDownInfo {
            projection: Some(projection),
            output_columns,
            filters: push_down_filter,
            is_deterministic,
            prewhere: prewhere_info,
            limit,
            order_by,
            virtual_column,
            lazy_materialization: !metadata.lazy_columns().is_empty(),
            agg_index: None,
            change_type: scan.change_type.clone(),
            inverted_index: scan.inverted_index.clone(),
            vector_index: scan.vector_index.clone(),
            sample: scan.sample.clone(),
        })
    }

    fn build_prewhere_virtual_column_ids(&self, indices: &ColumnSet) -> Option<Vec<u32>> {
        let mut virtual_column_ids = Vec::new();
        for index in indices.iter() {
            if let ColumnEntry::VirtualColumn(virtual_column) = self.metadata.read().column(*index)
            {
                virtual_column_ids.push(virtual_column.column_id);
            }
        }
        if !virtual_column_ids.is_empty() {
            Some(virtual_column_ids)
        } else {
            None
        }
    }

    fn build_virtual_column(
        &self,
        virtual_columns: BTreeMap<IndexType, VirtualColumn>,
    ) -> Result<Option<VirtualColumnInfo>> {
        if virtual_columns.is_empty() {
            return Ok(None);
        }
        let mut source_column_ids = HashSet::new();
        let mut virtual_column_fields = Vec::with_capacity(virtual_columns.len());

        for (_, virtual_column) in virtual_columns.into_iter() {
            source_column_ids.insert(virtual_column.source_column_id);
            let target_type = virtual_column.data_type.remove_nullable();
            let cast_func_name = if target_type != TableDataType::Variant {
                Some(format!("to_{}", target_type.to_string().to_lowercase()))
            } else {
                None
            };

            let virtual_column_field = VirtualColumnField {
                source_column_id: virtual_column.source_column_id,
                source_name: virtual_column.source_column_name.clone(),
                column_id: virtual_column.column_id,
                name: virtual_column.column_name.clone(),
                key_paths: virtual_column.key_paths.clone(),
                cast_func_name,
                data_type: Box::new(virtual_column.data_type.clone()),
            };
            virtual_column_fields.push(virtual_column_field);
        }

        let virtual_column_info = VirtualColumnInfo {
            source_column_ids,
            virtual_column_fields,
        };
        Ok(Some(virtual_column_info))
    }

    pub fn build_agg_index(
        agg: &databend_common_sql::plans::AggIndexInfo,
        source_fields: &[DataField],
    ) -> Result<databend_common_catalog::plan::AggIndexInfo> {
        // Build projection
        let used_columns = agg.used_columns();
        let mut col_indices = Vec::with_capacity(used_columns.len());
        for index in used_columns.iter() {
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
                Ok(cast_expr_to_non_null_boolean(
                    pred.as_expr()?
                        .project_column_ref(|col| output_schema.index_of(&col.index.to_string()))?,
                )?
                .as_remote_expr())
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
                        .project_column_ref(|col| output_schema.index_of(&col.index.to_string()))?
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

    pub fn build_projection<'a>(
        metadata: &Metadata,
        schema: &TableSchema,
        columns: impl Iterator<Item = &'a IndexType>,
        has_inner_column: bool,
        ignore_internal_column: bool,
        add_virtual_source_column: bool,
    ) -> Projection {
        if !has_inner_column {
            let mut col_indices = Vec::new();
            let mut virtual_col_indices = HashSet::new();
            for index in columns {
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
