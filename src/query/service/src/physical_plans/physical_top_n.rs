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
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::runtime_filter_info::RuntimeTopNFilter;
use databend_common_catalog::table_context::TableContextRuntimeFilter;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::RemoteExpr;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::types::DataType;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;
use databend_common_pipeline_transforms::sorts::core::RowConverter;
use databend_common_pipeline_transforms::sorts::core::Rows;
use databend_common_pipeline_transforms::sorts::core::RowsTypeVisitor;
use databend_common_pipeline_transforms::sorts::core::SortKeyDescription;
use databend_common_pipeline_transforms::sorts::core::select_row_type;
use databend_common_sql::ColumnSet;
use databend_common_sql::executor::physical_plans::SortDesc;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_storages_parquet::ReadSettings;
use itertools::Itertools;

use crate::physical_plans::EvalScalar;
use crate::physical_plans::Filter;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::TableScan;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::FinalTopNFormatter;
use crate::physical_plans::format::PartialTopNFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::top_n::TransformFinalTopN;
use crate::pipelines::processors::transforms::top_n::TransformPartialTopN;
use crate::sessions::QueryContext;

/// Per-stream TopN candidate selection. It consumes unsorted input and emits
/// at most `candidate_count` internally sorted rows with an order column.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PartialTopNPlan {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub order_by: Vec<SortDesc>,
    pub candidate_count: usize,
    pub enable_fixed_rows: bool,

    // Only used for explain.
    pub stat_info: Option<PlanStatsInfo>,
}

/// Global TopN selection. It consumes sorted candidate blocks, applies the
/// final offset/limit and removes the internal order column.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FinalTopNPlan {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub order_by: Vec<SortDesc>,
    pub limit: usize,
    pub offset: usize,
    pub enable_fixed_rows: bool,

    // Only used for explain.
    pub stat_info: Option<PlanStatsInfo>,
}

/// Find the scan eligible for runtime TopN pruning.
///
/// Only row-preserving operators may sit between the partial TopN and the scan.
/// The generic `try_find_single_data_source` traversal is intentionally not used
/// here because operators such as Limit, Window, Sort, UDF, and ProjectSet also
/// expose their input source but may change which rows are eligible for TopN.
fn runtime_top_n_data_source(plan: &PhysicalPlan) -> Option<&DataSourcePlan> {
    if let Some(scan) = plan.as_any().downcast_ref::<TableScan>() {
        return Some(&scan.source);
    }
    if let Some(filter) = plan.as_any().downcast_ref::<Filter>() {
        return runtime_top_n_data_source(&filter.input);
    }
    if let Some(eval_scalar) = plan.as_any().downcast_ref::<EvalScalar>() {
        return runtime_top_n_data_source(&eval_scalar.input);
    }
    None
}

impl PartialTopNPlan {
    /// Runtime block pruning is only safe when this TopN is ordered by one
    /// Fuse column with orderable statistics and the scan has the exact same
    /// pushdown. Returns the scan id and the shared filter.
    fn runtime_top_n_filter(&self) -> Option<(usize, Arc<RuntimeTopNFilter>)> {
        if self.candidate_count == 0 || self.order_by.len() != 1 {
            return None;
        }

        let source = runtime_top_n_data_source(&self.input)?;
        let DataSourceInfo::TableSource(table_info) = &source.source_info else {
            return None;
        };
        if table_info.engine() != "FUSE" {
            return None;
        }

        // Cost gate: boundary-based pruning and scheduling only pay off when
        // the scan is substantially larger than the candidate set.
        if source.statistics.read_rows < self.candidate_count.saturating_mul(2) {
            return None;
        }

        let push_down = source.push_downs.as_ref()?;
        let [(expr, asc, nulls_first)] = push_down.order_by.as_slice() else {
            return None;
        };
        let desc = &self.order_by[0];
        if *asc != desc.asc || *nulls_first != desc.nulls_first {
            return None;
        }

        let RemoteExpr::ColumnRef { id, data_type, .. } = expr else {
            return None;
        };
        // The types Fuse maintains orderable min/max statistics for, and whose
        // `Scalar` ordering matches the sort ordering. Boolean and TimestampTz
        // have no usable statistics and are excluded.
        if id != &desc.display_name
            || !matches!(
                data_type.remove_nullable(),
                DataType::Number(_)
                    | DataType::Decimal(_)
                    | DataType::Date
                    | DataType::Timestamp
                    | DataType::String
            )
        {
            return None;
        }

        let schema = source.source_info.schema();
        let field = schema.field_with_name(id).ok()?;
        if DataType::from(field.data_type()) != *data_type {
            return None;
        }
        let column_ids = field.leaf_column_ids();
        let [column_id] = column_ids.as_slice() else {
            return None;
        };

        Some((
            source.scan_id,
            Arc::new(RuntimeTopNFilter::new(
                *column_id,
                desc.asc,
                desc.nulls_first,
            )),
        ))
    }
}

#[typetag::serde]
impl IPhysicalPlan for PartialTopNPlan {
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
        let input_schema = self.input.output_schema()?;
        let key_desc = SortKeyDescription::new(
            sort_column_descriptions(&self.order_by, &input_schema)?.into(),
            input_schema,
            self.enable_fixed_rows,
        )?;
        Ok(key_desc.schema_with_order_col())
    }

    fn children(&self) -> Box<dyn Iterator<Item = &'_ PhysicalPlan> + '_> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut(&mut self) -> Box<dyn Iterator<Item = &'_ mut PhysicalPlan> + '_> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(PartialTopNFormatter::create(self))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn get_desc(&self) -> Result<String> {
        Ok(format!(
            "{}; candidates {}",
            format_order_by(&self.order_by),
            self.candidate_count
        ))
    }

    fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        Ok(HashMap::from([(
            String::from("Number of candidates"),
            vec![self.candidate_count.to_string()],
        )]))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        PhysicalPlan::new(PartialTopNPlan {
            meta: self.meta.clone(),
            input: children.pop().unwrap(),
            order_by: self.order_by.clone(),
            candidate_count: self.candidate_count,
            enable_fixed_rows: self.enable_fixed_rows,
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        // Register before building the scan pipeline so every Fuse partition
        // producer/reader can retain the same live boundary handle.
        let runtime_top_n_filter = self.runtime_top_n_filter();
        if let Some((scan_id, filter)) = &runtime_top_n_filter {
            builder
                .ctx
                .register_runtime_top_n_filter(*scan_id, filter.clone());

            // Scope the filter to this pipeline: a later plan reusing the
            // same QueryContext with a colliding scan id must not observe a
            // stale boundary.
            let ctx = builder.ctx.clone();
            let scan_id = *scan_id;
            let filter = filter.clone();
            builder
                .main_pipeline
                .set_on_finished(move |_info: &ExecutionInfo| {
                    ctx.unregister_runtime_top_n_filter(scan_id, &filter);
                    Ok(())
                });
        }

        self.input.build_pipeline(builder)?;

        let input_schema = self.input.output_schema()?;
        let key_desc = SortKeyDescription::new(
            sort_column_descriptions(&self.order_by, &input_schema)?.into(),
            input_schema,
            self.enable_fixed_rows,
        )?;
        let spill_schema = key_desc.schema_with_order_col();
        let max_threads = builder.settings.get_max_threads()? as usize;
        if builder.main_pipeline.output_len() == 1 || max_threads == 1 {
            builder.main_pipeline.try_resize(max_threads)?;
        }

        let max_block_size = builder.settings.get_max_block_size()? as usize;
        let memory_settings = MemorySettings::from_sort_settings(&builder.ctx)?;
        let writer_pool_bytes = builder
            .settings
            .get_spill_writer_memory_pool_size_mb()?
            .saturating_mul(1024 * 1024);
        let read_settings = ReadSettings::from_settings(&builder.settings)?;
        let ctx = builder.ctx.clone();
        let candidate_count = self.candidate_count;
        let enable_fixed_rows = self.enable_fixed_rows;
        // Boundary rows are published from the source sort column, so pass its
        // payload offset along with the filter.
        let runtime_top_n_filter =
            runtime_top_n_filter.map(|(_, filter)| (key_desc.sort_column_desc()[0].offset, filter));

        builder.main_pipeline.add_transform(|input, output| {
            create_partial_top_n_processor(
                input,
                output,
                ctx.clone(),
                key_desc.clone(),
                candidate_count,
                max_block_size,
                memory_settings.clone(),
                spill_schema.clone(),
                writer_pool_bytes,
                read_settings,
                enable_fixed_rows,
                runtime_top_n_filter.clone(),
            )
        })
    }
}

#[typetag::serde]
impl IPhysicalPlan for FinalTopNPlan {
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
        candidate_input_schema(
            &self.order_by,
            self.input.output_schema()?,
            self.enable_fixed_rows,
        )
    }

    fn children(&self) -> Box<dyn Iterator<Item = &'_ PhysicalPlan> + '_> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut(&mut self) -> Box<dyn Iterator<Item = &'_ mut PhysicalPlan> + '_> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(FinalTopNFormatter::create(self))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn get_desc(&self) -> Result<String> {
        Ok(format!(
            "{}; limit {}; offset {}",
            format_order_by(&self.order_by),
            self.limit,
            self.offset
        ))
    }

    fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        Ok(HashMap::from([
            (String::from("Number of rows"), vec![self.limit.to_string()]),
            (String::from("Offset"), vec![self.offset.to_string()]),
        ]))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        PhysicalPlan::new(FinalTopNPlan {
            meta: self.meta.clone(),
            input: children.pop().unwrap(),
            order_by: self.order_by.clone(),
            limit: self.limit,
            offset: self.offset,
            enable_fixed_rows: self.enable_fixed_rows,
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let input_schema = self.input.output_schema()?;
        let schema =
            candidate_input_schema(&self.order_by, input_schema.clone(), self.enable_fixed_rows)?;
        let key_desc = SortKeyDescription::new(
            sort_column_descriptions(&self.order_by, &input_schema)?.into(),
            schema,
            self.enable_fixed_rows,
        )?;
        let max_block_size = builder.settings.get_max_block_size()? as usize;
        let limit = self.limit;
        let offset = self.offset;
        let enable_fixed_rows = self.enable_fixed_rows;

        builder.main_pipeline.try_resize(1)?;
        builder.main_pipeline.add_transform(|input, output| {
            create_final_top_n_processor(
                input,
                output,
                key_desc.clone(),
                limit,
                offset,
                max_block_size,
                enable_fixed_rows,
            )
        })
    }
}

fn sort_column_descriptions(
    order_by: &[SortDesc],
    schema: &DataSchema,
) -> Result<Vec<SortColumnDescription>> {
    order_by
        .iter()
        .map(|desc| {
            Ok(SortColumnDescription {
                offset: schema.index_of(&desc.order_by.to_string())?,
                asc: desc.asc,
                nulls_first: desc.nulls_first,
            })
        })
        .collect()
}

fn candidate_input_schema(
    order_by: &[SortDesc],
    schema: DataSchemaRef,
    enable_fixed_rows: bool,
) -> Result<DataSchemaRef> {
    SortKeyDescription::strip_order_col_schema(
        sort_column_descriptions(order_by, &schema)?.into(),
        schema,
        enable_fixed_rows,
    )
}

fn format_order_by(order_by: &[SortDesc]) -> String {
    order_by
        .iter()
        .map(|desc| {
            format!(
                "{}{}{}",
                desc.display_name,
                if desc.asc { "" } else { " DESC" },
                if desc.nulls_first { " NULLS FIRST" } else { "" }
            )
        })
        .join(", ")
}

#[allow(clippy::too_many_arguments)]
fn create_partial_top_n_processor(
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    ctx: Arc<QueryContext>,
    key_desc: SortKeyDescription,
    candidate_count: usize,
    max_block_size: usize,
    memory_settings: MemorySettings,
    spill_schema: DataSchemaRef,
    writer_pool_bytes: usize,
    read_settings: ReadSettings,
    enable_fixed_rows: bool,
    runtime_top_n_filter: Option<(usize, Arc<RuntimeTopNFilter>)>,
) -> Result<ProcessorPtr> {
    struct PartialTopNRowsVisitor {
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        ctx: Arc<QueryContext>,
        key_desc: SortKeyDescription,
        candidate_count: usize,
        max_block_size: usize,
        memory_settings: MemorySettings,
        spill_schema: DataSchemaRef,
        writer_pool_bytes: usize,
        read_settings: ReadSettings,
        runtime_top_n_filter: Option<(usize, Arc<RuntimeTopNFilter>)>,
    }

    impl RowsTypeVisitor for PartialTopNRowsVisitor {
        type Result = Result<ProcessorPtr>;

        fn sort_key_desc(&self) -> SortKeyDescription {
            self.key_desc.clone()
        }

        fn visit_type<R>(&mut self) -> Self::Result
        where
            R: Rows + 'static,
            R::Converter: Send + 'static,
        {
            let row_converter = if self.key_desc.uses_source_sort_col() {
                None
            } else {
                Some(R::Converter::new(self.key_desc.clone())?)
            };
            let transform = TransformPartialTopN::<R>::new(
                self.ctx.clone(),
                self.candidate_count,
                row_converter,
                self.key_desc.sort_row_offset(),
                self.max_block_size,
                self.memory_settings.clone(),
                self.spill_schema.clone(),
                self.writer_pool_bytes,
                self.read_settings,
                self.runtime_top_n_filter.clone(),
            );
            Ok(ProcessorPtr::create(AccumulatingTransformer::create(
                self.input.clone(),
                self.output.clone(),
                transform,
            )))
        }
    }

    let mut visitor = PartialTopNRowsVisitor {
        input,
        output,
        ctx,
        key_desc,
        candidate_count,
        max_block_size,
        memory_settings,
        spill_schema,
        writer_pool_bytes,
        read_settings,
        runtime_top_n_filter,
    };
    select_row_type(&mut visitor, enable_fixed_rows)
}

#[allow(clippy::too_many_arguments)]
fn create_final_top_n_processor(
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    key_desc: SortKeyDescription,
    limit: usize,
    offset: usize,
    max_block_size: usize,
    enable_fixed_rows: bool,
) -> Result<ProcessorPtr> {
    struct FinalTopNRowsVisitor {
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        key_desc: SortKeyDescription,
        limit: usize,
        offset: usize,
        max_block_size: usize,
    }

    impl RowsTypeVisitor for FinalTopNRowsVisitor {
        type Result = Result<ProcessorPtr>;

        fn sort_key_desc(&self) -> SortKeyDescription {
            self.key_desc.clone()
        }

        fn visit_type<R>(&mut self) -> Self::Result
        where
            R: Rows + 'static,
            R::Converter: Send + 'static,
        {
            let transform = TransformFinalTopN::<R>::new(
                self.limit,
                self.offset,
                !self.key_desc.uses_source_sort_col(),
                self.key_desc.sort_row_offset(),
                self.max_block_size,
            );
            Ok(ProcessorPtr::create(AccumulatingTransformer::create(
                self.input.clone(),
                self.output.clone(),
                transform,
            )))
        }
    }

    let mut visitor = FinalTopNRowsVisitor {
        input,
        output,
        key_desc,
        limit,
        offset,
        max_block_size,
    };
    select_row_type(&mut visitor, enable_fixed_rows)
}

impl PhysicalPlanBuilder {
    pub async fn build_top_n(
        &mut self,
        s_expr: &SExpr,
        top_n: &databend_common_sql::plans::TopN,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let apply_lazy_materialization = top_n.after_exchange != Some(false)
            && !top_n.lazy_columns.is_empty()
            && s_expr.unary_child().support_lazy_materialize();

        if apply_lazy_materialization {
            required = required.difference(&top_n.lazy_columns).copied().collect();
            required.extend(self.metadata.read().row_id_indexes());
        }
        required.extend(top_n.items.iter().map(|item| item.index));

        let order_by = top_n
            .items
            .iter()
            .map(|item| SortDesc {
                asc: item.asc,
                nulls_first: item.nulls_first,
                order_by: item.index,
                display_name: self.metadata.read().column(item.index).name(),
            })
            .collect::<Vec<_>>();
        let enable_fixed_rows = self.ctx.get_settings().get_enable_fixed_rows_sort()?;
        let input = self.build(s_expr.unary_child(), required).await?;

        let plan = match top_n.after_exchange {
            Some(false) => PhysicalPlan::new(PartialTopNPlan {
                meta: PhysicalPlanMeta::new("PartialTopN"),
                input,
                order_by,
                candidate_count: top_n.candidate_count(),
                enable_fixed_rows,
                stat_info: Some(stat_info.clone()),
            }),
            Some(true) => PhysicalPlan::new(FinalTopNPlan {
                meta: PhysicalPlanMeta::new("FinalTopN"),
                input,
                order_by,
                limit: top_n.limit,
                offset: top_n.offset,
                enable_fixed_rows,
                stat_info: Some(stat_info.clone()),
            }),
            None => {
                let input_stats = self.build_plan_stat_info(s_expr.unary_child())?;
                let partial_stats = PlanStatsInfo {
                    estimated_rows: input_stats
                        .estimated_rows
                        .min(top_n.candidate_count() as f64),
                };
                let partial = PhysicalPlan::new(PartialTopNPlan {
                    meta: PhysicalPlanMeta::new("PartialTopN"),
                    input,
                    order_by: order_by.clone(),
                    candidate_count: top_n.candidate_count(),
                    enable_fixed_rows,
                    stat_info: Some(partial_stats),
                });
                PhysicalPlan::new(FinalTopNPlan {
                    meta: PhysicalPlanMeta::new("FinalTopN"),
                    input: partial,
                    order_by,
                    limit: top_n.limit,
                    offset: top_n.offset,
                    enable_fixed_rows,
                    stat_info: Some(stat_info.clone()),
                })
            }
        };

        if apply_lazy_materialization {
            self.build_row_fetch_for_lazy_columns(plan, &top_n.lazy_columns, stat_info)
        } else {
            Ok(plan)
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_catalog::plan::PartStatistics;
    use databend_common_catalog::plan::Partitions;
    use databend_common_catalog::plan::PushDownInfo;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::NumberDataType;
    use databend_common_meta_app::schema::TableInfo;

    use super::*;
    use crate::physical_plans::WindowPartition;

    fn table_scan_with(
        scan_id: usize,
        schema: Arc<TableSchema>,
        push_downs: Option<PushDownInfo>,
        read_rows: usize,
    ) -> PhysicalPlan {
        let mut table_info = TableInfo::simple("default", "top_n_source", schema.clone());
        table_info.meta.engine = "FUSE".to_string();
        let source = DataSourcePlan {
            source_info: DataSourceInfo::TableSource(table_info),
            output_schema: schema,
            parts: Partitions::default(),
            statistics: PartStatistics {
                read_rows,
                ..PartStatistics::default()
            },
            description: String::new(),
            tbl_args: None,
            push_downs,
            internal_columns: None,
            base_block_ids: None,
            block_meta_options: Default::default(),
            table_index: 0,
            scan_id,
        };
        TableScan::create(
            scan_id,
            Default::default(),
            Box::new(source),
            None,
            None,
            None,
        )
    }

    fn table_scan(scan_id: usize) -> PhysicalPlan {
        table_scan_with(scan_id, Arc::new(TableSchema::empty()), None, 0)
    }

    fn partial_top_n(input: PhysicalPlan, candidate_count: usize) -> PartialTopNPlan {
        PartialTopNPlan {
            meta: PhysicalPlanMeta::new("PartialTopN"),
            input,
            order_by: vec![SortDesc {
                asc: false,
                nulls_first: false,
                order_by: databend_common_expression::Symbol::new(0),
                display_name: "a".to_string(),
            }],
            candidate_count,
            enable_fixed_rows: false,
            stat_info: None,
        }
    }

    fn nullable_int_pushdown() -> (Arc<TableSchema>, PushDownInfo) {
        let data_type = DataType::Number(NumberDataType::Int32).wrap_nullable();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "a",
            TableDataType::Number(NumberDataType::Int32).wrap_nullable(),
        )]));
        let push_downs = PushDownInfo {
            order_by: vec![(
                RemoteExpr::ColumnRef {
                    span: None,
                    id: "a".to_string(),
                    data_type,
                    display_name: "a".to_string(),
                },
                false,
                false,
            )],
            ..PushDownInfo::default()
        };
        (schema, push_downs)
    }

    #[test]
    fn runtime_top_n_filter_supports_nullable_keys_behind_a_cost_gate() {
        let (schema, push_downs) = nullable_int_pushdown();

        // Nullable sort keys are eligible, and the ordering is carried over.
        let scan = table_scan_with(3, schema.clone(), Some(push_downs.clone()), 1000);
        let (scan_id, filter) = partial_top_n(scan, 5).runtime_top_n_filter().unwrap();
        assert_eq!(scan_id, 3);
        assert!(!filter.asc());
        assert!(!filter.nulls_first());

        // The cost gate rejects scans not substantially larger than the
        // candidate set.
        let scan = table_scan_with(3, schema, Some(push_downs), 9);
        assert!(partial_top_n(scan, 5).runtime_top_n_filter().is_none());
    }

    #[test]
    fn runtime_top_n_source_only_crosses_row_preserving_wrappers() {
        let scan = table_scan(7);
        let filter = PhysicalPlan::new(Filter {
            meta: PhysicalPlanMeta::new("Filter"),
            projections: Default::default(),
            input: scan.clone(),
            predicates: vec![],
            stat_info: None,
            is_secure: false,
        });
        let eval_scalar =
            PhysicalPlan::new(EvalScalar::create(filter, vec![], Default::default(), None));

        assert_eq!(
            runtime_top_n_data_source(&eval_scalar).map(|source| source.scan_id),
            Some(7)
        );

        let window = PhysicalPlan::new(WindowPartition {
            meta: PhysicalPlanMeta::new("WindowPartition"),
            input: scan,
            partition_by: vec![],
            order_by: vec![],
            top_n: None,
            stat_info: None,
        });
        assert!(window.try_find_single_data_source().is_some());
        assert!(runtime_top_n_data_source(&window).is_none());
    }
}
