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
use std::assert_matches::debug_assert_matches;
use std::cmp;
use std::cmp::Ordering;
use std::fmt::Display;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::ReadPartitionsPruningMode;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::i256;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_pipeline_transforms::sorts::core::SortKeyDescription;
use databend_common_sql::BaseTableColumn;
use databend_common_sql::ColumnEntry;
use databend_common_sql::ColumnSet;
use databend_common_sql::IndexType;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::SortDesc;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::WindowFuncType;
use databend_common_storages_fuse::FuseBlockPartInfo;
use databend_common_storages_fuse::FuseLazyPartInfo;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::SegmentLocation;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::table::ClusterType;
use itertools::Itertools;

use crate::physical_plans::EvalScalar;
use crate::physical_plans::Exchange;
use crate::physical_plans::Filter;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::PhysicalPlanCast;
use crate::physical_plans::TableScan;
use crate::physical_plans::WindowPartition;
use crate::physical_plans::WindowPartitionTopN;
use crate::physical_plans::WindowPartitionTopNFunc;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::SortFormatter;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::builders::SortPipelineBuilder;
use crate::spillers::SortSpillerImpl;

type TransformSortBuilder =
    crate::pipelines::processors::transforms::TransformSortBuilder<SortSpillerImpl>;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Sort {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub order_by: Vec<SortDesc>,
    /// limit = Limit.limit + Limit.offset
    pub limit: Option<usize>,
    pub step: SortStep,
    pub pre_projection: Option<Vec<IndexType>>,
    pub broadcast_id: Option<u32>,
    pub enable_fixed_rows: bool,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[derive(Debug, Hash, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SortStep {
    // single node mode
    Single,

    // cluster mode
    Partial, // before the exchange plan
    Final,   // after the exchange plan

    // range shuffle mode
    Sample,
    Shuffled,
    Route,

    // Input streams are already sorted by the required keys.
    PresortedMerge,
}

impl Display for SortStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SortStep::Single => write!(f, "Single"),
            SortStep::Partial => write!(f, "Partial"),
            SortStep::Final => write!(f, "Final"),
            SortStep::Sample => write!(f, "Sample"),
            SortStep::Shuffled => write!(f, "Shuffled"),
            SortStep::Route => write!(f, "Route"),
            SortStep::PresortedMerge => write!(f, "PresortedMerge"),
        }
    }
}

#[typetag::serde]
impl IPhysicalPlan for Sort {
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
        match self.step {
            SortStep::Final | SortStep::Shuffled => SortKeyDescription::strip_order_col_schema(
                self.sort_desc(&input_schema)?.into(),
                input_schema,
                self.enable_fixed_rows,
            ),
            SortStep::Single | SortStep::Partial | SortStep::Sample | SortStep::PresortedMerge => {
                let projected_schema =
                    DataSchema::new_ref(self.fields_after_projection(&input_schema));
                if matches!(self.step, SortStep::Single | SortStep::PresortedMerge) {
                    return Ok(projected_schema);
                }
                let key_desc = SortKeyDescription::new(
                    self.sort_desc(&projected_schema)?.into(),
                    projected_schema,
                    self.enable_fixed_rows,
                )?;
                Ok(key_desc.schema_with_order_col())
            }
            SortStep::Route => Ok(input_schema),
        }
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(SortFormatter::create(self))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn get_desc(&self) -> Result<String> {
        Ok(self
            .order_by
            .iter()
            .map(|x| {
                format!(
                    "{}{}{}",
                    x.display_name,
                    if x.asc { "" } else { " DESC" },
                    if x.nulls_first { " NULLS FIRST" } else { "" },
                )
            })
            .join(", "))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(Sort {
            meta: self.meta.clone(),
            input,
            order_by: self.order_by.clone(),
            limit: self.limit,
            step: self.step,
            pre_projection: self.pre_projection.clone(),
            broadcast_id: self.broadcast_id,
            enable_fixed_rows: self.enable_fixed_rows,
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let sort_schema = self.sort_pipeline_schema()?;
        let sort_desc = self.sort_desc(&sort_schema)?.into();

        if self.step != SortStep::Shuffled {
            self.input.build_pipeline(builder)?;
        }

        if let Some(proj) = &self.pre_projection {
            debug_assert_matches!(
                self.step,
                SortStep::Single | SortStep::Partial | SortStep::Sample | SortStep::PresortedMerge
            );

            let input_schema = self.input.output_schema()?;
            // Do projection to reduce useless data copying during sorting.
            let projection = proj
                .iter()
                .map(|i| input_schema.index_of(&i.to_string()).unwrap())
                .collect::<Vec<_>>();

            builder.main_pipeline.add_transformer(|| {
                CompoundBlockOperator::new(
                    vec![BlockOperator::Project {
                        projection: projection.clone(),
                    }],
                    builder.func_ctx.clone(),
                    input_schema.num_fields(),
                )
            });
        }

        let sort_builder = SortPipelineBuilder::create(
            builder.ctx.clone(),
            sort_schema,
            sort_desc,
            self.broadcast_id,
            self.enable_fixed_rows,
        )?
        .with_limit(self.limit);

        let max_threads = builder.settings.get_max_threads()? as usize;
        match self.step {
            SortStep::Single => {
                // Build for single node mode.
                // We build the full sort pipeline for it.
                if builder.main_pipeline.output_len() == 1 || max_threads == 1 {
                    builder.main_pipeline.try_resize(max_threads)?;
                }
                sort_builder.build_full_sort_pipeline(&mut builder.main_pipeline, false)
            }

            SortStep::Partial => {
                // Build for each cluster node.
                // We build the full sort pipeline for it.
                // Don't remove the order column at last.
                if builder.main_pipeline.output_len() == 1 || max_threads == 1 {
                    builder.main_pipeline.try_resize(max_threads)?;
                }
                sort_builder.build_full_sort_pipeline(&mut builder.main_pipeline, true)
            }
            SortStep::Final => {
                // TODO(Winter): the query will hang in MultiSortMergeProcessor when max_threads == 1 and output_len != 1
                if max_threads == 1 && builder.main_pipeline.output_len() > 1 {
                    builder.main_pipeline.try_resize(1)?;
                    return sort_builder.build_merge_sort_pipeline(
                        &mut builder.main_pipeline,
                        true,
                        false,
                    );
                }

                // Build for the coordinator node.
                // We only build a `MultiSortMergeTransform`,
                // as the data is already sorted in each cluster node.
                // The input number of the transform is equal to the number of cluster nodes.
                sort_builder.build_multi_merge(&mut builder.main_pipeline, true)
            }

            SortStep::Sample => {
                if builder.main_pipeline.output_len() == 1 || max_threads == 1 {
                    builder.main_pipeline.try_resize(max_threads)?;
                }
                sort_builder.build_sample(&mut builder.main_pipeline)?;
                builder.exchange_injector = TransformSortBuilder::exchange_injector();
                Ok(())
            }
            SortStep::Shuffled => {
                if Exchange::check_physical_plan(&self.input) {
                    let exchange = TransformSortBuilder::exchange_injector();
                    let old_inject = std::mem::replace(&mut builder.exchange_injector, exchange);
                    self.input.build_pipeline(builder)?;
                    builder.exchange_injector = old_inject;
                } else {
                    self.input.build_pipeline(builder)?;
                }

                if builder.main_pipeline.output_len() == 1 {
                    return Ok(());
                }

                if max_threads == 1 {
                    // TODO(Winter): the query will hang in MultiSortMergeProcessor when max_threads == 1 and output_len != 1
                    unimplemented!();
                }
                sort_builder.build_bounded_merge_sort(&mut builder.main_pipeline)
            }
            SortStep::Route => {
                if builder.main_pipeline.output_len() == 1 {
                    builder
                        .main_pipeline
                        .add_transformer(TransformSortBuilder::build_dummy_route);
                    Ok(())
                } else {
                    TransformSortBuilder::add_route(&mut builder.main_pipeline)
                }
            }
            SortStep::PresortedMerge => {
                sort_builder.build_presorted_merge_pipeline(&mut builder.main_pipeline)
            }
        }
    }
}

impl Sort {
    fn sort_pipeline_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        match self.step {
            SortStep::Single | SortStep::Partial | SortStep::Sample | SortStep::PresortedMerge => {
                Ok(DataSchema::new_ref(
                    self.fields_after_projection(&input_schema),
                ))
            }
            SortStep::Final | SortStep::Shuffled => SortKeyDescription::strip_order_col_schema(
                self.sort_desc(&input_schema)?.into(),
                input_schema,
                self.enable_fixed_rows,
            ),
            SortStep::Route => Ok(input_schema),
        }
    }

    fn fields_after_projection(&self, input_schema: &DataSchema) -> Vec<DataField> {
        debug_assert_matches!(
            self.step,
            SortStep::Single | SortStep::Partial | SortStep::Sample | SortStep::PresortedMerge
        );
        self.pre_projection
            .as_ref()
            .and_then(|proj| {
                let fileted_fields = proj
                    .iter()
                    .map(|index| {
                        input_schema
                            .field_with_name(&index.to_string())
                            .unwrap()
                            .clone()
                    })
                    .collect::<Vec<_>>();

                if fileted_fields.len() < input_schema.fields().len() {
                    // Only if the projection is not a full projection, we need to add a projection transform.
                    Some(fileted_fields)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| input_schema.fields().clone())
    }

    fn sort_desc(&self, schema: &DataSchema) -> Result<Vec<SortColumnDescription>> {
        self.order_by
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
}

impl PhysicalPlanBuilder {
    pub async fn build_sort(
        &mut self,
        s_expr: &SExpr,
        sort: &databend_common_sql::plans::Sort,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        sort.items.iter().for_each(|s| {
            required.insert(s.index);
        });

        // If the query will be optimized by lazy reading, we don't need to do pre-projection.
        let pre_projection: Option<Vec<usize>> = if self.metadata.read().lazy_columns().is_empty() {
            sort.pre_projection
                .as_ref()
                .map(|projection| projection.iter().map(|_| unimplemented!()).collect())
        } else {
            None
        };

        let order_by = sort
            .items
            .iter()
            .map(|v| SortDesc {
                asc: v.asc,
                nulls_first: v.nulls_first,
                order_by: v.index,
                display_name: self.metadata.read().column(v.index).name(),
            })
            .collect::<Vec<_>>();

        // Add WindowPartition for parallel sort in window.
        if let Some(window) = &sort.window_partition {
            let window_partition = window
                .partition_by
                .iter()
                .map(|v| v.index)
                .collect::<Vec<_>>();

            assert!(sort.after_exchange.is_none());

            let input_plan = self.build(s_expr.unary_child(), required).await?;

            return Ok(PhysicalPlan::new(WindowPartition {
                meta: PhysicalPlanMeta::new("WindowPartition"),
                input: input_plan,
                partition_by: window_partition.clone(),
                order_by: order_by.clone(),
                top_n: window.top.map(|top| WindowPartitionTopN {
                    func: match window.func {
                        WindowFuncType::RowNumber => WindowPartitionTopNFunc::RowNumber,
                        WindowFuncType::Rank => WindowPartitionTopNFunc::Rank,
                        WindowFuncType::DenseRank => WindowPartitionTopNFunc::DenseRank,
                        _ => unreachable!(),
                    },
                    top,
                }),
                stat_info: Some(stat_info.clone()),
            }));
        };

        // 2. Build physical plan.
        let settings = self.ctx.get_settings();
        let enable_fixed_rows = settings.get_enable_fixed_rows_sort()?;

        let Some(after_exchange) = sort.after_exchange else {
            let mut input_plan = self.build(s_expr.unary_child(), required).await?;
            if sort.limit.is_some()
                && self
                    .prove_cluster_key_ordering(&mut input_plan, &order_by)
                    .await?
            {
                return Ok(PhysicalPlan::new(Sort {
                    input: input_plan,
                    order_by,
                    limit: sort.limit,
                    step: SortStep::PresortedMerge,
                    pre_projection,
                    broadcast_id: None,
                    enable_fixed_rows,
                    stat_info: Some(stat_info),
                    meta: PhysicalPlanMeta::new("Sort"),
                }));
            }
            return Ok(PhysicalPlan::new(Sort {
                input: input_plan,
                order_by,
                limit: sort.limit,
                step: SortStep::Single,
                pre_projection,
                broadcast_id: None,
                enable_fixed_rows,
                stat_info: Some(stat_info),
                meta: PhysicalPlanMeta::new("Sort"),
            }));
        };

        if !settings.get_enable_shuffle_sort()? || settings.get_max_threads()? == 1 {
            let input_plan = self.build(s_expr.unary_child(), required).await?;
            return if !after_exchange {
                Ok(PhysicalPlan::new(Sort {
                    input: input_plan,
                    order_by,
                    limit: sort.limit,
                    step: SortStep::Partial,
                    pre_projection,
                    broadcast_id: None,
                    enable_fixed_rows,
                    stat_info: Some(stat_info),
                    meta: PhysicalPlanMeta::new("Sort"),
                }))
            } else {
                Ok(PhysicalPlan::new(Sort {
                    input: input_plan,
                    order_by,
                    limit: sort.limit,
                    step: SortStep::Final,
                    pre_projection: None,
                    broadcast_id: None,
                    enable_fixed_rows,
                    stat_info: Some(stat_info),
                    meta: PhysicalPlanMeta::new("Sort"),
                }))
            };
        }

        if after_exchange {
            let input_plan = self.build(s_expr.unary_child(), required).await?;
            return Ok(PhysicalPlan::new(Sort {
                input: input_plan,
                order_by,
                limit: sort.limit,
                step: SortStep::Route,
                pre_projection: None,
                broadcast_id: None,
                enable_fixed_rows,
                stat_info: Some(stat_info),
                meta: PhysicalPlanMeta::new("Sort"),
            }));
        }

        let input_plan = self.build(s_expr.unary_child(), required).await?;
        let sample = PhysicalPlan::new(Sort {
            input: input_plan,
            order_by: order_by.clone(),
            limit: sort.limit,
            step: SortStep::Sample,
            pre_projection,
            broadcast_id: Some(self.ctx.broadcast_registry().next_broadcast_id()),
            enable_fixed_rows,
            stat_info: Some(stat_info.clone()),
            meta: PhysicalPlanMeta::new("Sort"),
        });
        let exchange = PhysicalPlan::new(Exchange {
            input: sample,
            kind: FragmentKind::Normal,
            keys: vec![],
            ignore_exchange: false,
            allow_adjust_parallelism: false,
            meta: PhysicalPlanMeta::new("Exchange"),
        });

        Ok(PhysicalPlan::new(Sort {
            input: exchange,
            order_by,
            limit: sort.limit,
            step: SortStep::Shuffled,
            pre_projection: None,
            broadcast_id: None,
            enable_fixed_rows,
            stat_info: Some(stat_info),
            meta: PhysicalPlanMeta::new("Sort"),
        }))
    }
}

impl PhysicalPlanBuilder {
    pub(super) async fn try_apply_presorted_merge_for_limit(
        &self,
        plan: &mut PhysicalPlan,
        limit: usize,
    ) -> Result<()> {
        let Some(sort) = Sort::from_mut_physical_plan(plan) else {
            return Ok(());
        };
        if sort.step != SortStep::Single {
            return Ok(());
        }

        let order_by = sort.order_by.clone();
        if self
            .prove_cluster_key_ordering(&mut sort.input, &order_by)
            .await?
        {
            sort.limit = Some(sort.limit.map_or(limit, |v| cmp::max(v, limit)));
            sort.step = SortStep::PresortedMerge;
        }
        Ok(())
    }

    async fn prove_cluster_key_ordering(
        &self,
        input_plan: &mut PhysicalPlan,
        order_by: &[SortDesc],
    ) -> Result<bool> {
        if order_by.is_empty() {
            return Ok(false);
        }
        if !self.ctx.get_cluster().is_empty() {
            // Preserve-order streams are assigned before source fragments are
            // redistributed. Until the scheduler can remap stream ids per
            // executor, keep the optimization local to single-node plans.
            return Ok(false);
        }

        let Some(sort_exprs) = self.sort_exprs(order_by) else {
            return Ok(false);
        };

        let Some((fuse_table, max_overlap, max_streams_limit)) = (|| {
            let settings = self.ctx.get_settings();
            if !settings.get_enable_cluster_key_ordered_topk().ok()? {
                return None;
            }

            let scan = ordered_table_scan(input_plan)?;
            let table_index = scan.table_index?;
            let table = self.metadata.read().table(table_index).table().clone();
            let fuse_table = table.as_any().downcast_ref::<FuseTable>()?;
            if !matches!(fuse_table.get_storage_format(), FuseStorageFormat::Parquet)
                || fuse_table
                    .cluster_type()
                    .is_none_or(|v| v != ClusterType::Linear)
            {
                return None;
            }

            let cluster_keys = fuse_table.linear_cluster_keys(self.ctx.clone());
            if !cluster_keys_cover_ordering(&cluster_keys, scan, &sort_exprs) {
                return None;
            }

            let max_overlap = settings.get_max_cluster_key_ordered_topk_overlap().ok()?;
            let max_streams = cmp::min(
                settings.get_max_threads().ok()? as usize,
                settings.get_max_storage_io_requests().ok()? as usize,
            );
            Some((fuse_table.clone(), max_overlap, max_streams))
        })() else {
            return Ok(false);
        };

        let Some(scan) = ordered_table_scan(input_plan) else {
            return Ok(false);
        };
        let (statistics, partitions) =
            match cluster_order_partitions(self.ctx.clone(), scan, &fuse_table).await {
                Ok(partitions) => partitions,
                Err(error) => {
                    log::debug!(
                        "skip cluster key ordered top-k: failed to expand lazy partitions: {:?}",
                        error
                    );
                    return Ok(false);
                }
            };
        if !ordering_minus_safe(&sort_exprs, scan, &partitions) {
            return Ok(false);
        }
        let max_streams = cmp::min(max_streams_limit, partitions.len());
        let Some(ordered_partitions) = ordered_cluster_partitions(
            &partitions,
            fuse_table.cluster_key_id(),
            max_overlap,
            max_streams,
        ) else {
            return Ok(false);
        };

        if let Some(scan) = ordered_table_scan_mut(input_plan) {
            scan.source.parts.kind = PartitionsShuffleKind::PreserveOrder;
            scan.source.parts.partitions = ordered_partitions;
            scan.source.statistics = statistics;
            return Ok(true);
        }

        Ok(false)
    }

    fn sort_exprs(&self, order_by: &[SortDesc]) -> Option<Vec<(RemoteExpr<String>, bool, bool)>> {
        let metadata = self.metadata.read();
        order_by
            .iter()
            .map(|item| {
                let column = metadata.column(item.order_by);
                let ColumnEntry::BaseTableColumn(BaseTableColumn {
                    column_name,
                    data_type,
                    ..
                }) = column
                else {
                    return None;
                };
                Some((
                    RemoteExpr::ColumnRef {
                        span: None,
                        id: column_name.clone(),
                        data_type: DataType::from(data_type),
                        display_name: column_name.clone(),
                    },
                    item.asc,
                    item.nulls_first,
                ))
            })
            .collect()
    }
}

fn cluster_keys_cover_ordering(
    cluster_keys: &[RemoteExpr<String>],
    scan: &TableScan,
    sort_exprs: &[(RemoteExpr<String>, bool, bool)],
) -> bool {
    let sort_exprs = sort_exprs
        .iter()
        .filter(|(sort_expr, _, _)| !scan_cluster_key_fixed_by_filters(sort_expr, scan))
        .collect::<Vec<_>>();
    if sort_exprs.is_empty() {
        return false;
    }

    let mut sort_index = 0;
    for cluster_key in cluster_keys {
        if scan_cluster_key_fixed_by_filters(cluster_key, scan) {
            continue;
        }

        let Some((sort_expr, asc, nulls_first)) = sort_exprs.get(sort_index) else {
            return true;
        };
        if !cluster_key_matches_order_by(cluster_key, sort_expr, *asc, *nulls_first) {
            return false;
        }

        sort_index += 1;
        if sort_index == sort_exprs.len() {
            return true;
        }
    }

    false
}

async fn cluster_order_partitions(
    ctx: Arc<dyn TableContext>,
    scan: &TableScan,
    fuse_table: &FuseTable,
) -> Result<(PartStatistics, Vec<PartInfoPtr>)> {
    if scan.source.parts.partitions_type() != PartInfoType::LazyLevel {
        return Ok((
            scan.source.statistics.clone(),
            scan.source.parts.partitions.clone(),
        ));
    }

    let snapshot = scan.source.statistics.snapshot.clone();
    let snapshot_block_count = fuse_table
        .read_table_snapshot_with_location(snapshot.clone())
        .await?
        .map(|snapshot| snapshot.summary.block_count as usize)
        .unwrap_or(scan.source.statistics.partitions_total);
    let mut segments = Vec::with_capacity(scan.source.parts.len());
    for part in &scan.source.parts.partitions {
        let Some(lazy_part) = part.as_any().downcast_ref::<FuseLazyPartInfo>() else {
            return Ok((scan.source.statistics.clone(), vec![]));
        };
        segments.push(SegmentLocation {
            segment_idx: lazy_part.segment_index,
            location: lazy_part.segment_location.clone(),
            snapshot_loc: snapshot.clone(),
        });
    }

    let (statistics, partitions, _) = fuse_table
        .prune_snapshot_blocks(
            ctx,
            scan.source.push_downs.clone(),
            fuse_table.schema_with_stream(),
            segments,
            snapshot_block_count,
            ReadPartitionsPruningMode::Normal,
            None,
        )
        .await?;
    Ok((statistics, partitions.partitions))
}

fn ordered_cluster_partitions(
    partitions: &[PartInfoPtr],
    cluster_key_id: Option<u32>,
    max_overlap: u64,
    max_streams_limit: usize,
) -> Option<Vec<PartInfoPtr>> {
    if partitions.is_empty() || max_streams_limit == 0 {
        return None;
    }
    let cluster_key_id = cluster_key_id?;
    if partitions.len() == 1 {
        let mut fuse_part = FuseBlockPartInfo::from_part(&partitions[0]).ok()?.clone();
        valid_cluster_stats(&fuse_part, cluster_key_id)?;
        fuse_part.preserve_order_stream = Some(0);
        return Some(vec![Arc::new(Box::new(fuse_part) as Box<dyn PartInfo>)]);
    }

    let mut partitions = partitions
        .iter()
        .map(|part| {
            let fuse_part = FuseBlockPartInfo::from_part(part).ok()?;
            let stats = valid_cluster_stats(fuse_part, cluster_key_id)?;
            Some((stats.min().clone(), stats.max().clone(), part.clone()))
        })
        .collect::<Option<Vec<_>>>()?;

    partitions.sort_by(|left, right| {
        compare_cluster_values(&left.0, &right.0)
            .then_with(|| compare_cluster_values(&left.1, &right.1))
    });

    // `max_overlap` is the number of extra simultaneously-overlapping ranges.
    // The active range count therefore includes the current range itself.
    let max_allowed_active_ranges = match max_overlap {
        v if v >= usize::MAX as u64 => usize::MAX,
        v => v as usize + 1,
    };
    let max_allowed_active_ranges = cmp::min(max_allowed_active_ranges, max_streams_limit);
    let mut active_ranges: Vec<Vec<Scalar>> = Vec::new();
    let mut max_observed_active_ranges = 0;
    for (min, max, _) in &partitions {
        if compare_cluster_values(min, max) == Ordering::Greater {
            return None;
        }

        active_ranges
            .retain(|active_max| compare_cluster_values(active_max, min) != Ordering::Less);
        active_ranges.push(max.clone());
        if active_ranges.len() > max_allowed_active_ranges {
            return None;
        }
        max_observed_active_ranges = cmp::max(max_observed_active_ranges, active_ranges.len());
    }

    // Non-overlapping ranges can stay in one stream. That lets LIMIT over a
    // clustered scan consume blocks sequentially and stop without waiting for
    // unrelated source streams to produce their first block.
    let max_streams = cmp::max(1, cmp::min(max_observed_active_ranges, max_streams_limit));
    let mut stream_ranges: Vec<Option<Vec<Scalar>>> = vec![None; max_streams];
    let mut next_stream = 0;
    let mut ordered_partitions: Vec<PartInfoPtr> = Vec::with_capacity(partitions.len());
    for (min, max, part) in partitions {
        let stream_index = (0..max_streams).find_map(|offset| {
            let stream_index = (next_stream + offset) % max_streams;
            stream_ranges[stream_index]
                .as_ref()
                .is_none_or(|stream_max| compare_cluster_values(stream_max, &min) == Ordering::Less)
                .then_some(stream_index)
        })?;
        stream_ranges[stream_index] = Some(max.clone());
        next_stream = (stream_index + 1) % max_streams;

        let mut part = FuseBlockPartInfo::from_part(&part).ok()?.clone();
        part.preserve_order_stream = Some(stream_index);
        ordered_partitions.push(Arc::new(Box::new(part) as Box<dyn PartInfo>));
    }

    Some(ordered_partitions)
}

fn valid_cluster_stats(
    part: &FuseBlockPartInfo,
    cluster_key_id: u32,
) -> Option<&ClusterStatistics> {
    part.cluster_stats
        .as_ref()
        .filter(|stats| stats.cluster_key_id == cluster_key_id)
        .filter(|stats| !stats.min().is_empty() && stats.min().len() == stats.max().len())
}

fn compare_cluster_values(
    left: &[databend_common_expression::Scalar],
    right: &[databend_common_expression::Scalar],
) -> Ordering {
    left.iter()
        .map(databend_common_expression::Scalar::as_ref)
        .cmp(right.iter().map(databend_common_expression::Scalar::as_ref))
}

fn ordered_table_scan(plan: &PhysicalPlan) -> Option<&TableScan> {
    if let Some(scan) = TableScan::from_physical_plan(plan) {
        return Some(scan);
    }
    if let Some(filter) = Filter::from_physical_plan(plan) {
        return ordered_table_scan(&filter.input);
    }
    if let Some(eval_scalar) = EvalScalar::from_physical_plan(plan) {
        return ordered_table_scan(&eval_scalar.input);
    }
    None
}

fn ordered_table_scan_mut(plan: &mut PhysicalPlan) -> Option<&mut TableScan> {
    if TableScan::check_physical_plan(plan) {
        return TableScan::from_mut_physical_plan(plan);
    }
    if Filter::check_physical_plan(plan) {
        let filter = Filter::from_mut_physical_plan(plan).unwrap();
        return ordered_table_scan_mut(&mut filter.input);
    }
    if EvalScalar::check_physical_plan(plan) {
        let eval_scalar = EvalScalar::from_mut_physical_plan(plan).unwrap();
        return ordered_table_scan_mut(&mut eval_scalar.input);
    }
    None
}

fn scan_cluster_key_fixed_by_filters(cluster_key: &RemoteExpr<String>, scan: &TableScan) -> bool {
    scan.source.push_downs.as_ref().is_some_and(|push_downs| {
        push_downs
            .filters
            .as_ref()
            .is_some_and(|filters| expr_fixed_by_filters(cluster_key, filters))
            || push_downs
                .secure_filters
                .as_ref()
                .is_some_and(|filters| expr_fixed_by_filters(cluster_key, filters))
            || push_downs
                .prewhere
                .as_ref()
                .is_some_and(|prewhere| expr_fixed_by_filter_expr(cluster_key, &prewhere.filter))
    })
}

fn expr_fixed_by_filters(expr: &RemoteExpr<String>, filters: &Filters) -> bool {
    expr_fixed_by_filter_expr(expr, &filters.filter)
}

fn expr_fixed_by_filter_expr(expr: &RemoteExpr<String>, filter: &RemoteExpr<String>) -> bool {
    let RemoteExpr::FunctionCall { id, args, .. } = filter else {
        return false;
    };
    match id.name().as_ref() {
        "and" | "and_filters" => args.iter().any(|arg| expr_fixed_by_filter_expr(expr, arg)),
        "is_true" if args.len() == 1 => expr_fixed_by_filter_expr(expr, &args[0]),
        "is_null" if args.len() == 1 => {
            remote_expr_semantic_eq(expr, filter) || expr_matches_filter_arg(expr, &args[0])
        }
        "not" if args.len() == 1 => {
            remote_expr_semantic_eq(expr, filter)
                || expr_fixed_by_is_not_null_negation(expr, &args[0])
        }
        "contains" if args.len() == 2 => {
            single_constant_array_expr(&args[0]) && expr_matches_filter_arg(expr, &args[1])
        }
        "eq" if args.len() == 2 => {
            expr_eq_constant(expr, &args[0], &args[1]) || expr_eq_constant(expr, &args[1], &args[0])
        }
        _ => false,
    }
}

fn expr_fixed_by_is_not_null_negation(
    expr: &RemoteExpr<String>,
    maybe_is_not_null: &RemoteExpr<String>,
) -> bool {
    let RemoteExpr::FunctionCall { id, args, .. } = maybe_is_not_null else {
        return false;
    };
    if id.name().as_ref() != "is_not_null" || args.len() != 1 {
        return false;
    }

    is_null_cluster_key_matches_expr(expr, &args[0]) || expr_matches_filter_arg(expr, &args[0])
}

fn is_null_cluster_key_matches_expr(
    cluster_key: &RemoteExpr<String>,
    filter_expr: &RemoteExpr<String>,
) -> bool {
    let RemoteExpr::FunctionCall { id, args, .. } = cluster_key else {
        return false;
    };
    id.name().as_ref() == "is_null"
        && args.len() == 1
        && expr_matches_filter_arg(&args[0], filter_expr)
}

fn expr_eq_constant(
    expr: &RemoteExpr<String>,
    maybe_expr: &RemoteExpr<String>,
    maybe_constant: &RemoteExpr<String>,
) -> bool {
    expr_is_constant(maybe_constant) && expr_matches_filter_arg(expr, maybe_expr)
}

fn expr_is_constant(expr: &RemoteExpr<String>) -> bool {
    match expr {
        RemoteExpr::Constant { .. } => true,
        RemoteExpr::Cast { is_try, expr, .. } if !*is_try => expr_is_constant(expr),
        _ => false,
    }
}

fn single_constant_array_expr(expr: &RemoteExpr<String>) -> bool {
    match expr {
        RemoteExpr::Constant {
            scalar: Scalar::Array(array),
            ..
        } => array.len() == 1,
        RemoteExpr::FunctionCall { id, args, .. } if id.name().as_ref() == "array" => {
            matches!(args.as_slice(), [arg] if expr_is_constant(arg))
        }
        RemoteExpr::FunctionCall { id, args, .. } if id.name().as_ref() == "array_distinct" => {
            matches!(args.as_slice(), [arg] if single_constant_array_expr(arg))
        }
        RemoteExpr::Cast { is_try, expr, .. } if !*is_try => single_constant_array_expr(expr),
        _ => false,
    }
}

fn expr_matches_filter_arg(expr: &RemoteExpr<String>, maybe_expr: &RemoteExpr<String>) -> bool {
    remote_expr_semantic_eq(maybe_expr, expr)
        || cast_expr_matches_expr(maybe_expr, expr)
        || negated_column_matches_expr(expr, maybe_expr)
        || string_prefix_cluster_key_matches_expr(expr, maybe_expr)
}

fn cast_expr_matches_expr(maybe_cast: &RemoteExpr<String>, expr: &RemoteExpr<String>) -> bool {
    let RemoteExpr::Cast {
        is_try,
        expr: inner,
        dest_type,
        ..
    } = maybe_cast
    else {
        return false;
    };
    !*is_try
        && cast_preserves_ordering_type(dest_type, expr)
        && expr_matches_filter_arg(expr, inner)
}

fn cast_preserves_ordering_type(dest_type: &DataType, expr: &RemoteExpr<String>) -> bool {
    let source_type = remote_expr_data_type(expr);
    source_type.remove_nullable() == dest_type.remove_nullable()
}

fn remote_expr_data_type(expr: &RemoteExpr<String>) -> &DataType {
    match expr {
        RemoteExpr::Constant { data_type, .. } => data_type,
        RemoteExpr::ColumnRef { data_type, .. } => data_type,
        RemoteExpr::Cast { dest_type, .. } => dest_type,
        RemoteExpr::FunctionCall { return_type, .. } => return_type,
        RemoteExpr::LambdaFunctionCall { return_type, .. } => return_type,
    }
}

fn string_prefix_cluster_key_matches_expr(
    cluster_key: &RemoteExpr<String>,
    filter_expr: &RemoteExpr<String>,
) -> bool {
    let RemoteExpr::FunctionCall { id, args, .. } = cluster_key else {
        return false;
    };
    if id.name().as_ref() != "substr" || args.len() != 3 {
        return false;
    }

    expr_matches_filter_arg(&args[0], filter_expr)
        && is_number_constant(&args[1], 1)
        && is_number_constant(&args[2], 8)
}

fn is_number_constant(expr: &RemoteExpr<String>, expected: i128) -> bool {
    match expr {
        RemoteExpr::Constant {
            scalar: Scalar::Number(number),
            ..
        } => number.integer_to_i128().is_some_and(|v| v == expected),
        RemoteExpr::Cast { is_try, expr, .. } if !*is_try => is_number_constant(expr, expected),
        _ => false,
    }
}

fn ordering_minus_safe(
    sort_exprs: &[(RemoteExpr<String>, bool, bool)],
    scan: &TableScan,
    partitions: &[PartInfoPtr],
) -> bool {
    sort_exprs
        .iter()
        .filter(|(_, asc, nulls_first)| !*asc && !*nulls_first)
        .all(|(sort_expr, _, _)| ordering_minus_safe_for_expr(sort_expr, scan, partitions))
}

fn ordering_minus_safe_for_expr(
    sort_expr: &RemoteExpr<String>,
    scan: &TableScan,
    partitions: &[PartInfoPtr],
) -> bool {
    let RemoteExpr::ColumnRef { id, data_type, .. } = sort_expr else {
        return false;
    };

    match data_type.remove_nullable() {
        DataType::Number(NumberDataType::Int64) => {
            let Ok(column_id) = scan.source.schema().column_id_of(id) else {
                return false;
            };
            partitions
                .iter()
                .all(|part| partition_column_stats(part, column_id).is_some_and(i64_min_safe))
        }
        DataType::Decimal(_) => {
            let Ok(column_id) = scan.source.schema().column_id_of(id) else {
                return false;
            };
            partitions
                .iter()
                .all(|part| partition_column_stats(part, column_id).is_some_and(decimal_min_safe))
        }
        _ => true,
    }
}

fn partition_column_stats(
    part: &PartInfoPtr,
    column_id: u32,
) -> Option<&databend_storages_common_table_meta::meta::ColumnStatistics> {
    let fuse_part = FuseBlockPartInfo::from_part(part).ok()?;
    fuse_part
        .columns_stat
        .as_ref()
        .and_then(|stats| stats.get(&column_id))
}

fn i64_min_safe(stat: &databend_storages_common_table_meta::meta::ColumnStatistics) -> bool {
    !matches!(
        stat.min(),
        Scalar::Number(NumberScalar::Int64(v)) if *v == i64::MIN
    )
}

fn decimal_min_safe(stat: &databend_storages_common_table_meta::meta::ColumnStatistics) -> bool {
    !matches!(
        stat.min(),
        Scalar::Decimal(DecimalScalar::Decimal64(v, _)) if *v == <i64 as Decimal>::DECIMAL_MIN
    ) && !matches!(
        stat.min(),
        Scalar::Decimal(DecimalScalar::Decimal128(v, _)) if *v == <i128 as Decimal>::DECIMAL_MIN
    ) && !matches!(
        stat.min(),
        Scalar::Decimal(DecimalScalar::Decimal256(v, _)) if *v == <i256 as Decimal>::DECIMAL_MIN
    )
}

fn cluster_key_matches_order_by(
    cluster_key: &RemoteExpr<String>,
    sort_expr: &RemoteExpr<String>,
    asc: bool,
    nulls_first: bool,
) -> bool {
    if asc && !nulls_first && remote_expr_semantic_eq(cluster_key, sort_expr) {
        return true;
    }

    !asc && !nulls_first && matches_negated_column(cluster_key, sort_expr)
}

fn matches_negated_column(
    cluster_key: &RemoteExpr<String>,
    sort_expr: &RemoteExpr<String>,
) -> bool {
    let RemoteExpr::ColumnRef { data_type, .. } = sort_expr else {
        return false;
    };
    supports_order_reversing_minus(data_type) && negated_column_matches_expr(cluster_key, sort_expr)
}

fn negated_column_matches_expr(
    cluster_key: &RemoteExpr<String>,
    sort_expr: &RemoteExpr<String>,
) -> bool {
    let RemoteExpr::FunctionCall { id, args, .. } = cluster_key else {
        return false;
    };
    if id.name().as_ref() != "minus" || args.len() != 1 {
        return false;
    }

    let RemoteExpr::ColumnRef {
        id: sort_column, ..
    } = sort_expr
    else {
        return false;
    };

    matches!(
        &args[0],
        RemoteExpr::ColumnRef {
            id: cluster_column,
            ..
        } if cluster_column == sort_column
    )
}

fn remote_expr_semantic_eq(left: &RemoteExpr<String>, right: &RemoteExpr<String>) -> bool {
    match (left, right) {
        (
            RemoteExpr::Constant {
                scalar: left_scalar,
                data_type: left_type,
                ..
            },
            RemoteExpr::Constant {
                scalar: right_scalar,
                data_type: right_type,
                ..
            },
        ) => left_scalar == right_scalar && left_type == right_type,
        (
            RemoteExpr::ColumnRef {
                id: left_id,
                data_type: left_type,
                ..
            },
            RemoteExpr::ColumnRef {
                id: right_id,
                data_type: right_type,
                ..
            },
        ) => left_id == right_id && left_type == right_type,
        (
            RemoteExpr::Cast {
                is_try: left_is_try,
                expr: left_expr,
                dest_type: left_dest_type,
                ..
            },
            RemoteExpr::Cast {
                is_try: right_is_try,
                expr: right_expr,
                dest_type: right_dest_type,
                ..
            },
        ) => {
            left_is_try == right_is_try
                && left_dest_type == right_dest_type
                && remote_expr_semantic_eq(left_expr, right_expr)
        }
        (
            RemoteExpr::FunctionCall {
                id: left_id,
                generics: left_generics,
                args: left_args,
                return_type: left_return_type,
                ..
            },
            RemoteExpr::FunctionCall {
                id: right_id,
                generics: right_generics,
                args: right_args,
                return_type: right_return_type,
                ..
            },
        ) => {
            left_id.name() == right_id.name()
                && left_generics == right_generics
                && left_return_type == right_return_type
                && left_args.len() == right_args.len()
                && left_args
                    .iter()
                    .zip(right_args)
                    .all(|(left_arg, right_arg)| remote_expr_semantic_eq(left_arg, right_arg))
        }
        (RemoteExpr::LambdaFunctionCall { .. }, RemoteExpr::LambdaFunctionCall { .. }) => false,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_expression::FunctionID;
    use databend_common_expression::types::NumberScalar;
    use databend_storages_common_table_meta::meta::Compression;

    use super::*;

    fn scalar(value: i64) -> Scalar {
        Scalar::Number(NumberScalar::Int64(value))
    }

    fn builtin(name: &str) -> Box<FunctionID> {
        Box::new(FunctionID::Builtin {
            name: name.to_string(),
            id: 0,
        })
    }

    fn column(name: &str, data_type: DataType) -> RemoteExpr<String> {
        RemoteExpr::ColumnRef {
            span: None,
            id: name.to_string(),
            data_type,
            display_name: name.to_string(),
        }
    }

    fn int_column(name: &str) -> RemoteExpr<String> {
        column(name, DataType::Number(NumberDataType::Int32))
    }

    fn int_constant(value: i32) -> RemoteExpr<String> {
        RemoteExpr::Constant {
            span: None,
            scalar: Scalar::Number(NumberScalar::Int32(value)),
            data_type: DataType::Number(NumberDataType::Int32),
        }
    }

    fn function(
        name: &str,
        args: Vec<RemoteExpr<String>>,
        return_type: DataType,
    ) -> RemoteExpr<String> {
        RemoteExpr::FunctionCall {
            span: None,
            id: builtin(name),
            generics: vec![],
            args,
            return_type,
        }
    }

    fn eq(left: RemoteExpr<String>, right: RemoteExpr<String>) -> RemoteExpr<String> {
        function("eq", vec![left, right], DataType::Boolean)
    }

    fn is_null(expr: RemoteExpr<String>) -> RemoteExpr<String> {
        function("is_null", vec![expr], DataType::Boolean)
    }

    fn is_not_null(expr: RemoteExpr<String>) -> RemoteExpr<String> {
        function("is_not_null", vec![expr], DataType::Boolean)
    }

    fn not(expr: RemoteExpr<String>) -> RemoteExpr<String> {
        function("not", vec![expr], DataType::Boolean)
    }

    fn contains(array: RemoteExpr<String>, expr: RemoteExpr<String>) -> RemoteExpr<String> {
        function("contains", vec![array, expr], DataType::Boolean)
    }

    fn array(args: Vec<RemoteExpr<String>>) -> RemoteExpr<String> {
        function(
            "array",
            args,
            DataType::Array(Box::new(DataType::Number(NumberDataType::Int32))),
        )
    }

    fn and(args: Vec<RemoteExpr<String>>) -> RemoteExpr<String> {
        function("and", args, DataType::Boolean)
    }

    fn part(location: &str, min: i64, max: i64) -> PartInfoPtr {
        FuseBlockPartInfo::create(
            location.to_string(),
            None,
            0,
            None,
            0,
            1,
            HashMap::new(),
            None,
            None,
            Compression::legacy(),
            None,
            Some(ClusterStatistics::new(
                7,
                vec![scalar(min)],
                vec![scalar(max)],
                0,
                None,
            )),
            None,
            None,
        )
    }

    fn stream_ids(parts: &[PartInfoPtr]) -> Vec<usize> {
        parts
            .iter()
            .map(|part| {
                FuseBlockPartInfo::from_part(part)
                    .unwrap()
                    .preserve_order_stream
                    .unwrap()
            })
            .collect()
    }

    #[test]
    fn test_ordered_cluster_partitions_uses_single_stream_for_non_overlap() {
        let parts = vec![part("b", 10, 19), part("a", 0, 8), part("c", 21, 29)];

        let ordered = ordered_cluster_partitions(&parts, Some(7), 10, 8).unwrap();

        assert_eq!(stream_ids(&ordered), vec![0, 0, 0]);
        let locations = ordered
            .iter()
            .map(|part| {
                FuseBlockPartInfo::from_part(part)
                    .unwrap()
                    .location
                    .as_str()
            })
            .collect::<Vec<_>>();
        assert_eq!(locations, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_ordered_cluster_partitions_uses_overlap_width_streams() {
        let parts = vec![part("a", 0, 10), part("b", 5, 15), part("c", 12, 20)];

        let ordered = ordered_cluster_partitions(&parts, Some(7), 10, 8).unwrap();
        let stream_ids = stream_ids(&ordered);

        assert_eq!(stream_ids, vec![0, 1, 0]);
        assert_eq!(stream_ids.iter().max().copied().unwrap() + 1, 2);
    }

    #[test]
    fn test_ordered_cluster_partitions_treats_touching_ranges_as_overlap() {
        let parts = vec![part("a", 0, 10), part("b", 10, 20), part("c", 21, 30)];

        let ordered = ordered_cluster_partitions(&parts, Some(7), 10, 8).unwrap();
        let stream_ids = stream_ids(&ordered);

        assert_eq!(stream_ids, vec![0, 1, 0]);
        assert_eq!(stream_ids.iter().max().copied().unwrap() + 1, 2);
    }

    #[test]
    fn test_ordered_cluster_partitions_rejects_excess_overlap() {
        let parts = vec![part("a", 0, 10), part("b", 5, 15), part("c", 7, 20)];

        assert!(ordered_cluster_partitions(&parts, Some(7), 1, 8).is_none());
    }

    #[test]
    fn test_filter_fixed_expr_matches_single_value_in_and_is_null_rewrite() {
        let tag = column("tag", DataType::Nullable(Box::new(DataType::String)));
        let filter = and(vec![
            eq(int_column("a"), int_constant(1)),
            contains(array(vec![int_constant(2)]), int_column("b")),
            not(is_not_null(tag.clone())),
            eq(int_column("e"), int_constant(3)),
        ]);

        assert!(expr_fixed_by_filter_expr(&int_column("a"), &filter));
        assert!(expr_fixed_by_filter_expr(&int_column("b"), &filter));
        assert!(expr_fixed_by_filter_expr(&is_null(tag.clone()), &filter));
        assert!(expr_fixed_by_filter_expr(
            &not(is_not_null(tag.clone())),
            &filter
        ));
        assert!(expr_fixed_by_filter_expr(&int_column("e"), &filter));
        assert!(!expr_fixed_by_filter_expr(&int_column("c"), &filter));
        assert!(!expr_fixed_by_filter_expr(
            &int_column("b"),
            &contains(
                array(vec![int_constant(2), int_constant(3)]),
                int_column("b")
            )
        ));
    }

    #[test]
    fn test_supports_order_reversing_minus_for_safe_integer_widths() {
        assert!(supports_order_reversing_minus(&DataType::Number(
            NumberDataType::UInt8
        )));
        assert!(supports_order_reversing_minus(&DataType::Number(
            NumberDataType::UInt16
        )));
        assert!(supports_order_reversing_minus(&DataType::Number(
            NumberDataType::UInt32
        )));
        assert!(supports_order_reversing_minus(&DataType::Number(
            NumberDataType::Int8
        )));
        assert!(supports_order_reversing_minus(&DataType::Number(
            NumberDataType::Int16
        )));
        assert!(supports_order_reversing_minus(&DataType::Number(
            NumberDataType::Int32
        )));
        assert!(supports_order_reversing_minus(&DataType::Number(
            NumberDataType::Int64
        )));
        assert!(!supports_order_reversing_minus(&DataType::Number(
            NumberDataType::UInt64
        )));
    }
}

fn supports_order_reversing_minus(data_type: &DataType) -> bool {
    match data_type.remove_nullable() {
        // Unary minus widens these integer types, so their minimum values do
        // not overflow the cluster-key expression.
        DataType::Number(
            NumberDataType::UInt8 | NumberDataType::UInt16 | NumberDataType::UInt32,
        ) => true,
        DataType::Number(NumberDataType::Int8 | NumberDataType::Int16 | NumberDataType::Int32) => {
            true
        }
        DataType::Number(NumberDataType::Int64) => true,
        DataType::Decimal(_) => true,
        // UInt64/Float boundary ordering is not proven here.
        DataType::Number(
            NumberDataType::UInt64 | NumberDataType::Float32 | NumberDataType::Float64,
        ) => false,
        _ => false,
    }
}
