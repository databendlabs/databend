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
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartInfoType;
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
        let configured_max_presorted_streams = self
            .ctx
            .get_settings()
            .get_max_cluster_key_ordered_topk_overlap()?;
        if configured_max_presorted_streams == 0 {
            return Ok(false);
        }

        let Some(sort_exprs) = self.sort_exprs(order_by) else {
            return Ok(false);
        };

        let Some((fuse_table, max_presorted_streams)) = (|| {
            let scan = ordered_table_scan(input_plan)?;
            let table_index = scan.table_index?;
            let table = self.metadata.read().table(table_index).table().clone();
            let fuse_table = table.as_any().downcast_ref::<FuseTable>()?;
            if fuse_table
                .cluster_type()
                .is_none_or(|v| v != ClusterType::Linear)
            {
                return None;
            }

            let settings = self.ctx.get_settings();
            let max_presorted_streams = cmp::min(
                configured_max_presorted_streams,
                cmp::min(
                    settings.get_max_threads().ok()? as usize,
                    settings.get_max_storage_io_requests().ok()? as usize,
                ),
            );
            Some((fuse_table.clone(), max_presorted_streams))
        })() else {
            return Ok(false);
        };

        let Some(scan) = ordered_table_scan(input_plan) else {
            return Ok(false);
        };
        let partitions = cluster_order_partitions(self.ctx.clone(), scan, &fuse_table).await?;
        let Some(cluster_key_id) = fuse_table.cluster_key_id() else {
            return Ok(false);
        };

        let mut pruned_scan = scan.clone();
        pruned_scan.source.parts.partitions = partitions.clone();
        let cluster_keys = fuse_table.linear_cluster_keys(self.ctx.clone());
        if !cluster_keys_cover_ordering(&cluster_keys, &pruned_scan, &sort_exprs, cluster_key_id) {
            return Ok(false);
        }

        let Some(ordered_partitions) =
            ordered_cluster_partitions(&partitions, Some(cluster_key_id), max_presorted_streams)
        else {
            return Ok(false);
        };

        if let Some(scan) = ordered_table_scan_mut(input_plan) {
            scan.source.parts.kind = PartitionsShuffleKind::PreserveOrder;
            scan.source.parts.partitions = ordered_partitions;
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
    cluster_key_id: u32,
) -> bool {
    let sort_exprs = sort_exprs
        .iter()
        .filter(|(sort_expr, _, _)| !scan_cluster_key_fixed_by_filters(sort_expr, scan))
        .collect::<Vec<_>>();
    if sort_exprs.is_empty() {
        return false;
    }

    let mut sort_index = 0;
    for (cluster_key_index, cluster_key) in cluster_keys.iter().enumerate() {
        if scan_cluster_key_fixed_by_filters(cluster_key, scan)
            || cluster_key_component_fixed_by_parts(scan, cluster_key_id, cluster_key_index)
        {
            continue;
        }

        let Some((sort_expr, asc, nulls_first)) = sort_exprs.get(sort_index) else {
            return true;
        };
        if !cluster_key_matches_order_by(cluster_key, sort_expr, *asc, *nulls_first, scan) {
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
) -> Result<Vec<PartInfoPtr>> {
    if scan.source.parts.partitions_type() != PartInfoType::LazyLevel {
        return Ok(scan.source.parts.partitions.clone());
    }

    let snapshot = scan.source.statistics.snapshot.clone();
    let mut segments = Vec::with_capacity(scan.source.parts.len());
    for part in &scan.source.parts.partitions {
        let Some(lazy_part) = part.as_any().downcast_ref::<FuseLazyPartInfo>() else {
            return Ok(vec![]);
        };
        segments.push(SegmentLocation {
            segment_idx: lazy_part.segment_index,
            location: lazy_part.segment_location.clone(),
            snapshot_loc: snapshot.clone(),
        });
    }

    let (_, partitions, _) = fuse_table
        .prune_snapshot_blocks(
            ctx,
            scan.source.push_downs.clone(),
            fuse_table.schema_with_stream(),
            segments,
            scan.source.statistics.partitions_total,
            ReadPartitionsPruningMode::Normal,
            None,
        )
        .await?;
    Ok(partitions.partitions)
}

fn cluster_key_component_fixed_by_parts(
    scan: &TableScan,
    cluster_key_id: u32,
    component_index: usize,
) -> bool {
    let mut fixed_value = None;

    for part in &scan.source.parts.partitions {
        let Ok(part) = FuseBlockPartInfo::from_part(part) else {
            return false;
        };
        let Some(stats) = valid_cluster_stats(part, cluster_key_id) else {
            return false;
        };
        let (Some(min), Some(max)) = (
            stats.min().get(component_index),
            stats.max().get(component_index),
        ) else {
            return false;
        };
        if min != max {
            return false;
        }
        match &fixed_value {
            Some(value) if value != min => return false,
            None => fixed_value = Some(min.clone()),
            _ => {}
        }
    }

    fixed_value.is_some()
}

fn ordered_cluster_partitions(
    partitions: &[PartInfoPtr],
    cluster_key_id: Option<u32>,
    max_streams: usize,
) -> Option<Vec<PartInfoPtr>> {
    if partitions.is_empty() || max_streams == 0 {
        return None;
    }
    let cluster_key_id = cluster_key_id?;
    if partitions.len() == 1 {
        let fuse_part = FuseBlockPartInfo::from_part(&partitions[0]).ok()?;
        valid_cluster_stats(fuse_part, cluster_key_id)?;
        return Some(partitions.to_vec());
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

    let mut stream_maxes: Vec<Vec<databend_common_expression::Scalar>> = Vec::new();
    for (min, max, _) in &partitions {
        let reusable_stream = stream_maxes
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| compare_cluster_values(left, right))
            .and_then(|(index, stream_max)| {
                (compare_cluster_values(stream_max, min) != Ordering::Greater).then_some(index)
            });

        if let Some(index) = reusable_stream {
            stream_maxes[index] = max.clone();
            continue;
        }
        if stream_maxes.len() == max_streams {
            return None;
        }
        stream_maxes.push(max.clone());
    }

    Some(partitions.into_iter().map(|(_, _, part)| part).collect())
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
    if null_predicate_fixed_by_filter(expr, filter) {
        return true;
    }

    let RemoteExpr::FunctionCall { id, args, .. } = filter else {
        return false;
    };
    match id.name().as_ref() {
        "and" | "and_filters" if args.len() == 2 => {
            args.iter().any(|arg| expr_fixed_by_filter_expr(expr, arg))
        }
        "is_true" if args.len() == 1 => expr_fixed_by_filter_expr(expr, &args[0]),
        "eq" if args.len() == 2 => {
            expr_eq_constant(expr, &args[0], &args[1]) || expr_eq_constant(expr, &args[1], &args[0])
        }
        _ => false,
    }
}

fn null_predicate_fixed_by_filter(expr: &RemoteExpr<String>, filter: &RemoteExpr<String>) -> bool {
    let Some(expr_arg) = null_predicate_arg(expr) else {
        return false;
    };

    let Some(filter_arg) = null_predicate_arg(filter) else {
        return false;
    };

    remote_expr_semantic_eq(expr_arg, filter_arg)
}

fn null_predicate_arg(expr: &RemoteExpr<String>) -> Option<&RemoteExpr<String>> {
    let RemoteExpr::FunctionCall { id, args, .. } = expr else {
        return None;
    };
    if id.name().as_ref() == "not" && args.len() == 1 {
        return null_predicate_arg(&args[0]);
    }
    matches!(id.name().as_ref(), "is_null" | "is_not_null")
        .then(|| args.first())
        .flatten()
}

fn expr_eq_constant(
    expr: &RemoteExpr<String>,
    maybe_expr: &RemoteExpr<String>,
    maybe_constant: &RemoteExpr<String>,
) -> bool {
    matches!(maybe_constant, RemoteExpr::Constant { .. })
        && expr_matches_filter_arg(expr, maybe_expr)
}

fn expr_matches_filter_arg(expr: &RemoteExpr<String>, maybe_expr: &RemoteExpr<String>) -> bool {
    remote_expr_semantic_eq(maybe_expr, expr)
        || negated_column_matches_expr(expr, maybe_expr)
        || string_prefix_cluster_key_matches_expr(expr, maybe_expr)
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

    remote_expr_semantic_eq(&args[0], filter_expr)
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

fn cluster_key_matches_order_by(
    cluster_key: &RemoteExpr<String>,
    sort_expr: &RemoteExpr<String>,
    asc: bool,
    nulls_first: bool,
    scan: &TableScan,
) -> bool {
    if asc && !nulls_first && remote_expr_semantic_eq(cluster_key, sort_expr) {
        return true;
    }

    !asc && !nulls_first && matches_negated_column(cluster_key, sort_expr, scan)
}

fn matches_negated_column(
    cluster_key: &RemoteExpr<String>,
    sort_expr: &RemoteExpr<String>,
    scan: &TableScan,
) -> bool {
    let RemoteExpr::ColumnRef { data_type, .. } = sort_expr else {
        return false;
    };
    if !negated_column_matches_expr(cluster_key, sort_expr) {
        return false;
    }

    supports_order_reversing_minus(data_type)
        || selected_part_stats_support_order_reversing_minus(sort_expr, scan)
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
        (RemoteExpr::ColumnRef { id: left_id, .. }, RemoteExpr::ColumnRef { id: right_id, .. }) => {
            left_id == right_id
        }
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

fn selected_part_stats_support_order_reversing_minus(
    sort_expr: &RemoteExpr<String>,
    scan: &TableScan,
) -> bool {
    let RemoteExpr::ColumnRef { id, .. } = sort_expr else {
        return false;
    };
    let Ok(field) = scan.source.output_schema.field_with_name(id) else {
        return false;
    };
    let column_id = field.column_id();

    scan.source.parts.partitions.iter().all(|part| {
        FuseBlockPartInfo::from_part(part)
            .ok()
            .and_then(|part| part.columns_stat.as_ref())
            .and_then(|stats| stats.get(&column_id))
            .is_some_and(|stats| scalar_supports_order_reversing_minus(stats.min()))
    })
}

fn scalar_supports_order_reversing_minus(scalar: &Scalar) -> bool {
    match scalar {
        Scalar::Number(NumberScalar::Int64(value)) => value.checked_neg().is_some(),
        Scalar::Decimal(DecimalScalar::Decimal64(value, _)) => value.checked_neg().is_some(),
        Scalar::Decimal(DecimalScalar::Decimal128(value, _)) => value.checked_neg().is_some(),
        Scalar::Decimal(DecimalScalar::Decimal256(value, _)) => value.checked_neg().is_some(),
        _ => false,
    }
}

fn supports_order_reversing_minus(data_type: &DataType) -> bool {
    match data_type.remove_nullable() {
        DataType::Number(
            NumberDataType::UInt8
            | NumberDataType::UInt16
            | NumberDataType::UInt32
            | NumberDataType::Int8
            | NumberDataType::Int16
            | NumberDataType::Int32,
        ) => true,
        // Boundary ordering for these types must be proven from selected block stats.
        DataType::Number(
            NumberDataType::UInt64
            | NumberDataType::Int64
            | NumberDataType::Float32
            | NumberDataType::Float64,
        )
        | DataType::Decimal(_) => false,
        _ => false,
    }
}
