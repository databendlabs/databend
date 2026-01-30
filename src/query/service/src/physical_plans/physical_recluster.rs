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
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;

use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::ExtendedTableInfo;
use databend_common_catalog::plan::ReclusterTask;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::SortColumnDescription;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_meta_app::schema::TableInfo;
use databend_common_metrics::storage::metrics_inc_recluster_block_bytes_to_read;
use databend_common_metrics::storage::metrics_inc_recluster_block_nums_to_read;
use databend_common_metrics::storage::metrics_inc_recluster_row_nums_to_read;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::EmptySource;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_pipeline_transforms::build_compact_block_no_split_pipeline;
use databend_common_pipeline_transforms::columns::TransformAddStreamColumns;
use databend_common_sql::StreamContext;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::statistics::ClusterStatsGenerator;
use databend_storages_common_cache::TempDirManager;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::builders::SortPipelineBuilder;
use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::CompactStrategy;
use crate::pipelines::processors::transforms::HilbertPartitionExchange;
use crate::pipelines::processors::transforms::TransformWindowPartitionCollect;
use crate::spillers::SpillerDiskConfig;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Recluster {
    pub meta: PhysicalPlanMeta,
    pub tasks: Vec<ReclusterTask>,
    pub table_info: TableInfo,
    pub table_meta_timestamps: TableMetaTimestamps,
}

#[typetag::serde]
impl IPhysicalPlan for Recluster {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn derive(&self, children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert!(children.is_empty());
        PhysicalPlan::new(Recluster {
            meta: self.meta.clone(),
            tasks: self.tasks.clone(),
            table_info: self.table_info.clone(),
            table_meta_timestamps: self.table_meta_timestamps,
        })
    }

    /// The flow of Pipeline is as follows:
    // ┌──────────┐     ┌───────────────┐     ┌─────────┐
    // │FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┐
    // └──────────┘     └───────────────┘     └─────────┘    │
    // ┌──────────┐     ┌───────────────┐     ┌─────────┐    │     ┌──────────────┐     ┌─────────┐
    // │FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┤────►│MultiSortMerge├────►│Resize(N)├───┐
    // └──────────┘     └───────────────┘     └─────────┘    │     └──────────────┘     └─────────┘   │
    // ┌──────────┐     ┌───────────────┐     ┌─────────┐    │                                        │
    // │FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┘                                        │
    // └──────────┘     └───────────────┘     └─────────┘                                             │
    // ┌──────────────────────────────────────────────────────────────────────────────────────────────┘
    // │         ┌──────────────┐
    // │    ┌───►│SerializeBlock├───┐
    // │    │    └──────────────┘   │
    // │    │    ┌──────────────┐   │    ┌─────────┐    ┌────────────────┐     ┌─────────────┐     ┌──────────┐
    // └───►│───►│SerializeBlock├───┤───►│Resize(1)├───►│SerializeSegment├────►│ReclusterAggr├────►│CommitSink│
    //      │    └──────────────┘   │    └─────────┘    └────────────────┘     └─────────────┘     └──────────┘
    //      │    ┌──────────────┐   │
    //      └───►│SerializeBlock├───┘
    //           └──────────────┘
    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        match self.tasks.len() {
            0 => builder.main_pipeline.add_source(EmptySource::create, 1),
            1 => {
                let table = builder
                    .ctx
                    .build_table_by_table_info(&self.table_info, &None, None)?;
                let table = FuseTable::try_from_table(table.as_ref())?;

                let task = &self.tasks[0];
                let recluster_block_nums = task.parts.len();
                let block_thresholds = table.get_block_thresholds();
                let table_info = table.get_table_info();
                let schema = table.schema_with_stream();
                let description = task.stats.get_description(&table_info.desc);
                let plan = DataSourcePlan {
                    source_info: DataSourceInfo::TableSource(ExtendedTableInfo {
                        inner: table_info.clone(),
                        branch_info: None,
                    }),
                    output_schema: schema.clone(),
                    parts: task.parts.clone(),
                    statistics: task.stats.clone(),
                    description,
                    tbl_args: table.table_args(),
                    push_downs: None,
                    internal_columns: None,
                    base_block_ids: None,
                    update_stream_columns: table.change_tracking_enabled(),
                    table_index: usize::MAX,
                    scan_id: usize::MAX,
                };

                {
                    metrics_inc_recluster_block_nums_to_read(recluster_block_nums as u64);
                    metrics_inc_recluster_block_bytes_to_read(task.total_bytes as u64);
                    metrics_inc_recluster_row_nums_to_read(task.total_rows as u64);

                    log::info!(
                        "Number of blocks scheduled for recluster: {}",
                        recluster_block_nums
                    );
                }

                builder.ctx.set_partitions(plan.parts.clone())?;

                table.read_data(
                    builder.ctx.clone(),
                    &plan,
                    &mut builder.main_pipeline,
                    false,
                )?;

                let num_input_columns = schema.fields().len();
                if table.change_tracking_enabled() {
                    let stream_ctx = StreamContext::try_create(
                        builder.ctx.get_function_context()?,
                        schema,
                        table_info.ident.seq,
                        false,
                        false,
                    )?;

                    builder
                        .main_pipeline
                        .add_transformer(|| TransformAddStreamColumns::new(stream_ctx.clone()));
                }

                let cluster_stats_gen = table.get_cluster_stats_gen(
                    builder.ctx.clone(),
                    task.level + 1,
                    block_thresholds,
                    None,
                )?;
                let operators = cluster_stats_gen.operators.clone();
                if !operators.is_empty() {
                    let func_ctx2 = cluster_stats_gen.func_ctx.clone();
                    builder.main_pipeline.add_transformer(move || {
                        CompoundBlockOperator::new(
                            operators.clone(),
                            func_ctx2.clone(),
                            num_input_columns,
                        )
                    });
                }

                // construct output fields
                let output_fields = cluster_stats_gen.out_fields.clone();
                let schema = DataSchemaRefExt::create(output_fields);
                let sort_descs: Vec<_> = cluster_stats_gen
                    .cluster_key_index
                    .iter()
                    .map(|offset| SortColumnDescription {
                        offset: *offset,
                        asc: true,
                        nulls_first: false,
                    })
                    .collect();

                // merge sort
                let sort_block_size = block_thresholds.calc_rows_for_recluster(
                    task.total_rows,
                    task.total_bytes,
                    task.total_compressed,
                );

                let settings = builder.ctx.get_settings();
                let sort_pipeline_builder = SortPipelineBuilder::create(
                    builder.ctx.clone(),
                    schema,
                    sort_descs.into(),
                    None,
                    settings.get_enable_fixed_rows_sort()?,
                )?
                .with_block_size_hit(sort_block_size)
                .remove_order_col_at_last();
                // Todo(zhyass): Recluster will no longer perform sort in the near future.
                sort_pipeline_builder.build_full_sort_pipeline(&mut builder.main_pipeline)?;

                // Compact after merge sort.
                let max_threads = settings.get_max_threads()? as usize;
                build_compact_block_no_split_pipeline(
                    &mut builder.main_pipeline,
                    block_thresholds,
                    max_threads,
                )?;

                builder.main_pipeline.add_transform(
                    |transform_input_port, transform_output_port| {
                        let proc = TransformSerializeBlock::try_create(
                            builder.ctx.clone(),
                            transform_input_port,
                            transform_output_port,
                            table,
                            cluster_stats_gen.clone(),
                            MutationKind::Recluster,
                            self.table_meta_timestamps,
                        )?;
                        proc.into_processor()
                    },
                )
            }
            _ => Err(ErrorCode::Internal(
                "A node can only execute one recluster task".to_string(),
            )),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct HilbertPartition {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub table_info: TableInfo,
    pub num_partitions: usize,
    pub table_meta_timestamps: TableMetaTimestamps,
    pub rows_per_block: usize,
}

#[typetag::serde]
impl IPhysicalPlan for HilbertPartition {
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
        Ok(DataSchemaRef::default())
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(HilbertPartition {
            meta: self.meta.clone(),
            input,
            table_info: self.table_info.clone(),
            num_partitions: self.num_partitions,
            table_meta_timestamps: self.table_meta_timestamps,
            rows_per_block: self.rows_per_block,
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let num_processors = builder.main_pipeline.output_len();
        let table = builder
            .ctx
            .build_table_by_table_info(&self.table_info, &None, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;

        builder.main_pipeline.exchange(
            num_processors,
            HilbertPartitionExchange::create(self.num_partitions),
        )?;

        let settings = builder.settings.clone();
        let temp_dir_manager = TempDirManager::instance();

        let disk_bytes_limit = GlobalConfig::instance()
            .spill
            .window_partition_spill_bytes_limit();

        let enable_dio = settings.get_enable_dio()?;
        let disk_spill = temp_dir_manager
            .get_disk_spill_dir(disk_bytes_limit, &builder.ctx.get_id())
            .map(|temp_dir| SpillerDiskConfig::new(temp_dir, enable_dio))
            .transpose()?;

        let window_spill_settings = MemorySettings::from_window_settings(&builder.ctx)?;
        let processor_id = AtomicUsize::new(0);
        let max_bytes_per_block = std::cmp::min(
            4 * table.get_option(
                FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD,
                DEFAULT_BLOCK_BUFFER_SIZE,
            ),
            400 * 1024 * 1024,
        );
        builder.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(Box::new(
                TransformWindowPartitionCollect::new(
                    builder.ctx.clone(),
                    input,
                    output,
                    &settings,
                    processor_id.fetch_add(1, atomic::Ordering::AcqRel),
                    num_processors,
                    self.num_partitions,
                    window_spill_settings.clone(),
                    disk_spill.clone(),
                    CompactStrategy::new(self.rows_per_block, max_bytes_per_block),
                )?,
            )))
        })?;

        builder
            .main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                let proc = TransformSerializeBlock::try_create(
                    builder.ctx.clone(),
                    transform_input_port,
                    transform_output_port,
                    table,
                    ClusterStatsGenerator::default(),
                    MutationKind::Recluster,
                    self.table_meta_timestamps,
                )?;
                proc.into_processor()
            })
    }
}
