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

use std::sync::atomic;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::SortColumnDescription;
use databend_common_metrics::storage::metrics_inc_recluster_block_bytes_to_read;
use databend_common_metrics::storage::metrics_inc_recluster_block_nums_to_read;
use databend_common_metrics::storage::metrics_inc_recluster_row_nums_to_read;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::EmptySource;
use databend_common_pipeline_transforms::processors::build_compact_block_no_split_pipeline;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_pipeline_transforms::sort::utils::add_order_field;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::physical_plans::Recluster;
use databend_common_sql::StreamContext;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::io::StreamBlockProperties;
use databend_common_storages_fuse::operations::TransformBlockBuilder;
use databend_common_storages_fuse::operations::TransformBlockWriter;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;

use crate::pipelines::builders::SortPipelineBuilder;
use crate::pipelines::processors::transforms::ReclusterPartitionExchange;
use crate::pipelines::processors::transforms::ReclusterPartitionStrategys;
use crate::pipelines::processors::transforms::SampleState;
use crate::pipelines::processors::transforms::TransformAddOrderColumn;
use crate::pipelines::processors::transforms::TransformAddStreamColumns;
use crate::pipelines::processors::transforms::TransformPartitionCollect;
use crate::pipelines::processors::transforms::TransformRangePartitionIndexer;
use crate::pipelines::processors::transforms::TransformReclusterCollect;
use crate::pipelines::processors::transforms::TransformReclusterPartition;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
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
    pub(crate) fn build_recluster(&mut self, recluster: &Recluster) -> Result<()> {
        match recluster.tasks.len() {
            0 => self.main_pipeline.add_source(EmptySource::create, 1),
            1 => {
                let table = self
                    .ctx
                    .build_table_by_table_info(&recluster.table_info, None)?;
                let table = FuseTable::try_from_table(table.as_ref())?;

                let task = &recluster.tasks[0];
                let recluster_block_nums = task.parts.len();
                let block_thresholds = table.get_block_thresholds();
                let table_info = table.get_table_info();
                let schema = Arc::new(table.schema_with_stream().remove_virtual_computed_fields());
                let description = task.stats.get_description(&table_info.desc);
                let plan = DataSourcePlan {
                    source_info: DataSourceInfo::TableSource(table_info.clone()),
                    output_schema: schema.clone(),
                    parts: task.parts.clone(),
                    statistics: task.stats.clone(),
                    description,
                    tbl_args: table.table_args(),
                    push_downs: None,
                    internal_columns: None,
                    base_block_ids: None,
                    update_stream_columns: table.change_tracking_enabled(),
                    data_mask_policy: None,
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

                self.ctx.set_partitions(plan.parts.clone())?;

                table.read_data(self.ctx.clone(), &plan, &mut self.main_pipeline, false)?;

                let num_input_columns = schema.fields().len();
                if table.change_tracking_enabled() {
                    let stream_ctx = StreamContext::try_create(
                        self.ctx.get_function_context()?,
                        schema,
                        table_info.ident.seq,
                        false,
                        false,
                    )?;
                    self.main_pipeline
                        .add_transformer(|| TransformAddStreamColumns::new(stream_ctx.clone()));
                }

                let level = task.level + 1;
                let enable_stream_writer =
                    self.ctx.get_settings().get_enable_block_stream_write()?
                        && table.storage_format_as_parquet();
                if enable_stream_writer {
                    let properties = StreamBlockProperties::try_create(
                        self.ctx.clone(),
                        table,
                        MutationKind::Recluster,
                        Some(level),
                        recluster.table_meta_timestamps,
                    )?;
                    let operators = properties.cluster_operators();
                    if !operators.is_empty() {
                        let func_ctx = self.ctx.get_function_context()?;
                        self.main_pipeline.add_transformer(move || {
                            CompoundBlockOperator::new(
                                operators.clone(),
                                func_ctx.clone(),
                                num_input_columns,
                            )
                        });
                    }

                    let sort_desc: Vec<_> = properties
                        .cluster_key_index()
                        .iter()
                        .map(|&offset| SortColumnDescription {
                            offset,
                            asc: true,
                            nulls_first: false,
                        })
                        .collect();
                    let fields_with_cluster_key = properties.fields_with_cluster_key();
                    let schema = DataSchemaRefExt::create(fields_with_cluster_key);
                    let schema = add_order_field(schema, &sort_desc);
                    let order_offset = schema.fields.len() - 1;

                    let num_processors = self.main_pipeline.output_len();
                    let sample_size = self
                        .ctx
                        .get_settings()
                        .get_recluster_sample_size_per_block()?
                        as usize;
                    let partitions = block_thresholds.calc_partitions_for_recluster(
                        task.total_rows,
                        task.total_bytes,
                        task.total_compressed,
                    );
                    let state = SampleState::new(num_processors, partitions);
                    let recluster_pipeline_builder = ReclusterPipelineBuilder::create(
                        schema.clone(),
                        sort_desc.clone(),
                        sample_size,
                    )
                    .with_state(state);
                    recluster_pipeline_builder
                        .build_recluster_sample_pipeline(&mut self.main_pipeline)?;

                    self.main_pipeline.exchange(
                        num_processors,
                        ReclusterPartitionExchange::create(0, partitions),
                    );
                    let processor_id = AtomicUsize::new(0);

                    let settings = self.ctx.get_settings();
                    let enable_writings = settings.get_enable_block_stream_writes()?;
                    if enable_writings {
                        let memory_settings = MemorySettings::disable_spill();
                        self.main_pipeline.add_transform(|input, output| {
                            let strategy =
                                ReclusterPartitionStrategys::new(properties.clone(), order_offset);

                            Ok(ProcessorPtr::create(Box::new(
                                TransformPartitionCollect::new(
                                    self.ctx.clone(),
                                    input,
                                    output,
                                    &settings,
                                    processor_id.fetch_add(1, atomic::Ordering::AcqRel),
                                    num_processors,
                                    partitions,
                                    memory_settings.clone(),
                                    None,
                                    strategy,
                                )?,
                            )))
                        })?;

                        self.main_pipeline.add_transform(|input, output| {
                            TransformBlockBuilder::try_create(input, output, properties.clone())
                        })?;
                    } else {
                        self.main_pipeline.add_transform(|input, output| {
                            TransformReclusterPartition::try_create(
                                input,
                                output,
                                properties.clone(),
                                processor_id.fetch_add(1, atomic::Ordering::AcqRel),
                                num_processors,
                                partitions,
                            )
                        })?;
                    }

                    self.main_pipeline.add_async_accumulating_transformer(|| {
                        TransformBlockWriter::create(
                            self.ctx.clone(),
                            MutationKind::Recluster,
                            table,
                            false,
                        )
                    });
                    Ok(())
                } else {
                    let cluster_stats_gen = table.get_cluster_stats_gen(
                        self.ctx.clone(),
                        level,
                        block_thresholds,
                        None,
                    )?;
                    let operators = cluster_stats_gen.operators.clone();
                    if !operators.is_empty() {
                        let func_ctx2 = cluster_stats_gen.func_ctx.clone();
                        self.main_pipeline.add_transformer(move || {
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
                    let sort_desc: Vec<_> = cluster_stats_gen
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

                    let sort_pipeline_builder =
                        SortPipelineBuilder::create(self.ctx.clone(), schema, sort_desc.into())?
                            .with_block_size_hit(sort_block_size)
                            .remove_order_col_at_last();
                    sort_pipeline_builder.build_full_sort_pipeline(&mut self.main_pipeline)?;

                    // Compact after merge sort.
                    let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
                    build_compact_block_no_split_pipeline(
                        &mut self.main_pipeline,
                        block_thresholds,
                        max_threads,
                    )?;

                    self.main_pipeline.add_transform(
                        |transform_input_port, transform_output_port| {
                            let proc = TransformSerializeBlock::try_create(
                                self.ctx.clone(),
                                transform_input_port,
                                transform_output_port,
                                table,
                                cluster_stats_gen.clone(),
                                MutationKind::Recluster,
                                recluster.table_meta_timestamps,
                            )?;
                            proc.into_processor()
                        },
                    )
                }
            }
            _ => Err(ErrorCode::Internal(
                "A node can only execute one recluster task".to_string(),
            )),
        }
    }
}

struct ReclusterPipelineBuilder {
    schema: DataSchemaRef,
    sort_desc: Vec<SortColumnDescription>,
    state: Option<Arc<SampleState>>,
    sample_size: usize,
    seed: u64,
}

impl ReclusterPipelineBuilder {
    fn create(
        schema: DataSchemaRef,
        sort_desc: Vec<SortColumnDescription>,
        sample_size: usize,
    ) -> Self {
        Self {
            schema,
            sort_desc,
            state: None,
            sample_size,
            seed: rand::random(),
        }
    }

    #[allow(unused)]
    fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    fn with_state(mut self, state: Arc<SampleState>) -> Self {
        self.state = Some(state);
        self
    }

    fn build_recluster_sample_pipeline(&self, pipeline: &mut Pipeline) -> Result<()> {
        pipeline.try_add_transformer(|| {
            TransformAddOrderColumn::try_new(self.sort_desc.clone(), self.schema.clone())
        })?;
        let offset = self.schema.num_fields() - 1;
        pipeline.add_accumulating_transformer(|| {
            TransformReclusterCollect::new(offset, self.sample_size, self.seed)
        });
        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(
                TransformRangePartitionIndexer::create(input, output, self.state.clone().unwrap()),
            ))
        })
    }
}
