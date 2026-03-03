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

use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::LimitType;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::sorts::TransformSortPartial;
use databend_common_pipeline_transforms::sorts::add_k_way_merge_sort;
use databend_common_pipeline_transforms::sorts::core::SortKeyDescription;
use databend_common_pipeline_transforms::sorts::try_add_multi_sort_merge;
use databend_common_storage::DataOperator;
use databend_common_storages_fuse::TableContext;
use databend_storages_common_cache::TempDirManager;

use crate::sessions::QueryContext;
use crate::spillers::SortSpillerImpl;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerDiskConfig;
use crate::spillers::SpillerType;

type TransformSortBuilder =
    crate::pipelines::processors::transforms::TransformSortBuilder<SortSpillerImpl>;

pub struct SortPipelineBuilder {
    ctx: Arc<QueryContext>,
    key_desc: SortKeyDescription,
    limit: Option<usize>,
    block_size: usize,
    enable_loser_tree: bool,
    broadcast_id: Option<u32>,
    enable_fixed_rows: bool,
    enable_sort_spill_prefetch: bool,
}

impl SortPipelineBuilder {
    pub fn create(
        ctx: Arc<QueryContext>,
        schema: DataSchemaRef,
        sort_desc: Arc<[SortColumnDescription]>,
        broadcast_id: Option<u32>,
        enable_fixed_rows: bool,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let block_size = settings.get_max_block_size()? as usize;
        let enable_loser_tree = settings.get_enable_loser_tree_merge_sort()?;
        let enable_sort_spill_prefetch = settings.get_enable_sort_spill_prefetch()?;
        Ok(Self {
            ctx,
            key_desc: SortKeyDescription::new(sort_desc, schema, enable_fixed_rows)?,
            limit: None,
            block_size,
            enable_loser_tree,
            broadcast_id,
            enable_fixed_rows,
            enable_sort_spill_prefetch,
        })
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    // The expected output block size, the actual output block size will be equal to or less than the given value.
    pub fn with_block_size_hit(mut self, block_size: usize) -> Self {
        self.block_size = self.block_size.min(block_size);
        self
    }

    pub fn build_full_sort_pipeline(
        self,
        pipeline: &mut Pipeline,
        output_order_col: bool,
    ) -> Result<()> {
        // Partial sort
        pipeline.add_transformer(|| {
            TransformSortPartial::new(
                LimitType::from_limit_rows(self.limit),
                self.key_desc.sort_column_desc(),
            )
        });

        self.build_merge_sort_pipeline(pipeline, false, output_order_col)
    }

    fn build_merge_sort(
        &self,
        pipeline: &mut Pipeline,
        order_col_generated: bool,
        output_order_col: bool,
    ) -> Result<()> {
        debug_assert!(
            // If `order_col_generated`, it means this transform is the last processor in the distributed sort pipeline.
            !order_col_generated || !output_order_col
        );

        let settings = self.ctx.get_settings();
        let enable_loser_tree = settings.get_enable_loser_tree_merge_sort()?;

        let spiller = {
            let temp_dir_manager = TempDirManager::instance();
            let disk_bytes_limit = GlobalConfig::instance().spill.sort_spill_bytes_limit();
            let enable_dio = settings.get_enable_dio()?;
            let disk_spill = temp_dir_manager
                .get_disk_spill_dir(disk_bytes_limit, &self.ctx.get_id())
                .map(|temp_dir| SpillerDiskConfig::new(temp_dir, enable_dio))
                .transpose()?;

            let location_prefix = self.ctx.query_id_spill_prefix();
            let config = SpillerConfig {
                spiller_type: SpillerType::OrderBy,
                location_prefix,
                disk_spill,
                use_parquet: settings.get_spilling_file_format()?.is_parquet(),
            };
            let op = DataOperator::instance().spill_operator();
            SortSpillerImpl::new(self.ctx.clone(), op, config)?
        };

        pipeline.add_transform(|input, output| {
            let builder = TransformSortBuilder::new(
                self.key_desc.clone(),
                self.block_size,
                self.enable_fixed_rows,
            )
            .with_spiller(spiller.clone())
            .with_limit(self.limit)
            .with_order_column(order_col_generated, output_order_col)
            .with_enable_restore_prefetch(self.enable_sort_spill_prefetch)
            .with_enable_loser_tree(enable_loser_tree);

            Ok(ProcessorPtr::create(builder.build(input, output)?))
        })
    }

    pub fn build_merge_sort_pipeline(
        self,
        pipeline: &mut Pipeline,
        order_col_generated: bool,
        output_order_col: bool,
    ) -> Result<()> {
        let need_multi_merge = pipeline.output_len() > 1;
        let local_output_order_col = need_multi_merge || output_order_col;
        self.build_merge_sort(pipeline, order_col_generated, local_output_order_col)?;

        if !need_multi_merge {
            return Ok(());
        }

        self.build_multi_merge(pipeline, !output_order_col)
    }

    pub fn build_multi_merge(self, pipeline: &mut Pipeline, remove_order_col: bool) -> Result<()> {
        // Multi-pipelines merge sort
        let settings = self.ctx.get_settings();
        if settings.get_enable_parallel_multi_merge_sort()? {
            let max_threads = settings.get_max_threads()? as usize;
            add_k_way_merge_sort(
                pipeline,
                self.key_desc.clone(),
                max_threads,
                self.block_size,
                self.limit,
                remove_order_col,
                self.enable_loser_tree,
                self.enable_fixed_rows,
            )
        } else {
            try_add_multi_sort_merge(
                pipeline,
                self.key_desc.clone(),
                self.block_size,
                self.limit,
                remove_order_col,
                self.enable_loser_tree,
                self.enable_fixed_rows,
            )
        }
    }

    pub fn build_sample(self, pipeline: &mut Pipeline) -> Result<()> {
        let settings = self.ctx.get_settings();
        let max_block_size = settings.get_max_block_size()? as usize;

        // Partial sort
        pipeline.add_transformer(|| {
            TransformSortPartial::new(
                LimitType::from_limit_rows(self.limit),
                self.key_desc.sort_column_desc(),
            )
        });

        let spiller = {
            let location_prefix = self.ctx.query_id_spill_prefix();
            let config = SpillerConfig {
                spiller_type: SpillerType::OrderBy,
                location_prefix,
                disk_spill: None,
                use_parquet: settings.get_spilling_file_format()?.is_parquet(),
            };
            let op = DataOperator::instance().spill_operator();
            SortSpillerImpl::new(self.ctx.clone(), op, config)?
        };

        let enable_loser_tree = settings.get_enable_loser_tree_merge_sort()?;

        let builder = TransformSortBuilder::new(
            self.key_desc.clone(),
            max_block_size,
            self.enable_fixed_rows,
        )
        .with_spiller(spiller)
        .with_limit(self.limit)
        .with_order_column(false, true)
        .with_enable_restore_prefetch(self.enable_sort_spill_prefetch)
        .with_enable_loser_tree(enable_loser_tree);

        let default_num_merge = settings.get_max_threads()?.max(2);
        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(builder.build_collect(
                input,
                output,
                default_num_merge as _,
            )?))
        })?;

        builder.add_bound_broadcast(
            pipeline,
            max_block_size,
            self.ctx.clone(),
            self.broadcast_id.unwrap(),
        )?;

        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(builder.build_restore(input, output)?))
        })?;

        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(
                builder.build_bound_edge(input, output)?,
            ))
        })?;

        Ok(())
    }

    pub fn build_bounded_merge_sort(self, pipeline: &mut Pipeline) -> Result<()> {
        let builder = TransformSortBuilder::new(
            self.key_desc.clone(),
            self.block_size,
            self.enable_fixed_rows,
        )
        .with_limit(self.limit)
        .with_order_column(true, false)
        .with_enable_restore_prefetch(self.enable_sort_spill_prefetch)
        .with_enable_loser_tree(self.enable_loser_tree);

        let inputs_port: Vec<_> = (0..pipeline.output_len())
            .map(|_| InputPort::create())
            .collect();
        let output_port = OutputPort::create();

        let processor = ProcessorPtr::create(
            builder.build_bounded_merge_sort(inputs_port.clone(), output_port.clone())?,
        );

        pipeline.add_pipe(Pipe::create(inputs_port.len(), 1, vec![PipeItem::create(
            processor,
            inputs_port,
            vec![output_port],
        )]));
        Ok(())
    }
}
