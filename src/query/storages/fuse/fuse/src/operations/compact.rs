//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_catalog::plan::Projection;
use common_catalog::table::CompactTarget;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_table_meta::meta::TableSnapshot;

use crate::operations::mutation::BlockCompactMutator;
use crate::operations::mutation::CompactSource;
use crate::operations::mutation::CompactTransform;
use crate::operations::mutation::MergeSegmentsTransform;
use crate::operations::mutation::MutationSink;
use crate::operations::mutation::SegmentCompactMutator;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::Pipe;
use crate::pipelines::Pipeline;
use crate::FuseTable;
use crate::Table;
use crate::TableContext;
use crate::TableMutator;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;

#[derive(Clone)]
pub struct CompactOptions {
    // the snapshot that compactor working on, it never changed during phases compaction.
    pub base_snapshot: Arc<TableSnapshot>,
    pub block_per_seg: usize,
    pub limit: Option<usize>,
}

impl FuseTable {
    pub(crate) async fn do_compact(
        &self,
        ctx: Arc<dyn TableContext>,
        target: CompactTarget,
        limit: Option<usize>,
        pipeline: &mut Pipeline,
    ) -> Result<bool> {
        let snapshot_opt = self.read_table_snapshot().await?;
        let base_snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no compaction.
            return Ok(false);
        };

        if base_snapshot.summary.block_count <= 1 {
            return Ok(false);
        }

        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

        let compact_params = CompactOptions {
            base_snapshot,
            block_per_seg,
            limit,
        };

        match target {
            CompactTarget::Blocks => self.compact_blocks(ctx, pipeline, compact_params).await,
            CompactTarget::Segments => self.compact_segments(ctx, pipeline, compact_params).await,
        }
    }

    async fn compact_segments(
        &self,
        ctx: Arc<dyn TableContext>,
        _pipeline: &mut Pipeline,
        options: CompactOptions,
    ) -> Result<bool> {
        let mut segment_mutator = SegmentCompactMutator::try_create(
            ctx.clone(),
            options,
            self.meta_location_generator().clone(),
            self.operator.clone(),
        )?;

        if !segment_mutator.target_select().await? {
            return Ok(false);
        }

        let mutator = Box::new(segment_mutator);
        mutator.try_commit(Arc::new(self.clone())).await?;

        Ok(true)
    }

    /// The flow of Pipeline is as follows:
    /// +--------------+        +-----------------+
    /// |CompactSource1|  --->  |CompactTransform1|  ------
    /// +--------------+        +-----------------+        |      +----------------------+      +------------+
    /// |    ...       |  ...   |       ...       |  ...   | ---> |MergeSegmentsTransform| ---> |MutationSink|
    /// +--------------+        +-----------------+        |      +----------------------+      +------------+
    /// |CompactSourceN|  --->  |CompactTransformN|  ------
    /// +--------------+        +-----------------+
    async fn compact_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        options: CompactOptions,
    ) -> Result<bool> {
        // skip cluster table.
        if self.cluster_key_meta.is_some() {
            return Ok(false);
        }

        let thresholds = self.get_block_compact_thresholds();

        let mut mutator = BlockCompactMutator::new(ctx.clone(), options, self.operator.clone());
        let need_compact = mutator.target_select().await?;
        if !need_compact {
            return Ok(false);
        }

        ctx.try_set_partitions(mutator.compact_tasks.clone())?;

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        // Add source pipe.
        pipeline.add_source(
            |output| CompactSource::try_create(ctx.clone(), output, thresholds),
            max_threads,
        )?;

        let all_col_ids = self.all_the_columns_ids();
        let projection = Projection::Columns(all_col_ids);
        let block_reader = self.create_block_reader(projection)?;

        pipeline.add_transform(|input, output| {
            CompactTransform::try_create(
                ctx.clone(),
                input,
                output,
                ctx.get_scan_progress(),
                block_reader.clone(),
                self.meta_location_generator().clone(),
                self.operator.clone(),
                thresholds,
            )
        })?;

        self.try_add_merge_segments_transform(mutator.clone(), pipeline)?;
        pipeline.add_sink(|input| {
            MutationSink::try_create(
                self,
                ctx.clone(),
                mutator.compact_params.base_snapshot.clone(),
                input,
            )
        })?;

        Ok(true)
    }

    fn try_add_merge_segments_transform(
        &self,
        mutator: BlockCompactMutator,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        match pipeline.pipes.last() {
            None => Err(ErrorCode::Internal("The pipeline is empty.")),
            Some(pipe) if pipe.output_size() == 0 => {
                Err(ErrorCode::Internal("The output of the last pipe is 0."))
            }
            Some(pipe) => {
                let input_size = pipe.output_size();
                let mut inputs_port = Vec::with_capacity(input_size);
                for _ in 0..input_size {
                    inputs_port.push(InputPort::create());
                }
                let output_port = OutputPort::create();
                let processor = MergeSegmentsTransform::try_create(
                    mutator,
                    inputs_port.clone(),
                    output_port.clone(),
                )?;
                pipeline.pipes.push(Pipe::ResizePipe {
                    inputs_port,
                    outputs_port: vec![output_port],
                    processor,
                });
                Ok(())
            }
        }
    }
}
