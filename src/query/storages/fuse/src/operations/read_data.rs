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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::sources::EmptySource;

use crate::FuseTable;
use crate::io::BlockReader;
use crate::io::VirtualColumnReader;
use crate::operations::read::build_fuse_source_pipeline;
use crate::operations::read::fuse_source::build_fuse_partitions_source_pipeline;
use crate::operations::read::fuse_source::build_fuse_read_transform_pipeline;

type FuseDataReaders = (Arc<BlockReader>, Arc<Option<VirtualColumnReader>>);

impl FuseTable {
    pub fn create_block_reader(
        &self,
        ctx: Arc<dyn TableContext>,
        projection: Projection,
        put_cache: bool,
    ) -> Result<Arc<BlockReader>> {
        let table_schema = self.schema_with_stream();
        BlockReader::create(
            ctx,
            self.operator.clone(),
            table_schema,
            projection,
            put_cache,
        )
    }

    // Build the block reader.
    pub fn build_block_reader(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        put_cache: bool,
    ) -> Result<Arc<BlockReader>> {
        self.create_block_reader(
            ctx,
            PushDownInfo::projection_of_push_downs(
                &self.schema_with_stream(),
                plan.push_downs.as_ref(),
            ),
            put_cache,
        )
    }

    pub(crate) fn adjust_io_request(&self, ctx: &Arc<dyn TableContext>) -> Result<usize> {
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;

        Ok(std::cmp::max(max_threads, max_io_requests))
    }

    fn build_data_readers(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        put_cache: bool,
    ) -> Result<FuseDataReaders> {
        let block_reader = self.build_block_reader(ctx.clone(), plan, put_cache)?;
        let virtual_reader = Arc::new(
            PushDownInfo::virtual_columns_of_push_downs(&plan.push_downs)
                .as_ref()
                .map(|virtual_column| {
                    VirtualColumnReader::try_create(
                        ctx,
                        self.operator.clone(),
                        block_reader.schema(),
                        virtual_column.clone(),
                        self.table_compression,
                    )
                })
                .transpose()?,
        );

        Ok((block_reader, virtual_reader))
    }

    /// Build a source that only emits the block partitions produced by the pruning pipeline.
    pub fn do_read_pruned_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        self.check_format_supported()?;
        let Some(receiver) = self.pruned_result_receiver.lock().take() else {
            return match plan.parts.is_empty() {
                true => pipeline.add_source(EmptySource::create, 1),
                false => Err(ErrorCode::Internal(
                    "Distributed Fuse prune operator did not produce a partition receiver",
                )),
            };
        };

        let batch_size = ctx.get_settings().get_storage_fetch_part_num()? as usize;
        // Metadata delivery is cheap and a single receiver preserves useful batching. Block reads
        // are expanded back to the configured IO parallelism after the exchange.
        build_fuse_partitions_source_pipeline(ctx, pipeline, plan, 1, Some(receiver), batch_size)
    }

    /// Attach block reading to a pipeline that already emits `BlockPartitionMeta`.
    pub fn do_read_data_from_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        put_cache: bool,
    ) -> Result<()> {
        self.check_format_supported()?;
        let (block_reader, virtual_reader) =
            self.build_data_readers(ctx.clone(), plan, put_cache)?;
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_io_requests = self.adjust_io_request(&ctx)?;

        build_fuse_read_transform_pipeline(
            ctx,
            self.storage_format,
            self.schema_with_stream(),
            pipeline,
            block_reader,
            max_threads,
            max_io_requests,
            max_io_requests,
            plan,
            virtual_reader,
            true,
        )
    }

    #[inline]
    pub fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        put_cache: bool,
    ) -> Result<()> {
        let (block_reader, virtual_reader) =
            self.build_data_readers(ctx.clone(), plan, put_cache)?;
        let max_io_requests = self.adjust_io_request(&ctx)?;

        // Lazy parts are pruned by `build_prune_pipeline`, which runs before
        // `read_data` and leaves the pruned parts in `pruned_result_receiver`.
        let rx = self.pruned_result_receiver.lock().take();

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let table_schema = self.schema_with_stream();
        build_fuse_source_pipeline(
            ctx.clone(),
            self.storage_format,
            table_schema,
            pipeline,
            block_reader,
            max_threads,
            plan,
            max_io_requests,
            virtual_reader,
            rx,
        )?;

        Ok(())
    }
}
