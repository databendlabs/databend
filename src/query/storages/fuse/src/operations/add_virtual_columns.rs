//  Copyright 2023 Datafuse Labs.
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

use std::collections::VecDeque;
use std::sync::Arc;

use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::Projection;
use common_exception::Result;
use common_expression::TableDataType;
use common_expression::TableSchemaRefExt;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::AsyncAccumulatingTransformer;

use crate::io::SegmentsIO;
use crate::operations::merge_into::mutation_meta::mutation_log::BlockMetaIndex;
use crate::operations::mutation::MutationSink;
use crate::operations::mutation::VirtualColumnPartInfo;
use crate::operations::mutation::VirtualColumnSource;
use crate::operations::mutation::VirtualColumnTransform;
use crate::pipelines::Pipeline;
use crate::FuseTable;
use crate::Table;
use crate::TableContext;

impl FuseTable {
    #[async_backtrace::framed]
    pub(crate) async fn do_add_virtual_columns(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let snapshot_opt = self.read_table_snapshot().await?;
        let snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot
            return Ok(());
        };

        let mut field_indices = Vec::new();
        for (field_index, field) in self.table_info.meta.schema.fields().iter().enumerate() {
            if field.data_type().remove_nullable() == TableDataType::Variant {
                field_indices.push(field_index);
            }
        }

        if field_indices.is_empty() {
            // no json columns
            return Ok(());
        }

        // TODO: Analyze query logs.
        // let log_table = ctx.get_table("default", "system", "query_log").await?;

        let mut virtual_column_tasks =
            Partitions::create_nolazy(PartitionsShuffleKind::Mod, vec![]);

        let segment_locations = &snapshot.segments;
        let segments_io = SegmentsIO::create(
            ctx.clone(),
            self.operator.clone(),
            Arc::new(snapshot.schema.clone()),
        );

        let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;
        for (segment_idx, chunk) in segment_locations.chunks(max_io_requests).enumerate() {
            // Read the segments information in parallel.
            let segment_infos = segments_io
                .read_segments(chunk, false)
                .await?
                .into_iter()
                .collect::<Result<Vec<_>>>()?;

            let mut tasks = VecDeque::new();
            let mut block_idx = 0;
            for segment in segment_infos.iter() {
                for block in segment.blocks.iter() {
                    tasks.push_back((block_idx, block.clone()));
                    block_idx += 1;
                }
            }

            let mut partitions = tasks
                .into_iter()
                .map(|(block_idx, block)| {
                    VirtualColumnPartInfo::create(block, BlockMetaIndex {
                        segment_idx,
                        block_idx,
                        range: None,
                    })
                })
                .collect();

            virtual_column_tasks.partitions.append(&mut partitions);
        }

        ctx.set_partitions(virtual_column_tasks)?;

        let source_schema = self.schema();
        let fields = field_indices
            .iter()
            .map(|i| source_schema.field(*i).clone())
            .collect::<Vec<_>>();

        let schema = TableSchemaRefExt::create(fields);
        let write_settings = self.get_write_settings();

        let projection = Projection::Columns(field_indices);
        let block_reader = self.create_block_reader(projection, false, ctx.clone())?;
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        // Add source pipe.
        pipeline.add_source(
            |output| {
                VirtualColumnSource::try_create(
                    ctx.clone(),
                    self.operator.clone(),
                    write_settings.clone(),
                    self.meta_location_generator().clone(),
                    schema.clone(),
                    block_reader.clone(),
                    output,
                )
            },
            max_threads,
        )?;

        pipeline.resize(1)?;

        pipeline.add_transform(|input, output| {
            let transformer = VirtualColumnTransform::new(
                ctx.clone(),
                self.operator.clone(),
                self.meta_location_generator().clone(),
            );

            Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
                input,
                output,
                transformer,
            )))
        })?;

        pipeline.add_sink(|input| {
            MutationSink::try_create(self, ctx.clone(), snapshot.clone(), input)
        })?;

        Ok(())
    }
}
