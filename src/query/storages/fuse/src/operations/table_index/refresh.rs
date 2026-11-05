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

//! Build the shared refresh pipeline with one metadata publication.

use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::Transformer;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::Statistics;
use parking_lot::Mutex;

use crate::FuseTable;
use crate::operations::CommitSink;
use crate::operations::MutationGenerator;
use crate::operations::TableMutationAggregator;
use crate::operations::table_index::meta::RefreshPlan;
use crate::operations::table_index::plan::collect_refresh_plan;
use crate::operations::table_index::processors::AssembleIndexes;
use crate::operations::table_index::processors::BuildIndex;
use crate::operations::table_index::processors::JoinIndexes;
use crate::operations::table_index::processors::ReadRefreshBlock;
use crate::operations::table_index::processors::WriteIndexArtifacts;

impl FuseTable {
    /// One snapshot, one read of each selected block, and one publish for all
    /// selected indexes. `Some([])` is deliberately not a full-table refresh.
    pub async fn do_refresh_indexes(
        &self,
        ctx: Arc<dyn TableContext>,
        index_names: &[String],
        segment_locs: Option<Vec<Location>>,
        pipeline: &mut Pipeline,
    ) -> Result<u64> {
        self.check_mutable()?;
        if index_names.is_empty() || segment_locs.as_ref().is_some_and(Vec::is_empty) {
            return Ok(0);
        }
        let Some(snapshot) = self.read_table_snapshot().await? else {
            return Ok(0);
        };
        let RefreshPlan { specs, blocks } =
            collect_refresh_plan(self, ctx.clone(), &snapshot, index_names, segment_locs).await?;
        if blocks.is_empty() {
            return Ok(0);
        }
        let operator = self.get_operator();
        let count = blocks.len();
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let lanes = count.min(max_threads);
        let workers = specs
            .iter()
            .map(|spec| {
                blocks
                    .iter()
                    .filter(|block| block.missing.contains(&spec.index.name))
                    .count()
                    .min(max_threads)
            })
            .sum();
        let queue = Arc::new(Mutex::new(blocks));
        pipeline.add_source(
            |output| {
                AsyncSourcer::create(
                    ctx.get_scan_progress(),
                    output,
                    ReadRefreshBlock::new(
                        queue.clone(),
                        ReadSettings::from_ctx(&ctx)?,
                        self.get_write_settings().storage_format,
                    ),
                )
            },
            lanes,
        )?;
        pipeline.duplicate(true, specs.len())?;
        let mut builders = Vec::with_capacity(lanes * specs.len());
        for _ in 0..lanes {
            for spec in &specs {
                let input = InputPort::create();
                let output = OutputPort::create();
                builders.push(PipeItem::create(
                    ProcessorPtr::create(Transformer::create(
                        input.clone(),
                        output.clone(),
                        BuildIndex::new(
                            ctx.clone(),
                            spec.clone(),
                            operator.clone(),
                            self.meta_location_generator().clone(),
                        ),
                    )),
                    vec![input],
                    vec![output],
                ));
            }
        }
        pipeline.add_pipe(Pipe::create(
            lanes * specs.len(),
            lanes * specs.len(),
            builders,
        ));
        // Upload independent bundles before fan-in, so a slow index does not
        // serialize other indexes' uploads or retain their serialized buffers.
        pipeline.add_async_transformer(|| WriteIndexArtifacts::new(operator.clone()));
        // Keep the formerly independent per-index budgets, before fan-in narrows the graph.
        pipeline.set_max_threads(workers);
        let items = (0..lanes)
            .map(|_| {
                let inputs = (0..specs.len())
                    .map(|_| InputPort::create())
                    .collect::<Vec<_>>();
                let output = OutputPort::create();
                PipeItem::create(
                    ProcessorPtr::create(Box::new(JoinIndexes::new(
                        inputs.clone(),
                        output.clone(),
                    ))),
                    inputs,
                    vec![output],
                )
            })
            .collect();
        pipeline.add_pipe(Pipe::create(lanes * specs.len(), lanes, items));
        let active_vector_versions = self
            .get_table_info()
            .meta
            .indexes
            .iter()
            .filter(|(_, index)| index.index_type == TableIndexType::Vector)
            .map(|(name, index)| (name.clone(), index.version.clone()))
            .collect();
        let assembler = AssembleIndexes::new(
            ctx.clone(),
            operator,
            self.meta_location_generator().clone(),
            active_vector_versions,
        );
        pipeline.add_async_transformer(|| assembler.clone());
        pipeline.try_resize(1)?;
        let timestamps = ctx.get_table_meta_timestamps(self, Some(snapshot.clone()))?;
        pipeline.add_async_accumulating_transformer(|| {
            TableMutationAggregator::create(
                self,
                ctx.clone(),
                snapshot.segments.clone(),
                Default::default(),
                vec![],
                Statistics::default(),
                MutationKind::Refresh,
                timestamps,
            )
        });
        let previous = snapshot.snapshot_id;
        let generator = MutationGenerator::new(Some(snapshot), MutationKind::Refresh);
        pipeline.add_sink(|input| {
            CommitSink::try_create(
                self,
                ctx.clone(),
                None,
                Default::default(),
                generator.clone(),
                input,
                None,
                Some(previous),
                None,
                timestamps,
                false,
                None,
            )
        })?;
        Ok(count as u64)
    }
}
