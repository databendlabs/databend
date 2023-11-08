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

use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_sources::EmptySource;
use common_sql::executor::ReclusterSink;
use common_sql::executor::ReclusterSource;
use common_storages_fuse::FuseTable;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_recluster_source(
        &mut self,
        recluster_source: &ReclusterSource,
    ) -> Result<()> {
        match recluster_source.tasks.len() {
            0 => self.main_pipeline.add_source(EmptySource::create, 1),
            1 => {
                let table = self.ctx.build_table_by_table_info(
                    &recluster_source.catalog_info,
                    &recluster_source.table_info,
                    None,
                )?;
                let table = FuseTable::try_from_table(table.as_ref())?;

                table.build_recluster_source(
                    self.ctx.clone(),
                    recluster_source.tasks[0].clone(),
                    recluster_source.catalog_info.clone(),
                    &mut self.main_pipeline,
                )
            }
            _ => Err(ErrorCode::Internal(
                "A node can only execute one recluster task".to_string(),
            )),
        }
    }

    pub(crate) fn build_recluster_sink(&mut self, recluster_sink: &ReclusterSink) -> Result<()> {
        self.build_pipeline(&recluster_sink.input)?;

        let table = self.ctx.build_table_by_table_info(
            &recluster_sink.catalog_info,
            &recluster_sink.table_info,
            None,
        )?;
        let table = FuseTable::try_from_table(table.as_ref())?;

        table.build_recluster_sink(self.ctx.clone(), recluster_sink, &mut self.main_pipeline)
    }
}
