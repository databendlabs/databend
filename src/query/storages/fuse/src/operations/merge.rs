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

use common_base::base::tokio::sync::Semaphore;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataSchemaRef;
use common_pipeline_core::pipe::PipeItem;
use common_sql::executor::MatchExpr;
use storages_common_table_meta::meta::Location;

use super::merge_into::MatchedAggregator;
use super::mutation::SegmentIndex;
use crate::io::BlockBuilder;
use crate::io::ReadSettings;
use crate::FuseTable;

impl FuseTable {
    // todo: (JackTan25) add pipeline picture
    #[allow(clippy::too_many_arguments)]
    pub fn matched_mutator(
        &self,
        ctx: Arc<dyn TableContext>,
        block_builder: BlockBuilder,
        io_request_semaphore: Arc<Semaphore>,
        row_id_idx: usize,
        matched: MatchExpr,
        input_schema: DataSchemaRef,
        segment_locations: Vec<(SegmentIndex, Location)>,
    ) -> Result<PipeItem> {
        let read_settings = ReadSettings::from_ctx(&ctx)?;
        let aggregator = MatchedAggregator::create(
            ctx.clone(),
            row_id_idx,
            matched,
            self.table_info.schema(),
            input_schema,
            self.get_operator(),
            self.get_write_settings(),
            read_settings,
            block_builder,
            io_request_semaphore,
            segment_locations,
            Arc::new(self.clone()),
        )?;
        Ok(aggregator.into_pipe_item())
    }
}
