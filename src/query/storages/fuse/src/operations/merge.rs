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

use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_pipeline_core::PipeItem;
use databend_storages_common_table_meta::meta::Location;

use super::merge_into::MatchedAggregator;
use super::mutation::SegmentIndex;
use crate::io::BlockBuilder;
use crate::io::ReadSettings;
use crate::FuseTable;

impl FuseTable {
    // todo: (JackTan25) add pipeline picture
    // The big picture of the merge into pipeline:
    //
    //                                                                                                                                                +-------------------+
    //                                                                                         +-----------------------------+    output_port_row_id  |                   |
    //                                            +-----------------------+     Matched        |                             +------------------------>-ResizeProcessor(1)+---------------+
    //                                            |                       +---+--------------->|    MatchedSplitProcessor    |                        |                   |               |
    //                                            |                       |   |                |                             +----------+             +-------------------+               |
    //         +----------------------+           |                       +---+                +-----------------------------+          |                                                 |
    //         |   MergeIntoSource    +---------->|MergeIntoSplitProcessor|                                                       output_port_updated                                     |
    //         +----------------------+           |                       +---+                +-----------------------------+          |             +-------------------+               |
    //                                            |                       |   | NotMatched     |                             |          |             |                   |               |
    //                                            |                       +---+--------------->| MergeIntoNotMatchedProcessor+----------+------------->-ResizeProcessor(1)+-----------+   |
    //                                            +-----------------------+                    |                             |                        |                   |           |   |
    //                                                                                         +-----------------------------+                        +-------------------+           |   |
    //                                                                                                                                                                                |   |
    //                                                                                                                                                                                |   |
    //                                                                                                                                                                                |   |
    //                                                                                                                                                                                |   |
    //                                                                                                                                                                                |   |
    //                                                                               +-------------------------------------------------+                                              |   |
    //                                                                               |                                                 |                                              |   |
    //                                                                               |                                                 |                                              |   |
    //           +--------------------------+        +-------------------------+     |         ++---------------------------+          |     +--------------------------------------+ |   |
    // +---------+ TransformSerializeSegment<--------+ TransformSerializeBlock <-----+---------+|TransformAddComputedColumns|<---------+-----+TransformResortAddOnWithoutSourceSchema<-+   |
    // |         +--------------------------+        +-------------------------+     |         ++---------------------------+          |     +--------------------------------------+     |
    // |                                                                             |                                                 |                                                  |
    // |                                                                             |                                                 |                                                  |
    // |                                                                             |                                                 |                                                  |
    // |                                                                             |                                                 |                                                  |
    // |          +---------------+                 +------------------------------+ |               ++---------------+                |               +---------------+                  |
    // +----------+ TransformDummy|<----------------+ AsyncAccumulatingTransformer <-+---------------+|TransformDummy |<---------------+---------------+TransformDummy <------------------+
    // |          +---------------+                 +------------------------------+ |               ++---------------+                |               +---------------+
    // |                                                                             |                                                 |
    // |                                                                             |  If it includes 'computed', this section        |
    // |                                                                             |  of code will be executed, otherwise it won't   |
    // |                                                                             |                                                 |
    // |                                                                            -+-------------------------------------------------+
    // |
    // |
    // |
    // |        +------------------+            +-----------------------+        +-----------+
    // +------->|ResizeProcessor(1)+----------->|TableMutationAggregator+------->|CommitSink |
    //          +------------------+            +-----------------------+        +-----------+
    pub fn rowid_aggregate_mutator(
        &self,
        ctx: Arc<dyn TableContext>,
        block_builder: BlockBuilder,
        io_request_semaphore: Arc<Semaphore>,
        segment_locations: Vec<(SegmentIndex, Location)>,
    ) -> Result<PipeItem> {
        let read_settings = ReadSettings::from_ctx(&ctx)?;
        let aggregator = MatchedAggregator::create(
            ctx.clone(),
            self.table_info.schema(),
            self.get_operator(),
            self.get_write_settings(),
            read_settings,
            block_builder,
            io_request_semaphore,
            segment_locations,
        )?;
        Ok(aggregator.into_pipe_item())
    }
}
