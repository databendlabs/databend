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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::OneBlockSource;

use super::CommitSink;
use crate::operations::common::AbortOperation;
use crate::operations::common::CommitMeta;
use crate::operations::common::ConflictResolveContext;
use crate::operations::common::TruncateGenerator;
use crate::FuseTable;

impl FuseTable {
    #[inline]
    #[async_backtrace::framed]
    pub async fn do_truncate(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        purge: bool,
    ) -> Result<()> {
        pipeline.add_source(
            |output| {
                let meta = CommitMeta {
                    conflict_resolve_context: ConflictResolveContext::None,
                    abort_operation: AbortOperation::default(),
                };
                let block = DataBlock::empty_with_meta(Box::new(meta));
                OneBlockSource::create(output, block)
            },
            1,
        )?;

        let snapshot_gen = TruncateGenerator::new(purge);
        pipeline.add_sink(|input| {
            CommitSink::try_create(
                self,
                ctx.clone(),
                None,
                vec![],
                snapshot_gen.clone(),
                input,
                None,
                None,
                None,
                None,
            )
        })
    }
}
