//  Copyright 2021 Datafuse Labs.
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
//

use std::sync::Arc;

use common_exception::Result;
use common_streams::SendableDataBlockStream;

use crate::datasources::table::fuse::io;
use crate::datasources::table::fuse::io::BlockAppender;
use crate::datasources::table::fuse::operations::AppendOperationLogEntry;
use crate::datasources::table::fuse::FuseTable;
use crate::datasources::table::fuse::DEFAULT_CHUNK_BLOCK_NUM;
use crate::datasources::table::fuse::TBL_OPT_KEY_CHUNK_BLOCK_NUM;
use crate::sessions::QueryContext;

impl FuseTable {
    #[inline]
    pub async fn append_trunks(
        &self,
        ctx: Arc<QueryContext>,
        stream: SendableDataBlockStream,
    ) -> Result<Vec<AppendOperationLogEntry>> {
        let chunk_size = self
            .table_info
            .options()
            .get(TBL_OPT_KEY_CHUNK_BLOCK_NUM)
            .map(|s| s.parse::<usize>().ok())
            .flatten()
            .unwrap_or(DEFAULT_CHUNK_BLOCK_NUM);

        let da = ctx.get_data_accessor()?;
        let segments = BlockAppender::append_blocks(
            da.clone(),
            stream,
            self.table_info.schema().as_ref(),
            chunk_size,
        )
        .await?;

        let mut result = Vec::with_capacity(segments.len());
        for seg in segments {
            let seg_loc = io::gen_segment_info_location();
            let bytes = serde_json::to_vec(&seg)?;
            da.put(&seg_loc, bytes).await?;
            result.push(AppendOperationLogEntry::new(seg_loc, seg))
        }
        Ok(result)
    }
}
