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

use std::str::FromStr;
use std::sync::Arc;

use async_stream::stream;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;

use crate::sessions::QueryContext;
use crate::storages::fuse::io;
use crate::storages::fuse::io::BlockStreamWriter;
use crate::storages::fuse::operations::AppendOperationLogEntry;
use crate::storages::fuse::FuseTable;
use crate::storages::fuse::DEFAULT_BLOCK_SIZE_IN_MEM_SIZE_THRESHOLD;
use crate::storages::fuse::DEFAULT_CHUNK_BLOCK_NUM;
use crate::storages::fuse::TBL_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD;
use crate::storages::fuse::TBL_OPT_KEY_CHUNK_BLOCK_NUM;

pub type AppendOperationLogEntryStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<AppendOperationLogEntry>> + Send>>;

impl FuseTable {
    #[inline]
    pub async fn append_trunks(
        &self,
        ctx: Arc<QueryContext>,
        stream: SendableDataBlockStream,
    ) -> Result<AppendOperationLogEntryStream> {
        let chunk_block_num = self.get_option(TBL_OPT_KEY_CHUNK_BLOCK_NUM, DEFAULT_CHUNK_BLOCK_NUM);
        let block_size_threshold = self.get_option(
            TBL_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD,
            DEFAULT_BLOCK_SIZE_IN_MEM_SIZE_THRESHOLD,
        );

        let da = ctx.get_data_accessor()?;

        let mut segment_stream = BlockStreamWriter::write_block_stream(
            da.clone(),
            stream,
            self.table_info.schema().clone(),
            chunk_block_num,
            block_size_threshold,
        )
        .await;

        let log_entries = stream! {
            while let Some(segment) = segment_stream.next().await {
                let log_entry_res = match segment {
                    Ok(seg) => {
                        let seg_loc = io::gen_segment_info_location();
                        let bytes = serde_json::to_vec(&seg)?;
                        da.put(&seg_loc, bytes).await?;
                        let log_entry = AppendOperationLogEntry::new(seg_loc, seg);
                        Ok(log_entry)
                    },
                    Err(err) => Err(err),
                };
                yield(log_entry_res);
            }
        };
        Ok(Box::pin(log_entries))
    }

    fn get_option<T: FromStr>(&self, opt_key: &str, default: T) -> T {
        self.table_info
            .options()
            .get(opt_key)
            .map(|s| s.parse::<T>().ok())
            .flatten()
            .unwrap_or(default)
    }
}
