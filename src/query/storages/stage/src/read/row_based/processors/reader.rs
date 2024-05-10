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

use bytes::BufMut;
use bytes::BytesMut;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_sources::PrefetchAsyncSource;
use futures::StreamExt;
use log::debug;
use opendal::Operator;

use crate::one_file_partition::OneFilePartition;
use crate::read::row_based::batch::BytesBatch;

struct FileState {
    file: OneFilePartition,
    reader: opendal::FuturesBytesStream,
    buf: BytesMut,

    consume_offset: usize,
    read_offset: usize,
}

pub struct BytesReader {
    table_ctx: Arc<dyn TableContext>,
    op: Operator,
    read_batch_size: usize,
    io_size: usize,
    file_state: Option<FileState>,
    prefetch_num: usize,
}

impl BytesReader {
    pub fn try_create(
        table_ctx: Arc<dyn TableContext>,
        op: Operator,
        read_batch_size: usize,
        prefetch_num: usize,
    ) -> Result<Self> {
        // TODO: Use 8MiB as default IO size for now, we can extract as a new config.
        let default_io_size = 8 * 1024 * 1024;
        // Calculate the IO size, which:
        //
        // - is the multiple of read_batch_size.
        // - is larger or equal to default_io_size.
        let io_size = ((default_io_size + read_batch_size - 1) / read_batch_size) * read_batch_size;

        Ok(Self {
            table_ctx,
            op,
            read_batch_size,
            io_size,
            file_state: None,
            prefetch_num,
        })
    }

    pub async fn read_batch(&mut self) -> Result<DataBlock> {
        if let Some(state) = &mut self.file_state {
            if state.buf.is_empty() {
                // TODO: we need to find a way to avoid this copy.
                let bs = state.reader.next().await.transpose()?;
                match bs {
                    Some(bs) => {
                        debug!("BytesReader read {} bytes", bs.len());
                        state.read_offset += bs.len();
                        state.buf.put(bs)
                    }
                    None => {
                        if state.read_offset != state.file.size {
                            return Err(ErrorCode::BadBytes(format!(
                                "Unexpected EOF {} expect {} bytes, read only {} bytes.",
                                state.file.path, state.file.size, state.read_offset
                            )));
                        };
                    }
                }
            }

            let size = self.read_batch_size.min(state.buf.len());
            // TODO: we can use bytes instead to better reuse existing allocation.
            let buffer = state.buf.split_to(size).to_vec();

            Profile::record_usize_profile(ProfileStatisticsName::ScanBytes, size);
            self.table_ctx.get_scan_progress().incr(&ProgressValues {
                rows: 0,
                bytes: size,
            });

            let offset = state.consume_offset;
            state.consume_offset += size;
            debug!("BytesReader consumed {size} bytes");
            let is_eof = state.consume_offset == state.file.size;

            let batch = Box::new(BytesBatch {
                data: buffer,
                path: state.file.path.clone(),
                offset,
                is_eof,
            });
            if is_eof {
                self.file_state = None;
            }
            Ok(DataBlock::empty_with_meta(batch))
        } else {
            Err(ErrorCode::Internal(
                "Bug: BytesReader::read_batch() should not be called with file_state = None.",
            ))
        }
    }
}

#[async_trait::async_trait]
impl PrefetchAsyncSource for BytesReader {
    const NAME: &'static str = "BytesReader";

    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    fn is_full(&self, prefetched: &[DataBlock]) -> bool {
        prefetched.len() >= self.prefetch_num
    }

    #[async_trait::unboxed_simple]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.file_state.is_none() {
            let part = match self.table_ctx.get_partition() {
                Some(part) => part,
                None => return Ok(None),
            };
            let file = OneFilePartition::from_part(&part)?.clone();

            let reader = self
                .op
                .reader_with(&file.path)
                .chunk(self.io_size)
                // TODO: Use 4 concurrent for test.
                .concurrent(4)
                .await?
                .into_bytes_stream(0..file.size as u64);
            self.file_state = Some(FileState {
                file,
                reader,
                buf: BytesMut::with_capacity(self.io_size),
                consume_offset: 0,
                read_offset: 0,
            })
        }
        match self.read_batch().await {
            Ok(block) => Ok(Some(block)),
            Err(e) => Err(e),
        }
    }
}
