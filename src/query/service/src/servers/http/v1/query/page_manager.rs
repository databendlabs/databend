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

use std::cmp::min;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::tokio;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_io::prelude::FormatSettings;
use fastrace::future::FutureExt;
use fastrace::Span;
use itertools::Itertools;
use log::debug;
use log::info;
use parking_lot::RwLock;

use super::blocks_serializer::BlocksSerializer;
use crate::servers::http::v1::query::sized_spsc::SizedChannelReceiver;

#[derive(Debug, PartialEq, Eq)]
pub enum Wait {
    Async,
    Deadline(Instant),
}

#[derive(Clone)]
pub struct Page {
    pub data: Arc<BlocksSerializer>,
}

pub struct ResponseData {
    pub page: Page,
    pub next_page_no: Option<usize>,
}

pub struct PageManager {
    max_rows_per_page: usize,
    total_rows: usize,
    total_pages: usize,
    end: bool,
    block_end: bool,
    last_page: Option<Page>,
    row_buffer: Option<Vec<Column>>,
    block_receiver: SizedChannelReceiver<DataBlock>,
    pub(crate) format_settings: Arc<RwLock<Option<FormatSettings>>>,
}

impl PageManager {
    pub fn new(
        max_rows_per_page: usize,
        block_receiver: SizedChannelReceiver<DataBlock>,
        format_settings: Arc<RwLock<Option<FormatSettings>>>,
    ) -> PageManager {
        PageManager {
            total_rows: 0,
            last_page: None,
            total_pages: 0,
            end: false,
            block_end: false,
            row_buffer: Default::default(),
            block_receiver,
            max_rows_per_page,
            format_settings,
        }
    }

    pub fn next_page_no(&mut self) -> Option<usize> {
        if self.end {
            None
        } else {
            Some(self.total_pages)
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace(name = "PageManager::get_a_page")]
    pub async fn get_a_page(&mut self, page_no: usize, tp: &Wait) -> Result<Page> {
        let next_no = self.total_pages;
        if page_no == next_no {
            let mut serializer = BlocksSerializer::new(self.format_settings.read().clone());
            if !self.end {
                let end = self.collect_new_page(&mut serializer, tp).await?;
                let num_row = serializer.num_rows();
                log::debug!(num_row, wait_type:? = tp; "collect_new_page");
                self.total_rows += num_row;
                let page = Page {
                    data: Arc::new(serializer),
                };
                if num_row > 0 {
                    self.total_pages += 1;
                    self.last_page = Some(page.clone());
                }
                self.end = end;
                Ok(page)
            } else {
                // when end is set to true, client should recv a response with next_url = final_url
                // but the response may be lost and client will retry,
                // we simply return an empty page.
                let page = Page {
                    data: Arc::new(serializer),
                };
                Ok(page)
            }
        } else if page_no + 1 == next_no {
            // later, there may be other ways to ack and drop the last page except collect_new_page.
            // but for now, last_page always exists in this branch, since page_no is unsigned.
            Ok(self
                .last_page
                .as_ref()
                .ok_or_else(|| {
                    ErrorCode::Internal("[HTTP-QUERY] Failed to retrieve last page: page is None")
                })?
                .clone())
        } else {
            let message = format!(
                "[HTTP-QUERY] Invalid page number: requested {}, current page is {}",
                page_no, next_no
            );
            Err(ErrorCode::HttpNotFound(message))
        }
    }

    fn append_block(
        &mut self,
        serializer: &mut BlocksSerializer,
        block: DataBlock,
        remain_rows: &mut usize,
        remain_size: &mut usize,
    ) -> Result<()> {
        assert!(self.row_buffer.is_none());
        if block.is_empty() {
            return Ok(());
        }
        if !serializer.has_format() {
            let guard = self.format_settings.read();
            serializer.set_format(guard.as_ref().unwrap().clone());
        }

        let columns = block
            .columns()
            .iter()
            .map(BlockEntry::to_column)
            .collect_vec();

        let block_memory_size = block.memory_size();
        let mut take_rows = min(
            *remain_rows,
            if block_memory_size > *remain_size {
                (*remain_size * block.num_rows()) / block_memory_size
            } else {
                block.num_rows()
            },
        );
        // this means that the data in remaining_size cannot satisfy even one row.
        if take_rows == 0 {
            take_rows = 1;
        }

        if take_rows == block.num_rows() {
            // theoretically, it should always be smaller than the memory_size of the block.
            *remain_size -= min(*remain_size, block_memory_size);
            *remain_rows -= take_rows;
            serializer.append(columns, block.num_rows());
        } else {
            // Since not all rows of the block are used, either the size limit or the row limit must have been exceeded.
            // simply set any of remain_xxx to end the page.
            *remain_rows = 0;
            let fn_slice = |columns: &[Column], range: Range<usize>| {
                columns
                    .iter()
                    .map(|column| column.slice(range.clone()))
                    .collect_vec()
            };

            serializer.append(fn_slice(&columns, 0..take_rows), take_rows);
            self.row_buffer = Some(fn_slice(&columns, take_rows..block.num_rows()));
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn collect_new_page(
        &mut self,
        serializer: &mut BlocksSerializer,
        tp: &Wait,
    ) -> Result<bool> {
        let mut remain_size = 10 * 1024 * 1024;
        let mut remain_rows = self.max_rows_per_page;
        while remain_rows > 0 && remain_size > 0 {
            let Some(block) = self.row_buffer.take() else {
                break;
            };
            self.append_block(
                serializer,
                DataBlock::new_from_columns(block),
                &mut remain_rows,
                &mut remain_size,
            )?;
        }

        while remain_rows > 0 && remain_size > 0 {
            match tp {
                Wait::Async => match self.block_receiver.try_recv() {
                    Some(block) => {
                        self.append_block(serializer, block, &mut remain_rows, &mut remain_size)?
                    }
                    None => break,
                },
                Wait::Deadline(t) => {
                    let now = Instant::now();
                    let d = *t - now;
                    if d.is_zero() {
                        // timeout() will return Ok if the future completes immediately
                        break;
                    }
                    match tokio::time::timeout(d, self.block_receiver.recv())
                        .in_span(Span::enter_with_local_parent("PageManager::recv_block"))
                        .await
                    {
                        Ok(Some(block)) => {
                            debug!(
                                "[HTTP-QUERY] Received new data block with {} rows",
                                block.num_rows()
                            );
                            self.append_block(
                                serializer,
                                block,
                                &mut remain_rows,
                                &mut remain_size,
                            )?
                        }
                        Ok(None) => {
                            info!("[HTTP-QUERY] Reached end of data blocks");
                            break;
                        }
                        Err(_) => {
                            debug!("[HTTP-QUERY] Long polling timeout reached");
                            break;
                        }
                    }
                }
            }
        }

        // try to report 'no more data' earlier to client to avoid unnecessary http call
        if !self.block_end {
            self.block_end = self.block_receiver.is_empty();
        }
        Ok(self.block_end && self.row_buffer.is_none())
    }

    #[async_backtrace::framed]
    pub async fn detach(&mut self) {
        self.block_receiver.close();
        self.last_page = None;
        self.row_buffer = None;
    }
}
