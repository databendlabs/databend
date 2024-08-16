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

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::tokio;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_io::prelude::FormatSettings;
use log::debug;
use log::info;
use parking_lot::RwLock;

use crate::servers::http::v1::query::sized_spsc::SizedChannelReceiver;
use crate::servers::http::v1::string_block::block_to_strings;
use crate::servers::http::v1::StringBlock;

#[derive(Debug, PartialEq, Eq)]
pub enum Wait {
    Async,
    Deadline(Instant),
}

#[derive(Clone)]
pub struct Page {
    pub data: StringBlock,
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
    row_buffer: VecDeque<Vec<Option<String>>>,
    block_receiver: SizedChannelReceiver<DataBlock>,
    format_settings: Arc<RwLock<Option<FormatSettings>>>,
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
    pub async fn get_a_page(&mut self, page_no: usize, tp: &Wait) -> Result<Page> {
        let next_no = self.total_pages;
        if page_no == next_no {
            if !self.end {
                let (block, end) = self.collect_new_page(tp).await?;
                let num_row = block.num_rows();
                self.total_rows += num_row;
                let page = Page { data: block };
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
                    data: StringBlock::default(),
                };
                Ok(page)
            }
        } else if page_no + 1 == next_no {
            // later, there may be other ways to ack and drop the last page except collect_new_page.
            // but for now, last_page always exists in this branch, since page_no is unsigned.
            Ok(self
                .last_page
                .as_ref()
                .ok_or_else(|| ErrorCode::Internal("last_page is None"))?
                .clone())
        } else {
            let message = format!("wrong page number {}", page_no,);
            Err(ErrorCode::HttpNotFound(message))
        }
    }

    fn append_block(
        &mut self,
        res: &mut Vec<Vec<Option<String>>>,
        block: DataBlock,
        remain_rows: &mut usize,
        remain_size: &mut usize,
    ) -> Result<()> {
        let format_settings = {
            let guard = self.format_settings.read();
            guard.as_ref().unwrap().clone()
        };
        let rows = block_to_strings(&block, &format_settings)?;
        let mut i = 0;
        while *remain_rows > 0 && *remain_size > 0 {
            let size = row_size(&rows[i]);
            if *remain_size > size {
                *remain_size -= size;
                *remain_rows -= 1;
                i += 1;
            } else {
                *remain_size = 0;
            }
        }
        res.extend_from_slice(&rows[..i]);
        self.row_buffer = rows[i..].iter().cloned().collect();
        Ok(())
    }

    #[async_backtrace::framed]
    async fn collect_new_page(&mut self, tp: &Wait) -> Result<(StringBlock, bool)> {
        let mut res: Vec<Vec<Option<String>>> = Vec::with_capacity(self.max_rows_per_page);
        let mut remain_size = 10 * 1024 * 1024;
        let mut remain_rows = self.max_rows_per_page;
        while remain_rows > 0 && remain_size > 0 {
            if let Some(row) = self.row_buffer.pop_front() {
                let size = row_size(&row);
                if remain_size > size {
                    res.push(row);
                    remain_size -= size;
                    remain_rows -= 1;
                } else {
                    remain_size = 0;
                }
            }
        }

        while remain_rows > 0 && remain_size > 0 {
            match tp {
                Wait::Async => match self.block_receiver.try_recv() {
                    Some(block) => {
                        self.append_block(&mut res, block, &mut remain_rows, &mut remain_size)?
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
                    match tokio::time::timeout(d, self.block_receiver.recv()).await {
                        Ok(Some(block)) => {
                            debug!("http query got new block with {} rows", block.num_rows());
                            self.append_block(&mut res, block, &mut remain_rows, &mut remain_size)?;
                        }
                        Ok(None) => {
                            info!("http query reach end of blocks");
                            break;
                        }
                        Err(_) => {
                            debug!("http query long pulling timeout");
                            break;
                        }
                    }
                }
            }
        }

        let block = StringBlock { data: res };

        // try to report 'no more data' earlier to client to avoid unnecessary http call
        if !self.block_end {
            self.block_end = self.block_receiver.is_empty();
        }
        let end = self.block_end && self.row_buffer.is_empty();
        Ok((block, end))
    }

    #[async_backtrace::framed]
    pub async fn detach(&mut self) {
        self.block_receiver.close();
        self.last_page = None;
        self.row_buffer.clear()
    }
}

fn row_size(row: &[Option<String>]) -> usize {
    let n = row.len();
    // ["1","2",null],
    row.iter()
        .map(|s| match s {
            Some(s) => s.len(),
            None => 2,
        })
        .sum::<usize>()
        + n * 3
        + 2
}
