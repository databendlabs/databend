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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::blocks_serializer::BlocksSerializer;
use crate::servers::http::v1::query::sized_spsc::SizedChannelReceiver;
use crate::servers::http::v1::query::sized_spsc::Wait;

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
    last_page: Option<Page>,
    block_receiver: SizedChannelReceiver,
}

impl PageManager {
    pub fn new(max_rows_per_page: usize, block_receiver: SizedChannelReceiver) -> PageManager {
        PageManager {
            total_rows: 0,
            last_page: None,
            total_pages: 0,
            end: false,
            block_receiver,
            max_rows_per_page,
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
            if !self.end {
                let (serializer, end) = self
                    .block_receiver
                    .collect_new_page(self.max_rows_per_page, tp)
                    .await?;
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
                    data: Arc::new(BlocksSerializer::empty()),
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

    #[async_backtrace::framed]
    pub async fn detach(&mut self) {
        self.block_receiver.close();
        self.last_page = None;
    }
}
