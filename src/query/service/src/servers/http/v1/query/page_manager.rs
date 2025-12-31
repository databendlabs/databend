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

use super::Wait;
use super::blocks_serializer::BlocksSerializer;
use super::http_query::PaginationConf;
use super::sized_spsc::SizedChannelReceiver;
use super::sized_spsc::SizedChannelSender;
use super::sized_spsc::sized_spsc;
use crate::spillers::LiteSpiller;

#[derive(Clone)]
pub struct Page {
    pub data: Arc<BlocksSerializer>,
}

pub struct ResponseData {
    pub page: Page,
    pub next_page_no: Option<usize>,
}

pub struct PageManager {
    rows_returned: usize,
    pages_returned: usize,
    end: bool,
    last_page: Option<Page>,
    receiver: SizedChannelReceiver<LiteSpiller>,
}

impl PageManager {
    pub fn create(conf: &PaginationConf) -> (PageManager, SizedChannelSender<LiteSpiller>) {
        let (sender, receiver) =
            sized_spsc::<LiteSpiller>(conf.max_rows_in_buffer, conf.max_rows_per_page);

        (
            PageManager {
                rows_returned: 0,
                last_page: None,
                pages_returned: 0,
                end: false,
                receiver,
            },
            sender,
        )
    }

    pub fn next_page_no(&mut self) -> Option<usize> {
        if self.end {
            None
        } else {
            Some(self.pages_returned)
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace(name = "PageManager::get_a_page")]
    pub async fn get_a_page(&mut self, page_no: usize, wait: &Wait) -> Result<Page> {
        let next_no = self.pages_returned;
        if page_no == next_no {
            if !self.end {
                let start_time = std::time::Instant::now();
                let (serializer, end) = self.receiver.next_page(wait).await?;
                let num_row = serializer.num_rows();
                let duration_ms = start_time.elapsed().as_millis();

                log::debug!(num_row, wait_type:? = wait; "collect_new_page");

                // Only log non-empty pages to avoid spam during long SQL waits
                if num_row > 0 {
                    log::info!(
                        target: "result-set-spill",
                        "[RESULT-SET-SPILL] Page received page_no={}, rows={}, total_rows={}, end={}, duration_ms={}",
                        self.pages_returned, num_row, self.rows_returned + num_row, end, duration_ms
                    );
                } else if end {
                    // Only log empty page when query ends
                    log::info!(
                        target: "result-set-spill",
                        "[RESULT-SET-SPILL] Query completed with empty final page page_no={}, total_rows={}",
                        self.pages_returned, self.rows_returned
                    );
                }

                let page = Page {
                    data: Arc::new(serializer),
                };
                if num_row > 0 {
                    self.rows_returned += num_row;
                    self.pages_returned += 1;
                    self.last_page = Some(page.clone());
                }
                self.end = end;
                Ok(page)
            } else {
                // when end is set to true, client should recv a response with next_url = final_url
                // but the response may be lost and client will retry,
                // we simply return an empty page.
                Ok(Page {
                    data: Arc::new(BlocksSerializer::empty()),
                })
            }
        } else if page_no + 1 == next_no {
            // later, there may be other ways to ack and drop the last page except collect_new_page.
            // but for now, last_page always exists in this branch, since page_no is unsigned.
            Ok(self
                .last_page
                .as_ref()
                .ok_or_else(|| ErrorCode::Internal("Failed to retrieve last page: page is None"))?
                .clone())
        } else {
            let message = format!(
                "Invalid page number: requested {}, current page is {}",
                page_no, next_no
            );
            Err(ErrorCode::HttpNotFound(message))
        }
    }

    #[async_backtrace::framed]
    pub async fn close(&mut self) {
        log::info!(
            target: "result-set-spill",
            "[RESULT-SET-SPILL] Query completed total_pages={}, total_rows={}",
            self.pages_returned, self.rows_returned
        );
        if let Some(spiller) = self.receiver.close() {
            let start_time = std::time::Instant::now();
            match spiller.cleanup().await {
                Ok(_) => {
                    let duration_ms = start_time.elapsed().as_millis();
                    log::info!(
                        target: "result-set-spill",
                        "[RESULT-SET-SPILL] Cleanup completed duration_ms={}",
                        duration_ms
                    );
                }
                Err(error) => {
                    log::error!(
                        target: "result-set-spill",
                        error:?; "[RESULT-SET-SPILL] Failed to cleanup spilled result set files"
                    );
                }
            }
        };
        self.last_page = None;
    }
}
