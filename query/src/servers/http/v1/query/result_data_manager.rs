// Copyright 2021 Datafuse Labs.
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
use std::time::Instant;

use common_base::tokio;
use common_base::tokio::sync::mpsc;
use common_base::tokio::sync::mpsc::error::TryRecvError;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;

use crate::servers::http::v1::block_to_json;
use crate::servers::http::v1::JsonBlock;
use crate::servers::http::v1::JsonBlockRef;

const TARGET_ROWS_PER_PAGE: usize = 10000;

#[derive(Debug, PartialEq, Eq)]
pub enum Wait {
    Async,
    Sync,
    Deadline(Instant),
}

#[derive(Clone)]
pub struct Page {
    pub data: JsonBlockRef,
    pub total_rows: usize,
}

pub struct ResponseData {
    pub page: Page,
    pub next_page_no: Option<usize>,
}

pub struct ResultDataManager {
    pub(crate) schema: DataSchemaRef,
    total_rows: usize,
    total_pages: usize,
    last_page: Option<Page>,
    pub(crate) block_rx: mpsc::Receiver<DataBlock>,
    end: bool,
}

impl ResultDataManager {
    pub fn new(schema: DataSchemaRef, block_rx: mpsc::Receiver<DataBlock>) -> ResultDataManager {
        ResultDataManager {
            schema,
            block_rx,
            total_rows: 0,
            last_page: None,
            total_pages: 0,
            end: false,
        }
    }

    pub fn next_page_no(&mut self) -> Option<usize> {
        if self.end {
            None
        } else {
            Some(self.total_pages)
        }
    }

    pub async fn get_a_page(&mut self, page_no: usize, tp: &Wait) -> Result<Page> {
        let next_no = self.total_pages;
        if page_no == next_no && !self.end {
            let (block, end) = self.collect_new_page(tp).await;
            let num_row = block.len();
            self.total_rows += num_row;
            let page = Page {
                data: Arc::new(block),
                total_rows: self.total_rows,
            };
            if num_row > 0 {
                self.total_pages += 1;
                self.last_page = Some(page.clone());
            }
            self.end = end;
            Ok(page)
        } else if page_no == next_no - 1 {
            // later, there may be other ways to ack and drop the last page except collect_new_page.
            // but for now, last_page always exists in this branch, since page_no is unsigned.
            Ok(self
                .last_page
                .as_ref()
                .ok_or_else(|| ErrorCode::UnexpectedError("last_page is None"))?
                .clone())
        } else {
            let message = format!("wrong page number {}", page_no,);
            Err(ErrorCode::HttpNotFound(message))
        }
    }

    pub async fn receive(
        block_rx: &mut mpsc::Receiver<DataBlock>,
        tp: &Wait,
    ) -> std::result::Result<DataBlock, TryRecvError> {
        use Wait::*;
        match tp {
            Async => block_rx.try_recv(),
            Sync => block_rx.recv().await.ok_or(TryRecvError::Disconnected),
            Deadline(t) => {
                let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(*t));
                tokio::select! {
                    biased;
                    block = block_rx.recv() => block.ok_or(TryRecvError::Disconnected),
                    _ = sleep => Err(TryRecvError::Empty)
                }
            }
        }
    }

    pub async fn collect_new_page(&mut self, tp: &Wait) -> (JsonBlock, bool) {
        let mut results: Vec<JsonBlock> = Vec::new();
        let mut rows = 0;
        let block_rx = &mut self.block_rx;

        let mut end = false;
        loop {
            match ResultDataManager::receive(block_rx, tp).await {
                Ok(block) => {
                    rows += block.num_rows();
                    results.push(block_to_json(&block).unwrap());
                    // TODO(youngsofun):  set it in post if needed
                    if rows >= TARGET_ROWS_PER_PAGE {
                        break;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    tracing::debug!("no more data");
                    end = true;
                    break;
                }
            }
        }
        (results.concat(), end)
    }
}
