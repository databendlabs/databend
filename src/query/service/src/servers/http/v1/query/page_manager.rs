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

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use common_base::base::tokio;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Chunk;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_io::prelude::FormatSettings;
use serde_json::Value as JsonValue;
use tracing::info;

use crate::servers::http::v1::json_block::block_to_json_value;
use crate::servers::http::v1::query::sized_spsc::SizedChannelReceiver;
use crate::servers::http::v1::JsonBlock;

#[derive(Debug, PartialEq, Eq)]
pub enum Wait {
    Async,
    Deadline(Instant),
}

#[derive(Clone)]
pub struct Page {
    pub data: JsonBlock,
    pub total_rows: usize,
}

pub struct ResponseData {
    pub page: Page,
    pub next_page_no: Option<usize>,
}

pub struct PageManager {
    query_id: String,
    max_rows_per_page: usize,
    total_rows: usize,
    total_pages: usize,
    end: bool,
    chunk_end: bool,
    schema: DataSchemaRef,
    last_page: Option<Page>,
    row_buffer: VecDeque<Vec<JsonValue>>,
    chunk_receiver: SizedChannelReceiver<Chunk>,
    string_fields: bool,
    format_settings: FormatSettings,
}

impl PageManager {
    pub fn new(
        query_id: String,
        max_rows_per_page: usize,
        chunk_receiver: SizedChannelReceiver<Chunk>,
        string_fields: bool,
        format_settings: FormatSettings,
    ) -> PageManager {
        PageManager {
            query_id,
            total_rows: 0,
            last_page: None,
            total_pages: 0,
            end: false,
            chunk_end: false,
            row_buffer: Default::default(),
            schema: Arc::new(DataSchema::empty()),
            chunk_receiver,
            max_rows_per_page,
            string_fields,
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

    pub async fn get_a_page(&mut self, page_no: usize, tp: &Wait) -> Result<Page> {
        let next_no = self.total_pages;
        if page_no == next_no && !self.end {
            let (chunk, end) = self.collect_new_page(tp).await?;
            let num_row = chunk.num_rows();
            self.total_rows += num_row;
            let page = Page {
                data: chunk,
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
                .ok_or_else(|| ErrorCode::Internal("last_page is None"))?
                .clone())
        } else {
            let message = format!("wrong page number {}", page_no,);
            Err(ErrorCode::HttpNotFound(message))
        }
    }

    fn append_chunk(
        &mut self,
        rows: &mut Vec<Vec<JsonValue>>,
        chunk: Chunk,
        remain: usize,
    ) -> Result<()> {
        let format_settings = &self.format_settings;
        if self.schema.fields().is_empty() {
            self.schema = chunk.schema().clone();
        }
        let mut iter = block_to_json_value(&chunk, format_settings, self.string_fields)?
            .into_iter()
            .peekable();
        let chunk: Vec<_> = iter.by_ref().take(remain).collect();
        rows.extend(chunk);
        self.row_buffer = iter.by_ref().collect();
        Ok(())
    }

    async fn collect_new_page(&mut self, tp: &Wait) -> Result<(JsonBlock, bool)> {
        let mut res: Vec<Vec<JsonValue>> = Vec::with_capacity(self.max_rows_per_page);
        while res.len() < self.max_rows_per_page {
            if let Some(row) = self.row_buffer.pop_front() {
                res.push(row)
            } else {
                break;
            }
        }
        loop {
            assert!(self.max_rows_per_page >= res.len());
            let remain = self.max_rows_per_page - res.len();
            if remain == 0 {
                break;
            }
            match tp {
                Wait::Async => match self.chunk_receiver.try_recv() {
                    Some(chunk) => self.append_chunk(&mut res, chunk, remain)?,
                    None => break,
                },
                Wait::Deadline(t) => {
                    let now = Instant::now();
                    let d = *t - now;
                    match tokio::time::timeout(d, self.chunk_receiver.recv()).await {
                        Ok(Some(chunk)) => {
                            info!(
                                "http query {} got new chunk with {} rows",
                                &self.query_id,
                                chunk.num_rows()
                            );
                            self.append_chunk(&mut res, chunk, remain)?;
                        }
                        Ok(None) => {
                            info!("http query {} reach end of chunks", &self.query_id);
                            break;
                        }
                        Err(_) => {
                            info!("http query {} long pulling timeout", &self.query_id);
                            break;
                        }
                    }
                }
            }
        }

        let block = JsonBlock {
            schema: self.schema.clone(),
            data: res,
        };

        // try to report 'no more data' earlier to client to avoid unnecessary http call
        if !self.chunk_end {
            self.chunk_end = self.chunk_receiver.is_empty();
        }
        let end = self.chunk_end && self.row_buffer.is_empty();
        Ok((block, end))
    }

    pub async fn detach(&self) {
        self.chunk_receiver.close();
    }
}
