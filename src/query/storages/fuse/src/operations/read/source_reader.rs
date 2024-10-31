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

use xorf::BinaryFuse16;

use crate::io::BlockReader;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ReaderState {
    Uninitialized,
    All,
    Filter(usize),
    Remaining,
    Finish,
}

impl ReaderState {
    pub fn next_reader_state(&self, source_reader: &SourceReader) -> ReaderState {
        match self {
            ReaderState::Uninitialized => match source_reader {
                SourceReader::Parquet { filter_readers, .. } => {
                    if filter_readers.is_empty() {
                        ReaderState::All
                    } else {
                        ReaderState::Filter(0)
                    }
                }
            },
            ReaderState::Filter(index) => match source_reader {
                SourceReader::Parquet {
                    filter_readers,
                    remaining_reader,
                    ..
                } => {
                    if index + 1 < filter_readers.len() {
                        ReaderState::Filter(*index + 1)
                    } else if remaining_reader.is_some() {
                        ReaderState::Remaining
                    } else {
                        ReaderState::Finish
                    }
                }
            },
            ReaderState::All | ReaderState::Remaining | ReaderState::Finish => ReaderState::Finish,
        }
    }
}

#[derive(Clone)]
pub enum SourceReader {
    Parquet {
        block_reader: Arc<BlockReader>,
        filter_readers: Vec<(Arc<BlockReader>, Arc<BinaryFuse16>)>,
        remaining_reader: Option<Arc<BlockReader>>,
        column_positions: Vec<usize>,
    },
}

impl SourceReader {
    pub fn source_block_reader(&self, reader_state: &ReaderState) -> SourceBlockReader {
        match self {
            SourceReader::Parquet {
                block_reader,
                filter_readers,
                remaining_reader,
                ..
            } => {
                let (source_block_reader, bloom_filter) = match reader_state {
                    ReaderState::All => (block_reader.clone(), None),
                    ReaderState::Filter(index) => (
                        filter_readers[*index].0.clone(),
                        Some(filter_readers[*index].1.clone()),
                    ),
                    ReaderState::Remaining => (remaining_reader.as_ref().unwrap().clone(), None),
                    _ => unreachable!(),
                };
                SourceBlockReader::Parquet {
                    block_reader: source_block_reader,
                    bloom_filter,
                }
            }
        }
    }
}

pub enum SourceBlockReader {
    Parquet {
        block_reader: Arc<BlockReader>,
        bloom_filter: Option<Arc<BinaryFuse16>>,
    },
}
