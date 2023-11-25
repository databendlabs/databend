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

mod data;
mod deserialize;
mod filter;
mod reader;
mod reader_merge_io;
mod reader_merge_io_async;
mod reader_merge_io_sync;

pub use data::BlockIterator;
pub use data::IndexedChunk;
pub use data::IndexedChunks;
pub use data::Parquet2PartData;
pub use reader::Parquet2Reader;
pub use reader_merge_io::MergeIOReadResult;
pub use reader_merge_io::OwnerMemory;
