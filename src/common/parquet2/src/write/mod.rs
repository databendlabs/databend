// Copyright [2021] [Jorge C Leitao]
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

mod column_chunk;
mod compression;
mod file;
mod indexes;
pub(crate) mod page;
mod row_group;
pub(self) mod statistics;

#[cfg(feature = "async")]
mod stream;
#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use stream::FileStreamer;

mod dyn_iter;
pub use compression::compress;
pub use compression::Compressor;
pub use dyn_iter::DynIter;
pub use dyn_iter::DynStreamingIterator;
pub use file::write_metadata_sidecar;
pub use file::FileWriter;
pub use row_group::ColumnOffsetsMetadata;

use crate::page::CompressedPage;

pub type RowGroupIter<'a, E> =
    DynIter<'a, std::result::Result<DynStreamingIterator<'a, CompressedPage, E>, E>>;

/// Write options of different interfaces on this crate
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct WriteOptions {
    /// Whether to write statistics, including indexes
    pub write_statistics: bool,
    /// Which Parquet version to use
    pub version: Version,
}

/// The parquet version to use
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Version {
    V1,
    V2,
}

/// Used to recall the state of the parquet writer - whether sync or async.
#[derive(PartialEq)]
enum State {
    Initialised,
    Started,
    Finished,
}

impl From<Version> for i32 {
    fn from(version: Version) -> Self {
        match version {
            Version::V1 => 1,
            Version::V2 => 2,
        }
    }
}
