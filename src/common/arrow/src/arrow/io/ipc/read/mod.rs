// Copyright 2020-2022 Jorge C. Leitão
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

//! APIs to read Arrow's IPC format.
//!
//! The two important structs here are the [`FileReader`](reader::FileReader),
//! which provides arbitrary access to any of its messages, and the
//! [`StreamReader`](stream::StreamReader), which only supports reading
//! data in the order it was written in.
use ahash::AHashMap;

use crate::arrow::array::Array;

mod array;
mod common;
mod deserialize;
mod error;
pub(crate) mod file;
mod read_basic;
mod reader;
mod schema;
mod stream;

#[cfg(feature = "io_flight")]
pub(crate) use common::read_dictionary;
#[cfg(feature = "io_flight")]
pub(crate) use common::read_record_batch;
pub use error::OutOfSpecKind;
pub use file::read_batch;
pub use file::read_file_dictionaries;
pub use file::read_file_metadata;
pub use file::FileMetadata;
pub use reader::FileReader;
pub use schema::deserialize_schema;
pub use stream::read_stream_metadata;
pub use stream::StreamMetadata;
pub use stream::StreamReader;
pub use stream::StreamState;

/// how dictionaries are tracked in this crate
pub type Dictionaries = AHashMap<i64, Box<dyn Array>>;

pub(crate) type Node<'a> = arrow_format::ipc::FieldNodeRef<'a>;
pub(crate) type IpcBuffer<'a> = arrow_format::ipc::BufferRef<'a>;
pub(crate) type Compression<'a> = arrow_format::ipc::BodyCompressionRef<'a>;
pub(crate) type Version = arrow_format::ipc::MetadataVersion;
