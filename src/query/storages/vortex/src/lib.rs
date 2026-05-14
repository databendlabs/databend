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

//! Vortex storage format integration for Databend Fuse engine.
//!
//! This crate bridges Databend's Fuse storage engine with the Vortex columnar
//! file format (https://github.com/vortex-data/vortex).
//!
//! # Architecture
//!
//! ```text
//! Databend Fuse                    databend-storages-vortex
//!
//! DataBlock ──to_record_batch()──► RecordBatch
//!           ──Arrow IPC bytes───► ──► VortexArray ──► .vortex file (object storage)
//!
//! DataBlock ◄──from_record_batch() ◄── RecordBatch
//!           ◄──Arrow IPC bytes──── ◄── VortexArray ◄── .vortex file (object storage)
//! ```
//!
//! # IO
//!
//! The IO bridge is implemented directly on top of `opendal::Operator` via
//! `OpendalVortexReader` (implements `VortexReadAt`), bypassing the
//! `object_store` version conflict between the workspace (0.12) and vortex-io (0.13).
//!
//! # Statistics
//!
//! Column statistics (min/max/null_count) are stored externally in Fuse's
//! `BlockMeta.col_stats` — NOT inside the Vortex file. The Vortex file is a
//! pure data container. This keeps the pruning logic entirely in Fuse's
//! existing metadata layer.

mod error;
mod io;
mod reader;
mod schema;
mod writer;

pub use error::VortexStorageError;
pub use reader::read_vortex_file;
pub use writer::write_vortex_file;
