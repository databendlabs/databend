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
//! This crate bridges Databend's storage layer with the Vortex columnar file format.
//! It intentionally uses arrow 58 (required by vortex) which differs from the arrow 56
//! used by the rest of Databend. The public interface is purely byte-based so both
//! arrow versions can coexist in the same workspace without type conflicts.
//!
//! # Architecture
//!
//! ```text
//! Databend (arrow 56)          │  This crate (arrow 58 + vortex)
//!                              │
//! DataBlock ──IPC bytes──────► │ ──► RecordBatch(58) ──► VortexArray ──► .vortex file
//!                              │
//! DataBlock ◄──IPC bytes────── │ ◄── RecordBatch(58) ◄── VortexArray ◄── .vortex file
//! ```
//!
//! The IPC byte stream is the stable binary protocol shared between arrow 56 and 58.

mod error;
mod io;
mod reader;
mod schema;
mod writer;

pub use error::VortexStorageError;
pub use reader::read_vortex_file;
pub use writer::write_vortex_file;
