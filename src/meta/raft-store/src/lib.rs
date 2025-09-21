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

//! Raft-based distributed metadata storage for Databend.
//!
//! This crate implements a distributed metadata store using the Raft consensus algorithm.
//! It provides transactional KV operations, cluster membership management, and state machine
//! replication with support for both legacy (V003) and current (V004) storage formats.
//!
//! ## Core Components
//!
//! - **`config`**: Raft cluster configuration and tuning parameters
//! - **`key_spaces`**: Storage partitioning by data type with unique prefixes
//! - **`leveled_store`**: Multi-level storage engine for efficient data organization
//! - **`sm_v003`**: Legacy sled-based state machine implementation
//! - **`raft_log_v004`**: Current WAL-based raft log storage
//! - **`state_machine`**: Core state machine API and metadata management
//! - **`applier`**: Log entry application and state transitions
//!
//! ## Version Compatibility
//!
//! - **V003**: Legacy format using sled for both logs and state machine
//! - **V004**: Current format with separate WAL for logs and leveled storage for state machine

#![allow(clippy::uninlined_format_args)]
#![feature(coroutines)]
#![feature(impl_trait_in_assoc_type)]
#![feature(try_blocks)]
#![allow(clippy::diverging_sub_expression)]

pub mod applier;
pub mod config;
pub mod immutable_compactor;
pub mod key_spaces;
pub mod leveled_store;
pub mod ondisk;
pub mod raft_log_v004;
pub mod sm_v003;
pub mod snapshot_config;
pub mod state;
pub mod state_machine;
pub mod state_machine_api_ext;
pub(crate) mod testing;
pub mod utils;

mod state_machine_features;

pub use state_machine_features::StateMachineFeature;
