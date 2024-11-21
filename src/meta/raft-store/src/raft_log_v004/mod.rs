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

pub mod callback;
pub mod callback_data;
pub mod codec_wrapper;
pub mod importer;
pub mod io_desc;
pub mod io_phase;
pub mod log_store_meta;
pub mod raft_log_io_error;
pub mod raft_log_types;
pub mod util;

pub const TREE_RAFT_LOG: &str = "raft_log";

pub type RaftLogV004 = raft_log::RaftLog<RaftLogTypes>;
pub type RaftLogConfig = raft_log::Config;
pub type RaftLogStat = raft_log::Stat<RaftLogTypes>;

pub use callback::Callback;
pub use callback_data::CallbackData;
pub use codec_wrapper::Cw;
pub use importer::Importer;
pub use io_desc::IODesc;
pub use io_phase::IOPhase;
pub use log_store_meta::LogStoreMeta;
pub use raft_log_io_error::RaftLogIOError;
pub use raft_log_types::RaftLogTypes;
