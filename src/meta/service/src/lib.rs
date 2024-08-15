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

#![feature(try_blocks)]
#![feature(coroutines)]
#![allow(clippy::uninlined_format_args)]

pub mod api;
pub mod configs;
pub mod export;
pub mod message;
pub mod meta_service;
pub mod metrics;
pub mod network;
pub mod raft_client;
pub(crate) mod request_handling;
pub mod store;
pub mod version;
pub mod watcher;

pub trait Opened {
    /// Return true if it is opened from a previous persistent state.
    /// Otherwise it is just created.
    fn is_opened(&self) -> bool;
}
