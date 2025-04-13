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

//! This mod integrates watcher into meta service

use std::fmt;
use std::future::Future;
use std::io::Error;
use std::ops::Deref;

use databend_common_meta_raft_store::state_machine_api::SMEventSender;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::SeqV;
use futures::future::BoxFuture;
use log::debug;
use tonic::Status;
use watcher::dispatch::Command;
use watcher::dispatch::DispatcherHandle as GenericDispatcherHandle;
use watcher::type_config::KVChange;
use watcher::type_config::TypeConfig;

use crate::metrics::server_metrics;

/// Watch Type Config
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchTypes {}

impl TypeConfig for WatchTypes {
    type Key = String;
    type Value = SeqV;
    type Response = WatchResponse;
    type Error = Status;

    fn new_response(change: KVChange<Self>) -> Self::Response {
        WatchResponse::new3(change.0, change.1, change.2)
    }

    fn data_error(error: Error) -> Self::Error {
        Status::internal(error.to_string())
    }

    fn update_watcher_metrics(delta: i64) {
        server_metrics::incr_watchers(delta);
    }

    fn spawn<T>(fut: T)
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        databend_common_base::runtime::spawn(fut);
    }
}

/// A handle to a watching stream that feeds messages to connected watchers.
///
/// This is a wrapper around the generic dispatcher handle,
/// in order to implement 3rd party traits for it.
#[derive(Debug)]
pub struct DispatcherHandle {
    handle: GenericDispatcherHandle<WatchTypes>,
    name: String,
}

impl Drop for DispatcherHandle {
    fn drop(&mut self) {
        debug!("{}: drop", self);
    }
}

impl Deref for DispatcherHandle {
    type Target = GenericDispatcherHandle<WatchTypes>;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl fmt::Display for DispatcherHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "watcher-DispatcherHandle({})", self.name,)
    }
}

impl DispatcherHandle {
    pub fn new(handle: GenericDispatcherHandle<WatchTypes>, id: impl ToString) -> Self {
        let h = DispatcherHandle {
            handle,
            name: id.to_string(),
        };
        debug!("{}: new", h);
        h
    }
}

impl SMEventSender for DispatcherHandle {
    fn send(&self, change: KVChange<WatchTypes>) {
        self.send_change(change);
    }

    fn send_future(&self, fut: BoxFuture<'static, ()>) {
        self.send_command(Command::Future(fut));
    }
}
