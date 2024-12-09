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

use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;

use databend_common_meta_types::protobuf::WatchResponse;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tonic::Status;

use crate::watcher::desc::WatchDesc;

/// A handle of a watching stream, for feeding messages to the stream.
#[derive(Clone)]
pub struct StreamSender {
    pub desc: WatchDesc,
    tx: mpsc::Sender<Result<WatchResponse, Status>>,
}

impl fmt::Debug for StreamSender {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "WatchStreamSender({:?})", self.desc)
    }
}

impl PartialEq for StreamSender {
    fn eq(&self, other: &Self) -> bool {
        self.desc.watcher_id == other.desc.watcher_id
    }
}

impl Eq for StreamSender {}

impl PartialOrd for StreamSender {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamSender {
    fn cmp(&self, other: &Self) -> Ordering {
        self.desc.watcher_id.cmp(&other.desc.watcher_id)
    }
}

impl StreamSender {
    pub fn new(desc: WatchDesc, tx: mpsc::Sender<Result<WatchResponse, Status>>) -> Self {
        StreamSender { desc, tx }
    }

    pub async fn send(
        &self,
        resp: WatchResponse,
    ) -> Result<(), SendError<Result<WatchResponse, Status>>> {
        self.tx.send(Ok(resp)).await
    }
}
