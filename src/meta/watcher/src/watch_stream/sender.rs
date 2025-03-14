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

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;

use crate::type_config::TypeConfig;
use crate::WatchDesc;
use crate::WatchResult;

/// A handle to a watching stream that feeds messages to connected watchers.
///
/// The stream sender is responsible for sending watch events through the stream
/// to the client-side watcher. It encapsulates the communication channel between
/// the server's event source and the client's watch request.
#[derive(Clone)]
pub struct WatchStreamSender<C>
where C: TypeConfig
{
    pub desc: WatchDesc<C>,
    tx: mpsc::Sender<WatchResult<C>>,
}

impl<C> fmt::Debug for WatchStreamSender<C>
where C: TypeConfig
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "WatchStreamSender({:?})", self.desc)
    }
}

impl<C> PartialEq for WatchStreamSender<C>
where C: TypeConfig
{
    fn eq(&self, other: &Self) -> bool {
        self.desc.watcher_id == other.desc.watcher_id
    }
}

impl<C> Eq for WatchStreamSender<C> where C: TypeConfig {}

impl<C> PartialOrd for WatchStreamSender<C>
where C: TypeConfig
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<C> Ord for WatchStreamSender<C>
where C: TypeConfig
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.desc.watcher_id.cmp(&other.desc.watcher_id)
    }
}

impl<C> WatchStreamSender<C>
where C: TypeConfig
{
    pub fn new(desc: WatchDesc<C>, tx: mpsc::Sender<WatchResult<C>>) -> Self {
        WatchStreamSender { desc, tx }
    }

    pub async fn send(&self, resp: C::Response) -> Result<(), SendError<WatchResult<C>>> {
        self.tx.send(Ok(resp)).await
    }
}
