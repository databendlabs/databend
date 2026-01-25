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

use std::io;
use std::sync::mpsc::SyncSender;
use std::time::Duration;

use databend_common_meta_types::raft_types;
use log::info;
use log::warn;
use tokio::sync::oneshot;

use crate::raft_log_v004::IODesc;
use crate::raft_log_v004::callback_data::CallbackData;

/// The callback to be called when the IO is completed.
///
/// This is used as a wrapper of Openraft callback or used directly internally in RaftLog.
pub struct Callback {
    io_desc: IODesc,
    data: CallbackData,
}

impl Callback {
    pub fn new_io_flushed(io_flushed: raft_types::IOFlushed, mut io_desc: IODesc) -> Self {
        io_desc.set_flush_time();

        Callback {
            io_desc,
            data: CallbackData::IOFlushed(io_flushed),
        }
    }

    pub fn new_oneshot(tx: oneshot::Sender<Result<(), io::Error>>, mut io_desc: IODesc) -> Self {
        io_desc.set_flush_time();

        Callback {
            io_desc,
            data: CallbackData::Oneshot(tx),
        }
    }

    pub fn new_sync_oneshot(tx: SyncSender<Result<(), io::Error>>, mut io_desc: IODesc) -> Self {
        io_desc.set_flush_time();

        Callback {
            io_desc,
            data: CallbackData::SyncOneshot(tx),
        }
    }
}

impl raft_log::Callback for Callback {
    fn send(mut self, res: Result<(), io::Error>) {
        self.io_desc.set_flush_done_time();

        info!("{}: Callback is called with: {:?}", self.io_desc, res);

        if let Some(duration) = self.io_desc.total_duration() {
            if duration > Duration::from_millis(50) {
                warn!("{}: Slow IO operation: {:?}", self.io_desc, res);
            }
        }

        match self.data {
            CallbackData::Oneshot(tx) => {
                let send_res = tx.send(res);
                if send_res.is_err() {
                    warn!(
                        "{}: Callback failed to send Oneshot result back to caller",
                        self.io_desc
                    );
                }
            }
            CallbackData::SyncOneshot(tx) => tx.send(res),
            CallbackData::IOFlushed(io_flushed) => io_flushed.io_completed(res),
        }
    }
}
