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

use raft_log::api::raft_log_writer::RaftLogWriter;
use tokio::sync::oneshot;

use crate::raft_log_v004::IODesc;
use crate::raft_log_v004::RaftLogV004;
use crate::raft_log_v004::callback::Callback;

pub async fn blocking_flush(rl: &mut RaftLogV004) -> Result<(), io::Error> {
    let (tx, rx) = oneshot::channel();
    let callback = Callback::new_oneshot(tx, IODesc::unknown("blocking_flush(RaftLogV004)"));

    rl.flush(callback)?;
    rx.await
        .map_err(|_e| io::Error::other("Failed to receive flush completion"))??;
    Ok(())
}
