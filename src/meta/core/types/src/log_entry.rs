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

use std::fmt::Display;
use std::fmt::Formatter;
use std::time::Duration;

use display_more::DisplayUnixTimeStampExt;
use serde::Deserialize;
use serde::Serialize;

use crate::Cmd;

/// The application data request type which the `metasrv` works with.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct LogEntry {
    /// The time in millisecond when this log is proposed by the leader.
    ///
    /// State machine depends on clock time to expire values.
    /// The time to use has to be consistent on leader and followers.
    /// Otherwise an `apply` results in different state on leader and followers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_ms: Option<u64>,

    /// The action a client want to take.
    pub cmd: Cmd,
}

impl Display for LogEntry {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if let Some(time_ms) = &self.time_ms {
            write!(
                f,
                "time: {}",
                Duration::from_millis(*time_ms).display_unix_timestamp_short()
            )?;
        }

        write!(f, " cmd: {}", self.cmd)
    }
}

impl LogEntry {
    pub fn new(cmd: Cmd) -> Self {
        Self { time_ms: None, cmd }
    }

    pub fn new_with_time(cmd: Cmd, time_ms: Option<u64>) -> Self {
        Self { time_ms, cmd }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// With txid before 2025-07-19
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
    pub struct LogEntryWithTxid {
        pub txid: Option<()>,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub time_ms: Option<u64>,

        /// The action a client want to take.
        pub cmd: Cmd,
    }

    #[test]
    fn test_compat_with_and_without_txid() {
        let json_with_txid = r#"{"txid":null,"time_ms":1667290824603,"cmd":{"UpsertKV":{"key":"__fd_clusters","seq":{"Exact":0},"value":{"Update":[123]},"value_meta":{"expire_at":1667290884}}}}"#;

        let log_entry: LogEntry = serde_json::from_str(json_with_txid).unwrap();
        println!("{:?}", log_entry);

        let json_without_txid = r#"{"time_ms":1667290824603,"cmd":{"UpsertKV":{"key":"__fd_clusters","seq":{"Exact":0},"value":{"Update":[123]},"value_meta":{"expire_at":1667290884}}}}"#;

        let log_entry: LogEntryWithTxid = serde_json::from_str(json_without_txid).unwrap();
        println!("{:?}", log_entry);
    }
}
