// Copyright 2021 Datafuse Labs.
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

use common_meta_sled_store::openraft;
use common_meta_types::anyerror::AnyError;
use common_meta_types::LogId;
use common_meta_types::LogIndex;
use common_meta_types::MetaStorageError;
use openraft::SnapshotMeta;
use serde::Deserialize;
use serde::Serialize;

/// The application snapshot type which the `MetaStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Snapshot {
    pub meta: SnapshotMeta,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

impl Snapshot {
    pub fn format_snapshot_id(last_applied: Option<LogId>, snapshot_index: u64) -> String {
        if let Some(last) = last_applied {
            format!("{}-{}-{}", last.term, last.index, snapshot_index)
        } else {
            format!("--{}", snapshot_index)
        }
    }

    pub fn parse_snapshot_id(s: &str) -> Result<(Option<LogId>, u64), MetaStorageError> {
        let invalid = || {
            MetaStorageError::SnapshotError(AnyError::error(format!("invalid snapshot_id: {}", s)))
        };
        let mut segs = s.split('-');

        let term = segs.next().ok_or_else(invalid)?;
        let log_index = segs.next().ok_or_else(invalid)?;
        let snapshot_index = segs.next().ok_or_else(invalid)?;

        let log_id = if term.is_empty() {
            if log_index.is_empty() {
                None
            } else {
                return Err(invalid());
            }
        } else {
            let t = term.parse::<u64>().map_err(|_e| invalid())?;
            let i = log_index.parse::<LogIndex>().map_err(|_e| invalid())?;
            Some(LogId::new(t, i))
        };

        let snapshot_index = snapshot_index.parse::<u64>().map_err(|_e| invalid())?;

        Ok((log_id, snapshot_index))
    }
}

#[cfg(test)]
mod tests {
    use common_meta_types::LogId;

    use crate::state_machine::Snapshot;

    #[test]
    fn test_snapshot_id() -> anyhow::Result<()> {
        assert_eq!("--5", Snapshot::format_snapshot_id(None, 5));
        assert_eq!(
            "1-2-5",
            Snapshot::format_snapshot_id(Some(LogId::new(1, 2)), 5)
        );

        assert_eq!(Ok((None, 5)), Snapshot::parse_snapshot_id("--5"));
        assert_eq!(
            Ok((Some(LogId::new(1, 2)), 5)),
            Snapshot::parse_snapshot_id("1-2-5")
        );

        assert!(Snapshot::parse_snapshot_id("").is_err());
        assert!(Snapshot::parse_snapshot_id("-").is_err());
        assert!(Snapshot::parse_snapshot_id("--").is_err());
        assert!(Snapshot::parse_snapshot_id("1--0").is_err());
        assert!(Snapshot::parse_snapshot_id("-1-0").is_err());
        assert!(Snapshot::parse_snapshot_id("x-1-0").is_err());
        assert!(Snapshot::parse_snapshot_id("1-x-0").is_err());
        assert!(Snapshot::parse_snapshot_id("1-1-x").is_err());

        Ok(())
    }
}
