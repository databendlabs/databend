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

use std::str::FromStr;

use common_meta_stoerr::MetaStorageError;
use common_meta_types::LogId;

/// Structured snapshot id used by meta service
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaSnapshotId {
    /// The last log id upto which a snapshot includes(inclusive).
    pub last_applied: Option<LogId>,

    /// A unique number to distinguish different snapshot with the same `last_applied.
    ///
    /// It is rare but possible a snapshot is built more than once with the same `last_applied`.
    pub uniq: u64,
}

impl MetaSnapshotId {
    pub(crate) fn new(last_applied: Option<LogId>, uniq: u64) -> Self {
        Self { last_applied, uniq }
    }
}

impl FromStr for MetaSnapshotId {
    type Err = MetaStorageError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // TODO:
        let _ = s;
        unimplemented!("do not need to parse snapshot id");

        // let invalid = || {
        //     MetaStorageError::SnapshotError(AnyError::error(format!("invalid snapshot_id: {}", s)))
        // };
        // let mut segs = s.split('-');
        //
        // let term = segs.next().ok_or_else(invalid)?;
        // let log_index = segs.next().ok_or_else(invalid)?;
        // let snapshot_index = segs.next().ok_or_else(invalid)?;
        //
        // let log_id = if term.is_empty() {
        //     if log_index.is_empty() {
        //         None
        //     } else {
        //         return Err(invalid());
        //     }
        // } else {
        //     let t = term.parse::<u64>().map_err(|_e| invalid())?;
        //     let i = log_index.parse::<LogIndex>().map_err(|_e| invalid())?;
        //     Some(LogId::new(t, i))
        // };
        //
        // let snapshot_index = snapshot_index.parse::<u64>().map_err(|_e| invalid())?;
        //
        // Ok(Self {
        //     last_applied: log_id,
        //     uniq: snapshot_index,
        // })
    }
}

impl ToString for MetaSnapshotId {
    fn to_string(&self) -> String {
        if let Some(last) = self.last_applied {
            format!("{}-{}-{}", last.leader_id, last.index, self.uniq)
        } else {
            format!("---{}", self.uniq)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use common_meta_types::new_log_id;

    use crate::state_machine::snapshot_id::MetaSnapshotId;

    #[test]
    fn test_meta_snapshot_id() -> anyhow::Result<()> {
        assert_eq!("--5", MetaSnapshotId::new(None, 5).to_string());
        assert_eq!(
            "1-8-2-5",
            MetaSnapshotId::new(Some(new_log_id(1, 8, 2)), 5).to_string()
        );

        assert_eq!(
            Ok(MetaSnapshotId::new(None, 5)),
            MetaSnapshotId::from_str("--5")
        );
        assert_eq!(
            Ok(MetaSnapshotId::new(Some(new_log_id(1, 8, 2)), 5)),
            MetaSnapshotId::from_str("1-2-5")
        );

        assert!(MetaSnapshotId::from_str("").is_err());
        assert!(MetaSnapshotId::from_str("-").is_err());
        assert!(MetaSnapshotId::from_str("--").is_err());
        assert!(MetaSnapshotId::from_str("1--0").is_err());
        assert!(MetaSnapshotId::from_str("-1-0").is_err());
        assert!(MetaSnapshotId::from_str("x-1-0").is_err());
        assert!(MetaSnapshotId::from_str("1-x-0").is_err());
        assert!(MetaSnapshotId::from_str("1-1-x").is_err());

        Ok(())
    }
}
