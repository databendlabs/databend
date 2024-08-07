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
use std::str::FromStr;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_meta_types::new_log_id;
use databend_common_meta_types::LogId;

/// Structured snapshot id used by meta service
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct MetaSnapshotId {
    /// The last log id upto which a snapshot includes(inclusive).
    pub last_applied: Option<LogId>,

    /// A unique number to distinguish different snapshot with the same `last_applied.
    ///
    /// It is rare but possible a snapshot is built more than once with the same `last_applied`.
    pub uniq: u64,

    /// Optionally embed number of keys in snapshot id.
    ///
    /// When encoding a snapshot id to string, the key_num must be appended to the end of the string,
    /// in order to keep alphabetical order.
    pub key_num: Option<u64>,
}

impl MetaSnapshotId {
    pub fn new(last_applied: Option<LogId>, uniq: u64) -> Self {
        Self {
            last_applied,
            uniq,
            key_num: None,
        }
    }

    /// Create a new snapshot id with current time as `uniq` index.
    pub fn new_with_epoch(last_applied: Option<LogId>) -> Self {
        // Avoid dup
        std::thread::sleep(std::time::Duration::from_millis(2));

        let uniq = Self::epoch_millis();
        Self {
            last_applied,
            uniq,
            key_num: None,
        }
    }

    /// Add key num to the snapshot id
    pub fn with_key_num(mut self, n: Option<u64>) -> Self {
        self.key_num = n;
        self
    }

    fn epoch_millis() -> u64 {
        let milli = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let milli: u64 = milli.try_into().unwrap();
        milli
    }
}

impl FromStr for MetaSnapshotId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid = || format!("invalid snapshot_id: {}", s);

        let mut segs = s.split('-');

        let term = segs.next().ok_or_else(invalid)?;
        let node_id = segs.next().ok_or_else(invalid)?;
        let log_index = segs.next().ok_or_else(invalid)?;
        let snapshot_index = segs.next().ok_or_else(invalid)?;
        let key_num = segs.next();

        if segs.next().is_some() {
            return Err(invalid());
        }

        let log_id = if term.is_empty() {
            if node_id.is_empty() && log_index.is_empty() {
                None
            } else {
                return Err(invalid());
            }
        } else {
            let t = term.parse::<u64>().map_err(|_e| invalid())?;
            let n = node_id.parse::<u64>().map_err(|_e| invalid())?;
            let i = log_index.parse::<u64>().map_err(|_e| invalid())?;
            Some(new_log_id(t, n, i))
        };

        let snapshot_index = snapshot_index.parse::<u64>().map_err(|_e| invalid())?;

        let key_num = if let Some(num) = key_num {
            if let Some(stripped) = num.strip_prefix('k') {
                let n = stripped.parse::<u64>().map_err(|_e| invalid())?;
                Some(n)
            } else {
                return Err(format!("{}: key_num must be in form `k01234`", invalid()));
            }
        } else {
            None
        };

        Ok(Self {
            last_applied: log_id,
            uniq: snapshot_index,
            key_num,
        })
    }
}

impl Display for MetaSnapshotId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(last) = self.last_applied {
            write!(
                f,
                "{}-{}-{}-{}",
                last.leader_id.term, last.leader_id.node_id, last.index, self.uniq
            )?;
        } else {
            write!(f, "---{}", self.uniq)?;
        }

        if let Some(n) = self.key_num {
            write!(f, "-k{n}")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use databend_common_meta_types::new_log_id;

    use crate::state_machine::snapshot_id::MetaSnapshotId;

    #[test]
    fn test_meta_snapshot_id() -> anyhow::Result<()> {
        test_codec(MetaSnapshotId::new(None, 5), "---5");
        test_codec(MetaSnapshotId::new(Some(new_log_id(1, 8, 2)), 5), "1-8-2-5");

        assert!(MetaSnapshotId::from_str("").is_err());
        assert!(MetaSnapshotId::from_str("-").is_err());
        assert!(MetaSnapshotId::from_str("--").is_err());
        assert!(MetaSnapshotId::from_str("---").is_err());
        assert!(MetaSnapshotId::from_str("1---0").is_err());
        assert!(MetaSnapshotId::from_str("-1--0").is_err());
        assert!(MetaSnapshotId::from_str("--1-0").is_err());
        assert!(MetaSnapshotId::from_str("x-1-1-0").is_err());
        assert!(MetaSnapshotId::from_str("1-x-1-0").is_err());
        assert!(MetaSnapshotId::from_str("1-1-x-0").is_err());
        assert!(MetaSnapshotId::from_str("1-1-1-x").is_err());

        Ok(())
    }

    #[test]
    fn test_meta_snapshot_id_with_key_num() -> anyhow::Result<()> {
        test_codec(
            MetaSnapshotId::new(None, 5).with_key_num(Some(3)),
            "---5-k3",
        );
        test_codec(
            MetaSnapshotId::new(Some(new_log_id(1, 8, 2)), 5).with_key_num(Some(333)),
            "1-8-2-5-k333",
        );

        assert!(
            MetaSnapshotId::from_str("1-1-1-1-").is_err(),
            "invalid key_num"
        );
        assert!(
            MetaSnapshotId::from_str("1-1-1-1-123").is_err(),
            "invalid key_num"
        );
        assert!(
            MetaSnapshotId::from_str("1-1-1-1-k123").is_ok(),
            "valid key_num"
        );
        Ok(())
    }

    fn test_codec(id: MetaSnapshotId, id_str: &str) {
        let got_str = id.to_string();
        assert_eq!(id_str, got_str);

        let id2 = MetaSnapshotId::from_str(&got_str).unwrap();
        assert_eq!(id, id2);
    }
}
