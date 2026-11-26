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

use std::fmt;
use std::io;

use crate::PermitSeq;

/// The key of a semaphore permit in the meta-service queue.
///
/// The serialized format is:
/// ```
/// {prefix}/queue/{seq}
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PermitKey {
    /// The prefix of the semaphore. Usually the name of the semaphore.
    pub prefix: String,

    /// The sequence number of the semaphore permit.
    ///
    /// A sequence number is assigned to each semaphore permit and is globally unique.
    pub seq: PermitSeq,
}

impl fmt::Display for PermitKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PermitKey({}/{:020})", self.prefix, self.seq)
    }
}

impl PermitKey {
    pub fn new(prefix: impl ToString, seq: PermitSeq) -> Self {
        Self {
            prefix: prefix.to_string(),
            seq,
        }
    }

    /// The directory of the permit keys of the semaphore `prefix`, `{prefix}/queue/`: every key
    /// that [`Self::format_key`] builds for the semaphore starts with it.
    pub fn queue_dir(prefix: &str) -> String {
        format!("{}/queue/", prefix)
    }

    /// Format the key as `{prefix}/queue/{seq}`, with `seq` zero-padded to 20 digits, so that the
    /// keys of a semaphore sort in the order of their seqs.
    pub fn format_key(&self) -> String {
        format!("{}{:020}", Self::queue_dir(&self.prefix), self.seq)
    }

    /// Parse the key from an encoded string.
    pub fn parse_key(key: &str) -> Result<Self, io::Error> {
        // split by the last 2 slashes:
        let parts = key.rsplitn(3, '/').collect::<Vec<_>>();

        if parts.len() != 3 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "semaphore permit key should have at least 3 segments: {}",
                    key
                ),
            ));
        }

        if parts[1] != "queue" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "semaphore permit key should have 'queue' as the second segment: {}",
                    key
                ),
            ));
        }

        let seq = parts[0].parse::<u64>().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("failed to parse seq number: {}", e),
            )
        })?;

        Ok(Self {
            prefix: parts[2].to_string(),
            seq,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        let key = PermitKey::new("test", 1);
        assert_eq!(key.to_string(), "PermitKey(test/00000000000000000001)");
    }

    #[test]
    fn test_parse_key() {
        let key = PermitKey::new("test", 1);
        assert_eq!(key.format_key(), "test/queue/00000000000000000001");

        let key = PermitKey::parse_key("test/queue/00000000000000000001").unwrap();
        assert_eq!(key.prefix, "test");
        assert_eq!(key.seq, 1);
    }

    #[test]
    fn test_queue_dir() {
        assert_eq!(PermitKey::queue_dir("test"), "test/queue/");

        let key = PermitKey::new("test", 1).format_key();
        assert!(key.starts_with(&PermitKey::queue_dir("test")), "{key}");
    }

    #[test]
    fn test_parse_invalid_key() {
        // Test with invalid format (not enough segments)
        let result = PermitKey::parse_key("test/queue");
        assert!(result.is_err());

        // Test with wrong second segment
        let result = PermitKey::parse_key("test/wrong/00000000000000000001");
        assert!(result.is_err());

        // Test with invalid sequence number
        let result = PermitKey::parse_key("test/queue/not_a_number");
        assert!(result.is_err());
    }

    #[test]
    fn test_format_key_order_preserving() {
        for i in 0u64..500 {
            let n = i.wrapping_mul(11400714819323198485u64);
            let nkey = PermitKey::new("test", n).format_key();
            for j in 500u64..800 {
                let m = j.wrapping_mul(11400714819323198485u64);
                let mkey = PermitKey::new("test", m).format_key();

                if n < m {
                    assert!(nkey < mkey, "n={} should < m={}, {nkey} {mkey}", n, m);
                } else {
                    assert!(nkey > mkey, "n={} should > m={}, {nkey} {mkey}", n, m);
                }
            }
        }
    }
}
