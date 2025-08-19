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
use std::time::Duration;

use display_more::DisplayUnixTimeStampExt;

use crate::protobuf as pb;

impl pb::PutSequential {
    /// - `prefix` is the prefix of the key.
    ///   Examples `prefix`:
    ///   - `/foo/bar/` → `/foo/bar/000_000_000_000_001_200_123`
    ///   - `/foo/bar_` → `/foo/bar_000_000_000_000_001_200_123`
    ///
    /// - `sequence_key` is the key of the sequence.
    pub fn new(prefix: impl ToString, sequence_key: impl ToString, value: Vec<u8>) -> Self {
        Self {
            prefix: prefix.to_string(),
            sequence_key: sequence_key.to_string(),
            value,
            expires_at_ms: None,
            ttl_ms: None,
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl_ms = Some(ttl.as_millis() as u64);
        self
    }

    pub fn with_expires_at(mut self, expires_at_ms: u64) -> Self {
        self.expires_at_ms = Some(expires_at_ms);
        self
    }

    pub fn build_key(&self, n: u64) -> String {
        format!("{}{}", self.prefix, Self::format_seq_number(n))
    }

    /// Format number in Rust style: `1_000_000`, keep max length of u64, 21 digits.
    pub(crate) fn format_seq_number(n: u64) -> String {
        // separate each 3 digit with a '_'
        format!("{:021}", n)
            .chars()
            .enumerate()
            .fold(String::new(), |mut acc, (i, c)| {
                if i > 0 && i % 3 == 0 {
                    acc.push('_');
                }
                acc.push(c);
                acc
            })
    }
}

impl fmt::Display for pb::PutSequential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PutSequential{{ key={}{{next_seq({})}}",
            self.prefix, self.sequence_key
        )?;
        if let Some(expires_at_ms) = self.expires_at_ms {
            write!(
                f,
                ", expires_at_ms: {}",
                Duration::from_millis(expires_at_ms).display_unix_timestamp_short()
            )?;
        }
        if let Some(ttl_ms) = self.ttl_ms {
            write!(f, ", ttl: {:?}", Duration::from_millis(ttl_ms))?;
        }
        write!(f, " }}")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_put_sequential() {
        let req = pb::PutSequential {
            prefix: "k1/".to_string(),
            sequence_key: "__seq".to_string(),
            value: b"v1".to_vec(),
            expires_at_ms: None,
            ttl_ms: None,
        };
        assert_eq!(req.to_string(), "PutSequential{ key=k1/{next_seq(__seq)} }");

        let req = pb::PutSequential {
            prefix: "k1/".to_string(),
            sequence_key: "__seq".to_string(),
            value: b"v1".to_vec(),
            expires_at_ms: Some(1751431243640),
            ttl_ms: None,
        };
        assert_eq!(
            req.to_string(),
            "PutSequential{ key=k1/{next_seq(__seq)}, expires_at_ms: 2025-07-02T04:40:43.640 }"
        );

        let req = pb::PutSequential {
            prefix: "k1/".to_string(),
            sequence_key: "__seq".to_string(),
            value: b"v1".to_vec(),
            expires_at_ms: Some(1751431243640),
            ttl_ms: Some(1000),
        };
        assert_eq!(
            req.to_string(),
            "PutSequential{ key=k1/{next_seq(__seq)}, expires_at_ms: 2025-07-02T04:40:43.640, ttl: 1s }"
        );
    }

    #[test]
    fn test_format_aligned_num() {
        assert_eq!(
            pb::PutSequential::format_seq_number(u64::MAX),
            "018_446_744_073_709_551_615"
        );
        assert_eq!(
            pb::PutSequential::format_seq_number(10_100_000_000_001_200_000),
            "010_100_000_000_001_200_000"
        );
        assert_eq!(
            pb::PutSequential::format_seq_number(1_200_000),
            "000_000_000_000_001_200_000"
        );
        assert_eq!(
            pb::PutSequential::format_seq_number(120_000),
            "000_000_000_000_000_120_000"
        );
    }
}
