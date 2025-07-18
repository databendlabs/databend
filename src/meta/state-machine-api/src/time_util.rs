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

use std::time::Duration;

/// Timestamp in **seconds or milliseconds** since Unix epoch (1970-01-01).
///
/// The interpretation depends on the magnitude of the value:
/// - Values > `100_000_000_000`: treated as milliseconds since epoch
/// - Values ≤ `100_000_000_000`: treated as seconds since epoch
///
/// Examples:
/// - `100_000_000_001` → `1973-03-03 17:46:40` (milliseconds)
/// - `100_000_000_000` → `5138-11-16 17:46:40` (seconds)
///
/// Valid ranges:
/// - Seconds: `1970-01-01 00:00:00` to `5138-11-16 17:46:40`
/// - Milliseconds: `1973-03-03 17:46:40` onwards
///
/// To avoid overflow issues, use timestamps between `1973-03-03 17:46:40`
/// and `5138-11-16 17:46:40` for reliable behavior across both interpretations.
pub fn flexible_timestamp_to_duration(timestamp: u64) -> Duration {
    if timestamp > 100_000_000_000 {
        // Milliseconds since epoch
        Duration::from_millis(timestamp)
    } else {
        // Seconds since epoch
        Duration::from_secs(timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adaptable_timestamp_to_duration() {
        assert_eq!(
            flexible_timestamp_to_duration(100_000_000_001),
            Duration::from_millis(100_000_000_001)
        );
        assert_eq!(
            flexible_timestamp_to_duration(100_000_000_000),
            Duration::from_secs(100_000_000_000)
        );
    }
}
