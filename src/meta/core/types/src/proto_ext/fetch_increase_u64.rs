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

use crate::protobuf as pb;

impl fmt::Display for pb::FetchIncreaseU64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FetchIncreaseU64 key={} max_value={} delta={}",
            self.key, self.max_value, self.delta
        )?;
        if let Some(match_seq) = self.match_seq {
            write!(f, " match_seq: {}", match_seq)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_fetch_increase_u64() {
        let req = pb::FetchIncreaseU64 {
            key: "k1".to_string(),
            match_seq: None,
            delta: 1,
            max_value: 0,
        };
        assert_eq!(
            req.to_string(),
            "FetchIncreaseU64 key=k1 max_value=0 delta=1"
        );

        let req_with_seq = pb::FetchIncreaseU64 {
            key: "k1".to_string(),
            match_seq: Some(10),
            delta: 1,
            max_value: 100,
        };
        assert_eq!(
            req_with_seq.to_string(),
            "FetchIncreaseU64 key=k1 max_value=100 delta=1 match_seq: 10"
        );
    }
}
