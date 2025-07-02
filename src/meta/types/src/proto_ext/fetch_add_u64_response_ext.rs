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

use crate::protobuf as pb;
use crate::protobuf::FetchAddU64Response;

impl pb::FetchAddU64Response {
    pub fn delta(&self) -> u64 {
        self.after.saturating_sub(self.before)
    }
}

impl Display for FetchAddU64Response {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FetchAddU64Response{{ key={} before=(seq={} {}), after=(seq={} {}), delta={} }}",
            self.key,
            self.before_seq,
            self.before,
            self.after_seq,
            self.after,
            self.delta()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protobuf::FetchAddU64Response;

    #[test]
    fn test_fetch_add_u64_response() {
        let resp = pb::FetchAddU64Response {
            key: "test_key".to_string(),
            before_seq: 10,
            before: 100,
            after_seq: 20,
            after: 200,
        };

        assert_eq!(resp.delta(), 100);
    }

    #[test]
    fn test_display_fetch_add_u64_response() {
        let resp = FetchAddU64Response {
            key: "k1".to_string(),
            before_seq: 1,
            before: 3,
            after_seq: 2,
            after: 4,
        };
        assert_eq!(
            resp.to_string(),
            "FetchAddU64Response{ key=k1 before=(seq=1 3), after=(seq=2 4), delta=1 }"
        );
    }
}
