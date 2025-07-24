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

use crate::TxnDeleteRequest;

impl TxnDeleteRequest {
    pub fn new(key: impl ToString, prev_value: bool, match_seq: Option<u64>) -> Self {
        Self {
            key: key.to_string(),
            prev_value,
            match_seq,
        }
    }
}

impl Display for TxnDeleteRequest {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Delete key={}", self.key)
    }
}
