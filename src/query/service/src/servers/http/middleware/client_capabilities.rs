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

use std::collections::HashSet;

/// client should carry header X-DATABEND-CLIENT-CAPS in each request.
/// the value should be capability name separated by ';'
#[derive(Debug, Default, Clone, Copy)]
pub struct ClientCapabilities {
    // client use one of session_header/session_cookie to carry session id and related info.
    // client connection write back X-DATABEND-SESSION as it is for each request.
    pub session_header: bool,
    // client connection use a global Cookie store
    pub session_cookie: bool,
}

impl ClientCapabilities {
    pub fn parse(header_value: &str) -> Self {
        let cap_set: HashSet<String> = header_value
            .split(';')
            .map(|cap| cap.trim())
            .filter(|cap| !cap.is_empty())
            .map(|cap| cap.to_lowercase())
            .collect();
        ClientCapabilities {
            session_header: cap_set.contains("session_header"),
            session_cookie: cap_set.contains("session_cookie"),
        }
    }
}
