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

use databend_common_meta_types::InvalidReply;
use databend_common_meta_types::MetaAPIError;
use databend_common_meta_types::protobuf::RaftReply;
use log::debug;
use serde::de::DeserializeOwned;

/// Deserialize a `RaftReply` into a result type.
///
/// If `msg.data` is non-empty, deserialize it as `T`.
/// Otherwise, deserialize `msg.error` as `MetaAPIError`.
pub fn reply_to_api_result<T>(msg: RaftReply) -> Result<T, MetaAPIError>
where T: DeserializeOwned {
    if !msg.data.is_empty() {
        let res: T = serde_json::from_str(&msg.data)
            .map_err(|e| InvalidReply::new("can not decode RaftReply.data", &e))?;
        Ok(res)
    } else {
        let err: MetaAPIError = serde_json::from_str(&msg.error)
            .map_err(|e| InvalidReply::new("can not decode RaftReply.error", &e))?;

        Err(err)
    }
}

/// A struct that implements the Drop trait to log a message when it is dropped.
///
/// It can be used to track the lifetime. For example, use it a struct field of as a local variable of a closure.
pub struct DropDebug {
    message: String,
}

impl Drop for DropDebug {
    fn drop(&mut self) {
        debug!("DropDebug: {}", self.message);
    }
}

impl DropDebug {
    pub fn new(m: impl ToString) -> Self {
        Self {
            message: m.to_string(),
        }
    }
}
