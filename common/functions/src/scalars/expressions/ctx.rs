// Copyright 2021 Datafuse Labs.
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

use std::collections::BTreeMap;

use common_exception::ErrorCode;
use common_exception::Result;

#[derive(Debug, Clone)]
pub struct EvalContext {
    pub factor: i64,
    pub error: Option<ErrorCode>,
    /// A map of key-value pairs containing additional custom meta data.
    pub metadata: Option<BTreeMap<String, String>>,
}

impl Default for EvalContext {
    fn default() -> Self {
        Self {
            factor: 1,
            error: None,
            metadata: None,
        }
    }
}

impl EvalContext {
    pub fn new(
        factor: i64,
        error: Option<ErrorCode>,
        metadata: Option<BTreeMap<String, String>>,
    ) -> Self {
        Self {
            factor,
            error,
            metadata,
        }
    }

    pub fn get_meta_value(&self, key: String) -> Result<String> {
        self.metadata
            .clone()
            .ok_or_else(|| ErrorCode::UnknownException("metadata is not set".to_string()))?
            .get(&key)
            .cloned()
            .ok_or_else(|| ErrorCode::UnknownException(format!("metadata key {} is not set", key)))
    }

    pub fn set_error(&mut self, e: ErrorCode) {
        if self.error.is_none() {
            self.error = Some(e);
        }
    }
}
