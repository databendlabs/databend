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

use crate::seq_value::SeqV;

/// An update event for a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Update<M, V = Vec<u8>> {
    pub key: String,
    pub before: Option<SeqV<M, V>>,
    pub after: Option<SeqV<M, V>>,
}

impl<M, V> Update<M, V> {
    pub fn new(key: String, before: Option<SeqV<M, V>>, after: Option<SeqV<M, V>>) -> Self {
        Self { key, before, after }
    }

    pub fn is_delete(&self) -> bool {
        self.after.is_none()
    }
}
