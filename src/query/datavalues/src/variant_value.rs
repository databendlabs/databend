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

use std::hash::Hash;

use serde_json::Value;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct VariantValue(pub Value);

impl From<Value> for VariantValue {
    fn from(val: Value) -> Self {
        VariantValue(val)
    }
}

impl AsRef<Value> for VariantValue {
    fn as_ref(&self) -> &Value {
        &self.0
    }
}

#[allow(clippy::derived_hash_with_manual_eq)]
impl Hash for VariantValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let v = self.as_ref().to_string();
        let u = v.as_bytes();
        Hash::hash(&u, state);
    }
}
