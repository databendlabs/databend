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

use map_api::mvcc;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;

use crate::applier::applier_data::ApplierData;
use crate::leveled_store::types::Key;
use crate::leveled_store::types::Namespace;
use crate::leveled_store::types::Value;

#[async_trait::async_trait]
impl mvcc::ScopedView<ExpireKey, String> for ApplierData {
    fn set(&mut self, key: ExpireKey, value: Option<String>) -> SeqMarked<()> {
        self.view.set(
            Namespace::Expire,
            Key::Expire(key),
            value.map(Value::Expire),
        )
    }
}
