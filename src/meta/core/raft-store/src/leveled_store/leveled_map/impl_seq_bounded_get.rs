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

use std::io::Error;

use map_api::mvcc;
use seq_marked::SeqMarked;

use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::types::Key;
use crate::leveled_store::types::Namespace;
use crate::leveled_store::types::Value;

#[async_trait::async_trait]
impl mvcc::SeqBoundedGet<Namespace, Key, Value> for LeveledMap {
    async fn get(
        &self,
        space: Namespace,
        key: Key,
        snapshot_seq: u64,
    ) -> Result<SeqMarked<Value>, Error> {
        match space {
            Namespace::User => {
                let key = key.into_user();
                let got = mvcc::ScopedSeqBoundedGet::get(self, key, snapshot_seq).await?;
                Ok(got.map(Value::User))
            }
            Namespace::Expire => {
                let key = key.into_expire();
                let got = mvcc::ScopedSeqBoundedGet::get(self, key, snapshot_seq).await?;
                Ok(got.map(Value::Expire))
            }
        }
    }
}
