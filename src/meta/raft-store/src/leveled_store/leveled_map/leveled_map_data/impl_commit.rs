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

use std::collections::BTreeMap;
use std::io::Error;
use std::sync::Arc;

use log::info;
use map_api::mvcc;
use map_api::mvcc::Table;
use seq_marked::InternalSeq;

use crate::leveled_store::leveled_map::leveled_map_data::LeveledMapData;
use crate::leveled_store::types::Key;
use crate::leveled_store::types::Namespace;
use crate::leveled_store::types::Value;

#[async_trait::async_trait]
impl mvcc::Commit<Namespace, Key, Value> for Arc<LeveledMapData> {
    async fn commit(
        &mut self,
        last_seq: InternalSeq,
        mut changes: BTreeMap<Namespace, Table<Key, Value>>,
    ) -> Result<(), Error> {
        info!(
            "Committing changes to leveled map data: last_seq={}, changes = {:?}",
            last_seq, changes
        );
        let mut inner = self.inner.lock().unwrap();

        // user map

        let user_updates = changes.remove(&Namespace::User);

        if let Some(updates) = user_updates {
            let it = updates.inner.into_iter().map(|((k, seq_marked), v)| {
                ((k.into_user(), seq_marked), v.map(|x| x.into_user()))
            });

            inner.writable.kv.apply_changes(updates.last_seq, it);
        }

        // expire map

        let expire_updates = changes.remove(&Namespace::Expire);

        if let Some(updates) = expire_updates {
            let it = updates.inner.into_iter().map(|((k, seq_marked), v)| {
                ((k.into_expire(), seq_marked), v.map(|x| x.into_expire()))
            });

            inner.writable.expire.apply_changes(updates.last_seq, it);
        }

        // seq

        inner.writable.with_sys_data(|sys_data| {
            sys_data.update_seq(*last_seq);
        });

        Ok(())
    }
}
