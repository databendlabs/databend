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
use std::io;
use std::io::Error;
use std::ops::RangeBounds;
use std::sync::Arc;

use futures_util::StreamExt;
use futures_util::TryStreamExt;
use log::info;
use map_api::mvcc;
use map_api::mvcc::Table;
use map_api::IOResultStream;
use seq_marked::InternalSeq;
use seq_marked::SeqMarked;

use crate::leveled_store::leveled_map::LeveledMapData;
use crate::leveled_store::types::Key;
use crate::leveled_store::types::Namespace;
use crate::leveled_store::types::Value;

#[async_trait::async_trait]
impl mvcc::ViewReadonly<Namespace, Key, Value> for Arc<LeveledMapData> {
    fn base_seq(&self) -> InternalSeq {
        let seq = self.with_sys_data(|sys_data| sys_data.curr_seq());
        InternalSeq::new(seq)
    }

    async fn get(&self, space: Namespace, key: Key) -> Result<SeqMarked<Value>, io::Error> {
        match space {
            Namespace::User => {
                let key = key.into_user();
                let got = self.compacted_view_get(key, *self.base_seq()).await?;
                Ok(got.map(Value::User))
            }
            Namespace::Expire => {
                let key = key.into_expire();
                let got = self.compacted_view_get(key, *self.base_seq()).await?;
                Ok(got.map(Value::Expire))
            }
        }
    }

    async fn range<R>(
        &self,
        space: Namespace,
        range: R,
    ) -> Result<IOResultStream<(Key, SeqMarked<Value>)>, io::Error>
    where
        R: RangeBounds<Key> + Send + Sync + Clone + 'static,
    {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();

        match space {
            Namespace::User => {
                let start = start.map(|k| k.into_user());
                let end = end.map(|k| k.into_user());

                let strm = self
                    .compacted_view_range((start, end), *self.base_seq())
                    .await?;

                Ok(strm
                    .map_ok(|(k, v)| (Key::User(k), v.map(Value::User)))
                    .boxed())
            }
            Namespace::Expire => {
                let start = start.map(|k| k.into_expire());
                let end = end.map(|k| k.into_expire());

                let strm = self
                    .compacted_view_range((start, end), *self.base_seq())
                    .await?;

                Ok(strm
                    .map_ok(|(k, v)| (Key::Expire(k), v.map(Value::Expire)))
                    .boxed())
            }
        }
    }
}

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
        let mut writable = self.writable.lock().unwrap();

        // user map

        let user_updates = changes.remove(&Namespace::User);

        if let Some(updates) = user_updates {
            let it = updates.inner.into_iter().map(|((k, seq_marked), v)| {
                ((k.into_user(), seq_marked), v.map(|x| x.into_user()))
            });

            writable.kv.apply_changes(updates.last_seq, it);
        }

        // expire map

        let expire_updates = changes.remove(&Namespace::Expire);

        if let Some(updates) = expire_updates {
            let it = updates.inner.into_iter().map(|((k, seq_marked), v)| {
                ((k.into_expire(), seq_marked), v.map(|x| x.into_expire()))
            });

            writable.expire.apply_changes(updates.last_seq, it);
        }

        // seq

        writable.with_sys_data(|sys_data| {
            sys_data.update_seq(*last_seq);
        });

        Ok(())
    }
}
