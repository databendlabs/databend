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
use std::ops::RangeBounds;

use futures_util::StreamExt;
use futures_util::TryStreamExt;
use map_api::IOResultStream;
use map_api::mvcc;
use seq_marked::SeqMarked;

use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::types::Key;
use crate::leveled_store::types::Namespace;
use crate::leveled_store::types::Value;

#[async_trait::async_trait]
impl mvcc::SeqBoundedRange<Namespace, Key, Value> for LeveledMap {
    async fn range<R>(
        &self,
        space: Namespace,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(Key, SeqMarked<Value>)>, Error>
    where
        R: RangeBounds<Key> + Send + Sync + Clone + 'static,
    {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();

        match space {
            Namespace::User => {
                let start = start.map(|k| k.into_user());
                let end = end.map(|k| k.into_user());

                let strm =
                    mvcc::ScopedSeqBoundedRange::range(self, (start, end), snapshot_seq).await?;

                Ok(strm
                    .map_ok(|(k, v)| (Key::User(k), v.map(Value::User)))
                    .boxed())
            }
            Namespace::Expire => {
                let start = start.map(|k| k.into_expire());
                let end = end.map(|k| k.into_expire());

                let strm =
                    mvcc::ScopedSeqBoundedRange::range(self, (start, end), snapshot_seq).await?;

                Ok(strm
                    .map_ok(|(k, v)| (Key::Expire(k), v.map(Value::Expire)))
                    .boxed())
            }
        }
    }
}
