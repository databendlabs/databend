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

use std::borrow::Borrow;
use std::ops::RangeBounds;

use common_meta_types::KVMeta;

use crate::sm_v002::marked::Marked;

/// Provide a key-value map API set, which is used to access state machine data.
pub(in crate::sm_v002) trait MapApi<K>
where K: Ord
{
    type V: Clone;

    /// Get an entry by key.
    fn get<Q>(&self, key: &Q) -> &Marked<Self::V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized;

    /// Set an entry and returns the old value and the new value.
    fn set(
        &mut self,
        key: K,
        value: Option<(Self::V, Option<KVMeta>)>,
    ) -> (Marked<Self::V>, Marked<Self::V>)
    where
        K: Ord;

    /// Iterate over a range of entries by keys.
    ///
    /// The returned iterator contains tombstone entries: [`Marked::TombStone`].
    fn range<'a, T: ?Sized, R>(
        &'a self,
        range: R,
    ) -> Box<dyn Iterator<Item = (&'a K, &'a Marked<Self::V>)> + 'a>
    where
        K: Clone + Borrow<T> + 'a,
        T: Ord,
        R: RangeBounds<T> + Clone;

    /// Update only the meta associated to an entry and keeps the value unchanged.
    /// If the entry does not exist, nothing is done.
    fn update_meta(&mut self, key: K, meta: Option<KVMeta>) -> (Marked<Self::V>, Marked<Self::V>) {
        // let f = key.borrow();

        let got = self.get(&key);
        if got.is_tomb_stone() {
            return (got.clone(), got.clone());
        }

        // Safe unwrap(), got is Normal
        let (v, _) = got.unpack().unwrap();

        self.set(key, Some((v.clone(), meta)))
    }

    /// Update only the value and keeps the meta unchanged.
    /// If the entry does not exist, create one.
    fn upsert_value(&mut self, key: K, value: Self::V) -> (Marked<Self::V>, Marked<Self::V>) {
        let got = self.get(&key);

        let meta = if let Some((_, meta)) = got.unpack() {
            meta
        } else {
            None
        };

        self.set(key, Some((value, meta.cloned())))
    }
}
