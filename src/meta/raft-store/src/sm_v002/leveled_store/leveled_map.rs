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
use std::fmt;
use std::ops::RangeBounds;

use common_meta_types::KVMeta;
use itertools::Itertools;

use crate::sm_v002::leveled_store::map_api::MapApi;
use crate::sm_v002::marked::Marked;

/// A map-like data structure that constructs its final state from multiple levels.
pub(in crate::sm_v002) trait MultiLevelMap<K>
where K: Ord
{
    /// The API to access the data at one level.
    type API: MapApi<K> + 'static;

    /// Returns the data associated to this level.
    fn data<'a>(&'a self) -> &Self::API
    where Self::API: 'a;

    /// Returns the mutable reference of the data associated to this level.
    fn data_mut<'a>(&'a mut self) -> &mut Self::API
    where Self::API: 'a;

    /// Return a readonly reference to the base level this level is built on top of.
    fn base(&self) -> Option<&Self>;
}

impl<L, K> MapApi<K> for L
where
    K: Ord + fmt::Debug,
    L: MultiLevelMap<K>,
{
    type V = <<L as MultiLevelMap<K>>::API as MapApi<K>>::V;

    fn get<Q>(&self, key: &Q) -> &Marked<Self::V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let api = self.data();
        let got = api.get(key);

        if got.is_not_found() {
            if let Some(base) = self.base() {
                return base.get(key);
            }
        }
        got
    }

    fn set(
        &mut self,
        key: K,
        value: Option<(Self::V, Option<KVMeta>)>,
    ) -> (Marked<Self::V>, Marked<Self::V>)
    where
        K: Ord,
    {
        // Get from this level or the base level.
        let prev = self.get(&key).clone();

        // No such entry at all, no need to create a tombstone for delete
        if prev.is_not_found() && value.is_none() {
            return (prev, Marked::new_tomb_stone(0));
        }

        // The data is a single level map and the returned `_prev` is only from that level.
        let (_prev, inserted) = self.data_mut().set(key, value);
        (prev, inserted)
    }

    fn range<'a, T: ?Sized, R>(
        &'a self,
        range: R,
    ) -> Box<dyn Iterator<Item = (&K, &Marked<Self::V>)> + 'a>
    where
        K: 'a,
        K: Borrow<T> + Clone,
        T: Ord,
        R: RangeBounds<T> + Clone,
    {
        let a = self.data().range(range.clone());

        if let Some(base) = self.base() {
            let b = base.range(range);
            let it = a
                // Put entries with the same key together, smaller internal-seq first
                .merge_by(b, |(k1, v1), (k2, v2)| {
                    assert_ne!((k1, v1.internal_seq()), (k2, v2.internal_seq()));
                    (k1, v1.internal_seq()) <= (k2, v2.internal_seq())
                })
                // Merge entries with the same key, keep the one with larger internal-seq
                .coalesce(|(k1, v1), (k2, v2)| {
                    if k1 == k2 {
                        Ok((k1, Marked::max(v1, v2)))
                    } else {
                        Err(((k1, v1), (k2, v2)))
                    }
                });
            Box::new(it)
        } else {
            Box::new(a)
        }
    }
}
