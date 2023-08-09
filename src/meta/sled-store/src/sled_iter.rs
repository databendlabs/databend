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

use std::marker::PhantomData;

use sled::Db;
use sled::IVec;

use crate::get_sled_db;

pub struct TreeIter<T> {
    db: Db,
    tree_names: Vec<String>,
    _p: PhantomData<T>,
}

impl<T> Iterator for TreeIter<T> {
    type Item = Result<(String, ItemIter<T>), sled::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.tree_names.is_empty() {
            return None;
        }

        let name = self.tree_names.remove(0);
        let res = self.db.open_tree(&name);
        let tree = match res {
            Ok(x) => x,
            Err(e) => return Some(Err(e)),
        };

        let it = tree.iter();
        Some(Ok((name, ItemIter::<T> {
            it,
            _p: PhantomData,
        })))
    }
}

pub struct ItemIter<T> {
    it: sled::Iter,
    _p: PhantomData<T>,
}

impl Iterator for ItemIter<IVec> {
    type Item = Result<(IVec, IVec), sled::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.it.next();
        let res = match next {
            Some(x) => x,
            None => return None,
        };

        match res {
            Ok((k, v)) => Some(Ok((k, v))),
            Err(e) => Some(Err(e)),
        }
    }
}

/// Iterator that output key and value in `Vec<u8>`.
impl Iterator for ItemIter<Vec<u8>> {
    type Item = Result<(Vec<u8>, Vec<u8>), sled::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.it.next();
        let res = match next {
            Some(x) => x,
            None => return None,
        };

        match res {
            Ok((k, v)) => Some(Ok((k.to_vec(), v.to_vec()))),
            Err(e) => Some(Err(e)),
        }
    }
}

/// Iterate every record in every tree.
///
/// It returns an Iterator of record Iterators.
///
/// Usage:
/// ```ignore
/// for name_tree_res in iter::<IVec>() {
///     let (tree_name, item_iter) = name_tree_res?;
///     for item_res in item_iter {
///         let (k, v) = item_res?;
///         println!("{:?}: {:?}", k, v);
///     }
/// }
/// ```
pub fn iter<T>() -> TreeIter<T> {
    let db = get_sled_db();

    let mut tree_names = db
        .tree_names()
        .iter()
        .map(|x: &IVec| String::from_utf8(x.to_vec()).unwrap())
        .collect::<Vec<_>>();
    tree_names.sort();

    TreeIter {
        db,
        tree_names,
        _p: PhantomData,
    }
}
