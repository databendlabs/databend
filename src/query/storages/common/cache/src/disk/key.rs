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

use std::fmt;
use std::hash::Hasher;
use std::ops::Range;

use siphasher::sip128;
use siphasher::sip128::Hasher128;

use crate::CacheKind;

/// One unit of an object in the disk cache: the whole object, or one range of it. Ranges come
/// from the object's own metadata (a column chunk, an index column, a footer, a page) so the
/// same unit gets the same key across queries. Parquet files read from a stage are the
/// exception: their units are the ranges merged for the read, see `cached_range_read`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiskKey {
    path: String,
    range: Option<Range<u64>>,
}

impl DiskKey {
    pub fn whole(path: impl Into<String>) -> Self {
        DiskKey {
            path: path.into(),
            range: None,
        }
    }

    pub fn range(path: impl Into<String>, range: Range<u64>) -> Self {
        DiskKey {
            path: path.into(),
            range: Some(range),
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    /// The id of the object the unit belongs to: what `DiskCache::begin_change` drops every
    /// unit of when the object is rewritten or deleted.
    pub fn object_id(path: &str) -> String {
        hash(path)
    }

    /// `<kind>/<first 3 hex digits of the object id>/<object id>.<unit id>`: the entry's path
    /// under its pool root and its key in the pool's index. Ids are the hex SipHash 2-4 128 of
    /// the object path and of the key string. Units of one object share a prefix, so the pool
    /// can find them all by the object id.
    pub fn rel_path(&self, kind: CacheKind) -> String {
        let object = Self::object_id(&self.path);
        let unit = hash(&self.to_string());
        format!("{}/{}/{object}.{unit}", kind.name(), &object[0..3])
    }
}

/// The object id of an index key built by `DiskKey::rel_path`.
pub(crate) fn object_of(rel: &str) -> Option<&str> {
    let name = rel.rsplit('/').next()?;
    name.split_once('.').map(|(object, _)| object)
}

fn hash(s: &str) -> String {
    let mut sip = sip128::SipHasher24::new();
    sip.write(s.as_bytes());
    hex::encode(sip.finish128().as_bytes())
}

/// The key string: the path, with `#start-end` for a range.
impl fmt::Display for DiskKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.range {
            None => f.write_str(&self.path),
            Some(range) => write!(f, "{}#{}-{}", self.path, range.start, range.end),
        }
    }
}
