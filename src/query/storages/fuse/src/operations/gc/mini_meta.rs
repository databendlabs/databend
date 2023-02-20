// Copyright 2023 Datafuse Labs.
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
//

use std::collections::HashSet;
use std::hash::Hasher;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common_exception::ErrorCode;
use opendal::ObjectMetadata;
use siphasher::sip128;
use siphasher::sip128::Hasher128;
use storages_common_table_meta::meta::FormatVersion;
use storages_common_table_meta::meta::SnapshotId;
use storages_common_table_meta::meta::TableSnapshot;

pub struct MiniMeta {
    pub path: String,
    pub size: usize,
    create_time: DateTime<Utc>,
}

impl From<ObjectMetadata> for MiniMeta {
    fn from(_value: ObjectMetadata) -> Self {
        todo!()
    }
}
// impl TryFrom<ObjectMetadata> for MiniMeta {
//    type Error = ErrorCode;
//
//    fn try_from(_value: ObjectMetadata) -> std::result::Result<Self, Self::Error> {
//        todo!()
//    }
//}

#[derive(Eq, Hash, PartialEq)]
pub struct LocationDigest {
    digest: u128,
}

impl<T: AsRef<str>> From<T> for LocationDigest {
    fn from(v: T) -> Self {
        let mut sip = sip128::SipHasher24::new();
        sip.write(v.as_ref().as_bytes());
        let digest = sip.finish128().into();
        LocationDigest { digest }
    }
}

pub struct MiniSnapshot {
    pub id: SnapshotId,
    pub timestamp: Option<DateTime<Utc>>,
    _version: FormatVersion,
    _prev_id: Option<SnapshotId>,
    pub segment_digests: HashSet<LocationDigest>,
    pub path: String,
}

impl MiniSnapshot {
    #[inline]
    pub fn snapshot_id(&self) -> SnapshotId {
        todo!()
    }
    #[inline]
    pub fn prev_snapshot_id(&self) -> Option<(SnapshotId, u64)> {
        todo!()
    }
}

impl<'a> From<(String, Arc<TableSnapshot>)> for MiniSnapshot {
    fn from(v: (String, Arc<TableSnapshot>)) -> Self {
        todo!()
    }
}
