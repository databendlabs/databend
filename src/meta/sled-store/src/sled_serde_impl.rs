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

use databend_common_meta_types::seq_value::SeqV;
use databend_common_meta_types::Entry;
use databend_common_meta_types::EntryPayload;
use databend_common_meta_types::LogId;
use databend_common_meta_types::Membership;
use databend_common_meta_types::Node;
use databend_common_meta_types::SeqNum;
use databend_common_meta_types::SnapshotMeta;
use databend_common_meta_types::StoredMembership;
use databend_common_meta_types::Vote;

use crate::SledBytesError;
use crate::SledSerde;

impl SledSerde for String {
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized {
        let s = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}

impl<U> SledSerde for SeqV<U>
where U: serde::Serialize + serde::de::DeserializeOwned
{
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized {
        let s = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}

impl SledSerde for SeqNum {
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized {
        let s = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}

impl SledSerde for LogId {
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized {
        let s: LogId = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}

impl SledSerde for Vote {
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized {
        let s: Vote = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}

impl SledSerde for Membership {
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized {
        let s: Membership = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}

impl SledSerde for StoredMembership {
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized {
        let s: StoredMembership = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}

impl SledSerde for EntryPayload {
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized {
        let s: EntryPayload = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}

impl SledSerde for Entry {
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized {
        let s: Entry = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}

impl SledSerde for SnapshotMeta {
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized {
        let s: SnapshotMeta = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}

impl SledSerde for Node {
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized {
        let s = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}
