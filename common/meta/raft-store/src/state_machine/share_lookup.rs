// Copyright 2022 Datafuse Labs.
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

use common_meta_sled_store::sled::IVec;
use common_meta_sled_store::SledOrderedSerde;
use common_meta_types::MetaStorageError;
use serde::Deserialize;
use serde::Serialize;

const DELIMITER: char = '/';

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ShareLookupKey {
    pub provider: String,
    pub share_name: String,
}

impl ShareLookupKey {
    pub fn new(provider: String, share_name: String) -> Self {
        ShareLookupKey {
            provider,
            share_name,
        }
    }
}

impl SledOrderedSerde for ShareLookupKey {
    fn ser(&self) -> Result<IVec, MetaStorageError> {
        let k = format!("{}{}{}", self.provider, DELIMITER, self.share_name);
        Ok(IVec::from(k.as_str()))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, MetaStorageError>
    where Self: Sized {
        let share_lookup_key = String::from_utf8(v.as_ref().to_vec())?;

        let share_lookup_key: Vec<&str> = share_lookup_key.split(DELIMITER as char).collect();

        Ok(ShareLookupKey {
            provider: share_lookup_key[0].to_string(),
            share_name: share_lookup_key[1].to_string(),
        })
    }
}

impl fmt::Display for ShareLookupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ShareLookupKey{}-{}", self.provider, self.share_name)
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ShareLookupValue(pub u64);

impl fmt::Display for ShareLookupValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
