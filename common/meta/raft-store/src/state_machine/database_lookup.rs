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

const DB_LOOKUP_KEY_DELIMITER: char = '/';

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DatabaseLookupKey {
    tenant: String,
    delimiter: char,
    database_name: String,
}

impl DatabaseLookupKey {
    pub fn new(tenant: String, database_name: String) -> Self {
        DatabaseLookupKey {
            tenant,
            delimiter: DB_LOOKUP_KEY_DELIMITER,
            database_name,
        }
    }

    pub fn get_database_name(&self) -> String {
        self.database_name.clone()
    }
}

impl SledOrderedSerde for DatabaseLookupKey {
    fn ser(&self) -> Result<IVec, MetaStorageError> {
        let k = format!("{}{}{}", self.tenant, self.delimiter, self.database_name);
        Ok(IVec::from(k.as_str()))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, MetaStorageError>
    where Self: Sized {
        let db_lookup_key = String::from_utf8(v.as_ref().to_vec())?;

        let db_lookup_key: Vec<&str> = db_lookup_key
            .split(DB_LOOKUP_KEY_DELIMITER as char)
            .collect();

        Ok(DatabaseLookupKey {
            tenant: db_lookup_key[0].to_string(),
            delimiter: DB_LOOKUP_KEY_DELIMITER,
            database_name: db_lookup_key[1].to_string(),
        })
    }
}

impl fmt::Display for DatabaseLookupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "DatabaseLookupKey_{}{}{}",
            self.tenant, self.delimiter, self.database_name
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct DatabaseLookupValue(pub u64);

impl fmt::Display for DatabaseLookupValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
