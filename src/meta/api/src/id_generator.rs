//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use common_meta_kvapi::kv_api_key::check_segment;
use common_meta_kvapi::kv_api_key::check_segment_absent;
use common_meta_kvapi::kv_api_key::check_segment_present;
use common_meta_kvapi::KVApiKey;
use common_meta_kvapi::KVApiKeyError;

use crate::schema_api_keys::ID_GEN_DATABASE;
use crate::schema_api_keys::ID_GEN_TABLE;
use crate::share_api_keys::ID_GEN_SHARE;

pub(crate) const PREFIX_ID_GEN: &str = "__fd_id_gen";

/// Key for resource id generator
///
/// This is a special key for an application to generate unique id with KVApi.
/// Generating an id by updating a record in KVApi and retrieve the seq number.
/// A seq number is monotonically incremental in KVApi.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdGenerator {
    pub resource: String,
}

impl IdGenerator {
    /// Create a key for generating table id with KVApi
    pub fn table_id() -> Self {
        Self {
            resource: ID_GEN_TABLE.to_string(),
        }
    }

    /// Create a key for generating database id with KVApi
    pub fn database_id() -> Self {
        Self {
            resource: ID_GEN_DATABASE.to_string(),
        }
    }

    /// Create a key for generating share id with KVApi
    pub fn share_id() -> Self {
        Self {
            resource: ID_GEN_SHARE.to_string(),
        }
    }
}

impl KVApiKey for IdGenerator {
    const PREFIX: &'static str = PREFIX_ID_GEN;

    fn to_string_key(&self) -> String {
        format!("{}/{}", Self::PREFIX, self.resource)
    }

    fn from_str_key(s: &str) -> Result<Self, KVApiKeyError> {
        let mut elts = s.split('/');

        let prefix = check_segment_present(elts.next(), 0, s)?;
        check_segment(prefix, 0, Self::PREFIX)?;

        let resource = check_segment_present(elts.next(), 1, s)?;

        check_segment_absent(elts.next(), 2, s)?;

        Ok(IdGenerator {
            resource: resource.to_string(),
        })
    }
}

#[cfg(test)]
mod t {
    use common_meta_kvapi::KVApiKey;

    use crate::id_generator::IdGenerator;

    #[test]
    fn test_id_generator() -> anyhow::Result<()> {
        // Table id generator
        {
            let g = IdGenerator::table_id();
            let k = g.to_string_key();
            assert_eq!("__fd_id_gen/table_id", k);

            let t2 = IdGenerator::from_str_key(&k)?;
            assert_eq!(g, t2);
        }

        // Database id generator
        {
            let g = IdGenerator::database_id();
            let k = g.to_string_key();
            assert_eq!("__fd_id_gen/database_id", k);

            let t2 = IdGenerator::from_str_key(&k)?;
            assert_eq!(g, t2);
        }

        // Share id generator
        {
            let g = IdGenerator::share_id();
            let k = g.to_string_key();
            assert_eq!("__fd_id_gen/share_id", k);

            let t2 = IdGenerator::from_str_key(&k)?;
            assert_eq!(g, t2);
        }

        Ok(())
    }

    #[test]
    fn test_id_generator_from_key_error() -> anyhow::Result<()> {
        assert!(IdGenerator::from_str_key("__fd_id_gen").is_err());
        assert!(IdGenerator::from_str_key("__fd_id_gen/foo/bar").is_err());

        assert!(IdGenerator::from_str_key("__foo/table_id").is_err());
        Ok(())
    }
}
