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

use databend_meta_client::kvapi;

pub(crate) const ID_GEN_GENERIC: &str = "generic";
pub(crate) const ID_GEN_TABLE: &str = "table_id";
pub(crate) const ID_GEN_DATABASE: &str = "database_id";
pub(crate) const ID_GEN_TABLE_LOCK: &str = "table_lock_id";
pub(crate) const ID_GEN_INDEX: &str = "index_id";
pub(crate) const ID_GEN_DICTIONARY: &str = "dictionary_id";

pub(crate) const ID_GEN_CATALOG: &str = "catalog_id";

pub(crate) const ID_GEN_SHARE: &str = "share_id";
pub(crate) const ID_GEN_SHARE_ENDPOINT: &str = "share_endpoint_id";

pub(crate) const ID_GEN_DATA_MASK: &str = "data_mask";
pub(crate) const ID_GEN_ROW_POLICY: &str = "row_access";
pub(crate) const ID_GEN_BACKGROUND_JOB: &str = "background_job";

pub(crate) const ID_GEN_PROCEDURE: &str = "procedure_id";

/// Key for resource id generator
///
/// This is a special key for an application to generate unique id with kvapi::KVApi.
/// Generating an id by updating a record in kvapi::KVApi and retrieve the seq number.
/// A seq number is monotonically incremental in kvapi::KVApi.
#[derive(Debug, Clone, PartialEq, Eq, kvapi::StructKey)]
#[structkey(prefix = "__fd_id_gen")]
pub struct IdGenerator {
    #[codec(raw)]
    pub resource: String,
}

impl IdGenerator {
    pub fn new(resource: &'static str) -> Self {
        Self {
            resource: resource.to_string(),
        }
    }

    /// Create a key for generating generic id
    pub fn generic() -> Self {
        Self::new(ID_GEN_GENERIC)
    }

    /// Create a key for generating table id with kvapi::KVApi
    pub fn table_id() -> Self {
        Self::new(ID_GEN_TABLE)
    }

    /// Create a key for generating database id with kvapi::KVApi
    pub fn database_id() -> Self {
        Self::new(ID_GEN_DATABASE)
    }

    /// Create a key for generating dictionary id with kvapi::KVApi
    pub fn dictionary_id() -> Self {
        Self::new(ID_GEN_DICTIONARY)
    }

    /// Create a key for generating share id with kvapi::KVApi
    pub fn share_id() -> Self {
        Self::new(ID_GEN_SHARE)
    }

    pub fn share_endpoint_id() -> Self {
        Self::new(ID_GEN_SHARE_ENDPOINT)
    }

    pub fn index_id() -> Self {
        Self::new(ID_GEN_INDEX)
    }

    pub fn data_mask_id() -> Self {
        Self::new(ID_GEN_DATA_MASK)
    }

    pub fn row_access_id() -> Self {
        Self::new(ID_GEN_ROW_POLICY)
    }

    pub fn table_lock_id() -> Self {
        Self::new(ID_GEN_TABLE_LOCK)
    }

    pub fn background_job_id() -> Self {
        Self::new(ID_GEN_BACKGROUND_JOB)
    }

    /// Create a key for generating catalog id with kvapi::KVApi
    pub fn catalog_id() -> Self {
        Self::new(ID_GEN_CATALOG)
    }

    /// Create a key for generating procedure id with kvapi::KVApi
    pub fn procedure_id() -> Self {
        Self::new(ID_GEN_PROCEDURE)
    }
}

impl kvapi::Key for IdGenerator {
    type ValueType = IdGeneratorValue;

    fn parent(&self) -> Option<String> {
        None
    }
}

#[derive(Debug)]
pub struct IdGeneratorValue;

impl kvapi::Value for IdGeneratorValue {
    type KeyType = IdGenerator;

    fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
        []
    }
}

#[cfg(test)]
mod t {

    use databend_meta_client::kvapi::StructKey;
    use databend_meta_client::kvapi::testing::assert_round_trip;

    use crate::id_generator::IdGenerator;

    #[test]
    fn test_id_generator() {
        assert_round_trip(IdGenerator::table_id(), "__fd_id_gen/table_id");
        assert_round_trip(IdGenerator::database_id(), "__fd_id_gen/database_id");
        assert_round_trip(IdGenerator::share_id(), "__fd_id_gen/share_id");
        assert_round_trip(
            IdGenerator::share_endpoint_id(),
            "__fd_id_gen/share_endpoint_id",
        );
        assert_round_trip(IdGenerator::index_id(), "__fd_id_gen/index_id");
        assert_round_trip(IdGenerator::data_mask_id(), "__fd_id_gen/data_mask");
        assert_round_trip(IdGenerator::table_lock_id(), "__fd_id_gen/table_lock_id");
        assert_round_trip(IdGenerator::procedure_id(), "__fd_id_gen/procedure_id");
    }

    #[test]
    fn test_id_generator_from_key_error() {
        assert!(IdGenerator::from_str_key("__fd_id_gen").is_err());
        assert!(IdGenerator::from_str_key("__fd_id_gen/foo/bar").is_err());
        assert!(IdGenerator::from_str_key("__foo/table_id").is_err());
    }
}
