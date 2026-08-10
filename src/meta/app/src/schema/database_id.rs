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
use std::fmt::Display;
use std::fmt::Formatter;

use databend_meta_client::kvapi;
use derive_more::Deref;
use derive_more::DerefMut;

use crate::KeyUnknownBuilder;
use crate::app_error::UnknownDatabaseId;

/// `__fd_database_by_id/<db_id>`
#[derive(
    Clone, Debug, Copy, Default, Eq, PartialEq, PartialOrd, Ord, Deref, DerefMut, kvapi::StructKey,
)]
#[structkey(prefix = "__fd_database_by_id")]
pub struct DatabaseId {
    pub db_id: u64,
}

impl DatabaseId {
    pub fn new(db_id: u64) -> Self {
        DatabaseId { db_id }
    }
}

impl From<u64> for DatabaseId {
    fn from(db_id: u64) -> Self {
        DatabaseId { db_id }
    }
}

impl Display for DatabaseId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.db_id)
    }
}

impl KeyUnknownBuilder for DatabaseId {
    type UnknownError = UnknownDatabaseId;

    fn unknown_error(&self, ctx: impl Display) -> Self::UnknownError {
        UnknownDatabaseId::new(self.db_id, ctx.to_string())
    }
}

mod kvapi_key_impl {
    use databend_meta_client::kvapi;

    use crate::schema::DatabaseId;
    use crate::schema::DatabaseMeta;

    impl kvapi::Key for DatabaseId {
        type ValueType = DatabaseMeta;
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_client::kvapi::testing::assert_round_trip;

    use super::DatabaseId;
    use crate::KeyUnknownBuilder;

    #[test]
    fn test_database_id_key_format() {
        assert_round_trip(DatabaseId::new(3), "__fd_database_by_id/3");
    }

    #[test]
    fn test_database_id_unknown_builder() {
        assert_eq!(
            DatabaseId::new(3).unknown_error("ctx").to_string(),
            "UnknownDatabaseId: `3` while `ctx`"
        );
    }
}
