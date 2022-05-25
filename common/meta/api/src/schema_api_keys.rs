// Copyright 2021 Datafuse Labs.
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

//! Defines structured keys used by SchemaApi

use std::fmt::Debug;

use common_meta_types::DBIdTableName;
use common_meta_types::DatabaseId;
use common_meta_types::DatabaseNameIdent;
use common_meta_types::DbIdListKey;
use common_meta_types::TableId;
use common_meta_types::TableIdListKey;
use kv_api_key::check_segment;
use kv_api_key::check_segment_absent;
use kv_api_key::check_segment_present;
use kv_api_key::decode_id;
use kv_api_key::escape;
use kv_api_key::unescape;

use crate::kv_api_key;
use crate::KVApiKey;
use crate::KVApiKeyError;

const PREFIX_DATABASE: &str = "__fd_database";
const PREFIX_DATABASE_BY_ID: &str = "__fd_database_by_id";
const PREFIX_DB_ID_LIST: &str = "__fd_db_id_list";
const PREFIX_TABLE: &str = "__fd_table";
const PREFIX_TABLE_BY_ID: &str = "__fd_table_by_id";
const PREFIX_TABLE_ID_LIST: &str = "__fd_table_id_list";
const PREFIX_ID_GEN: &str = "__fd_id_gen";

/// Key for database id generator
#[derive(Debug)]
pub struct DatabaseIdGen {}

/// Key for table id generator
#[derive(Debug)]
pub struct TableIdGen {}

/// __fd_database/<tenant>/<db_name> -> <db_id>
impl KVApiKey for DatabaseNameIdent {
    const PREFIX: &'static str = PREFIX_DATABASE;

    fn to_key(&self) -> String {
        format!(
            "{}/{}/{}",
            Self::PREFIX,
            escape(&self.tenant),
            escape(&self.db_name),
        )
    }

    fn from_key(s: &str) -> Result<Self, KVApiKeyError> {
        let mut elts = s.split('/');

        let prefix = check_segment_present(elts.next(), 0, s)?;
        check_segment(prefix, 0, Self::PREFIX)?;

        let tenant = check_segment_present(elts.next(), 1, s)?;

        let db_name = check_segment_present(elts.next(), 2, s)?;

        check_segment_absent(elts.next(), 3, s)?;

        let tenant = unescape(tenant)?;
        let db_name = unescape(db_name)?;

        Ok(DatabaseNameIdent { tenant, db_name })
    }
}

/// "__fd_database_by_id/<db_id>"
impl KVApiKey for DatabaseId {
    const PREFIX: &'static str = PREFIX_DATABASE_BY_ID;

    fn to_key(&self) -> String {
        format!("{}/{}", Self::PREFIX, self.db_id,)
    }

    fn from_key(s: &str) -> Result<Self, KVApiKeyError> {
        let mut elts = s.split('/');

        let prefix = check_segment_present(elts.next(), 0, s)?;
        check_segment(prefix, 0, Self::PREFIX)?;

        let db_id = check_segment_present(elts.next(), 1, s)?;
        let db_id = decode_id(db_id)?;

        check_segment_absent(elts.next(), 2, s)?;

        Ok(DatabaseId { db_id })
    }
}

/// "_fd_db_id_list/<tenant>/<db_name> -> db_id_list"
impl KVApiKey for DbIdListKey {
    const PREFIX: &'static str = PREFIX_DB_ID_LIST;

    fn to_key(&self) -> String {
        format!(
            "{}/{}/{}",
            Self::PREFIX,
            escape(&self.tenant),
            escape(&self.db_name),
        )
    }

    fn from_key(s: &str) -> Result<Self, KVApiKeyError> {
        let mut elts = s.split('/');

        let prefix = check_segment_present(elts.next(), 0, s)?;
        check_segment(prefix, 0, Self::PREFIX)?;

        let tenant = check_segment_present(elts.next(), 1, s)?;

        let db_name = check_segment_present(elts.next(), 2, s)?;

        check_segment_absent(elts.next(), 3, s)?;

        let tenant = unescape(tenant)?;
        let db_name = unescape(db_name)?;

        Ok(DbIdListKey { tenant, db_name })
    }
}

/// "__fd_table/<db_id>/<tb_name>"
impl KVApiKey for DBIdTableName {
    const PREFIX: &'static str = PREFIX_TABLE;

    fn to_key(&self) -> String {
        format!(
            "{}/{}/{}",
            Self::PREFIX,
            self.db_id,
            escape(&self.table_name),
        )
    }

    fn from_key(s: &str) -> Result<Self, KVApiKeyError> {
        let mut elts = s.split('/');

        let prefix = check_segment_present(elts.next(), 0, s)?;
        check_segment(prefix, 0, Self::PREFIX)?;

        let db_id = check_segment_present(elts.next(), 1, s)?;
        let db_id = decode_id(db_id)?;

        let tb_name = check_segment_present(elts.next(), 2, s)?;
        let tb_name = unescape(tb_name)?;

        check_segment_absent(elts.next(), 3, s)?;

        Ok(DBIdTableName {
            db_id,
            table_name: tb_name,
        })
    }
}

/// "__fd_table_by_id/<tb_id>"
impl KVApiKey for TableId {
    const PREFIX: &'static str = PREFIX_TABLE_BY_ID;

    fn to_key(&self) -> String {
        format!("{}/{}", Self::PREFIX, self.table_id,)
    }

    fn from_key(s: &str) -> Result<Self, KVApiKeyError> {
        let mut elts = s.split('/');

        let prefix = check_segment_present(elts.next(), 0, s)?;
        check_segment(prefix, 0, Self::PREFIX)?;

        let tb_id = check_segment_present(elts.next(), 1, s)?;
        let tb_id = decode_id(tb_id)?;

        check_segment_absent(elts.next(), 2, s)?;

        Ok(TableId { table_id: tb_id })
    }
}

/// "_fd_table_id_list/<db_id>/<tb_name> -> id_list"
impl KVApiKey for TableIdListKey {
    const PREFIX: &'static str = PREFIX_TABLE_ID_LIST;

    fn to_key(&self) -> String {
        format!(
            "{}/{}/{}",
            Self::PREFIX,
            self.db_id,
            escape(&self.table_name),
        )
    }

    fn from_key(s: &str) -> Result<Self, KVApiKeyError> {
        let mut elts = s.split('/');

        let prefix = check_segment_present(elts.next(), 0, s)?;
        check_segment(prefix, 0, Self::PREFIX)?;

        let db_id = check_segment_present(elts.next(), 1, s)?;
        let db_id = decode_id(db_id)?;

        let tb_name = check_segment_present(elts.next(), 2, s)?;
        let tb_name = unescape(tb_name)?;

        check_segment_absent(elts.next(), 3, s)?;

        Ok(TableIdListKey {
            db_id,
            table_name: tb_name,
        })
    }
}

impl KVApiKey for DatabaseIdGen {
    const PREFIX: &'static str = PREFIX_ID_GEN;

    fn to_key(&self) -> String {
        format!("{}/database_id", Self::PREFIX)
    }

    fn from_key(_s: &str) -> Result<Self, KVApiKeyError> {
        unimplemented!()
    }
}

impl KVApiKey for TableIdGen {
    const PREFIX: &'static str = PREFIX_ID_GEN;

    fn to_key(&self) -> String {
        format!("{}/table_id", Self::PREFIX)
    }

    fn from_key(_s: &str) -> Result<Self, KVApiKeyError> {
        unimplemented!()
    }
}
