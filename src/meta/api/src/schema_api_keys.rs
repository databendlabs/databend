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

use common_meta_app::schema::CountTablesKey;
use common_meta_app::schema::DBIdTableName;
use common_meta_app::schema::DatabaseId;
use common_meta_app::schema::DatabaseIdToName;
use common_meta_app::schema::DatabaseNameIdent;
use common_meta_app::schema::DbIdListKey;
use common_meta_app::schema::TableCopiedFileNameIdent;
use common_meta_app::schema::TableId;
use common_meta_app::schema::TableIdListKey;
use common_meta_app::schema::TableIdToName;
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
const PREFIX_TABLE_COUNT: &str = "__fd_table_count";
const PREFIX_DATABASE_ID_TO_NAME: &str = "__fd_database_id_to_name";
const PREFIX_TABLE_ID_TO_NAME: &str = "__fd_table_id_to_name";
const PREFIX_TABLE_COPIED_FILES: &str = "__fd_table_copied_files";

pub(crate) const ID_GEN_TABLE: &str = "table_id";
pub(crate) const ID_GEN_DATABASE: &str = "database_id";

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

/// "__fd_database_id_to_name/<db_id> -> DatabaseNameIdent"
impl KVApiKey for DatabaseIdToName {
    const PREFIX: &'static str = PREFIX_DATABASE_ID_TO_NAME;

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

        Ok(DatabaseIdToName { db_id })
    }
}

/// "__fd_table_id_to_name/<table_id> -> DBIdTableName"
impl KVApiKey for TableIdToName {
    const PREFIX: &'static str = PREFIX_TABLE_ID_TO_NAME;

    fn to_key(&self) -> String {
        format!("{}/{}", Self::PREFIX, self.table_id,)
    }

    fn from_key(s: &str) -> Result<Self, KVApiKeyError> {
        let mut elts = s.split('/');

        let prefix = check_segment_present(elts.next(), 0, s)?;
        check_segment(prefix, 0, Self::PREFIX)?;

        let table_id = check_segment_present(elts.next(), 1, s)?;
        let table_id = decode_id(table_id)?;

        check_segment_absent(elts.next(), 2, s)?;

        Ok(TableIdToName { table_id })
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

/// "__fd_table_by_id/<tb_id> -> TableMeta"
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

/// "__fd_table_count/<tenant>" -> <table_count>
impl KVApiKey for CountTablesKey {
    const PREFIX: &'static str = PREFIX_TABLE_COUNT;

    fn to_key(&self) -> String {
        format!("{}/{}", Self::PREFIX, self.tenant)
    }

    fn from_key(s: &str) -> Result<Self, KVApiKeyError> {
        let mut elts = s.split('/');

        let prefix = check_segment_present(elts.next(), 0, s)?;
        check_segment(prefix, 0, Self::PREFIX)?;

        let tenant = check_segment_present(elts.next(), 1, s)?;

        check_segment_absent(elts.next(), 2, s)?;

        let tenant = unescape(tenant)?;

        Ok(CountTablesKey { tenant })
    }
}

// __fd_table_stage_file/tenant/db_id/table_id/file_name -> TableCopiedFileInfo
impl KVApiKey for TableCopiedFileNameIdent {
    const PREFIX: &'static str = PREFIX_TABLE_COPIED_FILES;

    fn to_key(&self) -> String {
        format!(
            "{}/{}/{}/{}/{}",
            Self::PREFIX,
            self.tenant,
            self.db_id,
            self.table_id,
            self.file,
        )
    }

    fn from_key(s: &str) -> Result<Self, KVApiKeyError> {
        let elts: Vec<&str> = s.splitn(5, '/').collect();
        if elts.len() < 5 {
            return Err(KVApiKeyError::AtleastSegments {
                expect: 5,
                actual: elts.len(),
            });
        }
        let prefix = elts[0];
        check_segment(prefix, 0, Self::PREFIX)?;

        let tenant = unescape(elts[1])?;

        let db_id = decode_id(elts[2])?;
        let table_id = decode_id(elts[3])?;
        let file = unescape(elts[4])?;

        Ok(TableCopiedFileNameIdent {
            tenant,
            db_id,
            table_id,
            file,
        })
    }
}

#[cfg(test)]
mod tests {
    use common_meta_app::schema::TableCopiedFileNameIdent;

    use crate::kv_api_key::KVApiKey;
    use crate::KVApiKeyError;

    #[test]
    fn test_table_copied_file_name_ident_conversion() -> Result<(), KVApiKeyError> {
        // test with a key has a file has multi path
        {
            let name = TableCopiedFileNameIdent {
                tenant: "tenant".to_owned(),
                db_id: 1,
                table_id: 2,
                file: "/path/to/file".to_owned(),
            };

            let key = name.to_key();
            assert_eq!(
                key,
                format!(
                    "{}/{}/{}/{}/{}",
                    TableCopiedFileNameIdent::PREFIX,
                    name.tenant,
                    name.db_id,
                    name.table_id,
                    name.file,
                )
            );
            let from = TableCopiedFileNameIdent::from_key(&key)?;
            assert_eq!(from, name);
        }

        // test with a key has only 4 sub-path
        {
            let key = format!(
                "{}/{}/{}/{}",
                TableCopiedFileNameIdent::PREFIX,
                "tenant".to_owned(),
                1,
                2,
            );
            let res = TableCopiedFileNameIdent::from_key(&key);
            assert!(res.is_err());
            let err = res.unwrap_err();
            assert_eq!(err, KVApiKeyError::AtleastSegments {
                expect: 5,
                actual: 4,
            });
        }

        // test with a key has 5 sub-path but an empty file string
        {
            let key = format!(
                "{}/{}/{}/{}/{}",
                TableCopiedFileNameIdent::PREFIX,
                "tenant".to_owned(),
                1,
                2,
                ""
            );
            let res = TableCopiedFileNameIdent::from_key(&key)?;
            assert_eq!(res, TableCopiedFileNameIdent {
                tenant: "tenant".to_owned(),
                db_id: 1,
                table_id: 2,
                file: "".to_string(),
            });
        }
        Ok(())
    }
}
