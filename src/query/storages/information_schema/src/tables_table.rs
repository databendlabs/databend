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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_storages_view::view_table::ViewTable;
use databend_common_storages_view::view_table::QUERY;

pub struct TablesTable {}

impl TablesTable {
    // desc  information_schema.tables;
    // +-----------------+--------------------------------------------------------------------+------+-----+---------+-------+
    // | Field           | Type                                                               | Null | Key | Default | Extra |
    // +-----------------+--------------------------------------------------------------------+------+-----+---------+-------+
    // | TABLE_CATALOG   | varchar(64)                                                        | NO   |     | NULL    |       |
    // | TABLE_SCHEMA    | varchar(64)                                                        | NO   |     | NULL    |       |
    // | TABLE_NAME      | varchar(64)                                                        | NO   |     | NULL    |       |
    // | TABLE_TYPE      | enum('BASE TABLE','VIEW','SYSTEM VIEW')                            | NO   |     | NULL    |       |
    // | ENGINE          | varchar(64)                                                        | YES  |     | NULL    |       |
    // | VERSION         | int                                                                | YES  |     | NULL    |       |
    // | ROW_FORMAT      | enum('Fixed','Dynamic','Compressed','Redundant','Compact','Paged') | YES  |     | NULL    |       |
    // | TABLE_ROWS      | bigint unsigned                                                    | YES  |     | NULL    |       |
    // | AVG_ROW_LENGTH  | bigint unsigned                                                    | YES  |     | NULL    |       |
    // | DATA_LENGTH     | bigint unsigned                                                    | YES  |     | NULL    |       |
    // | MAX_DATA_LENGTH | bigint unsigned                                                    | YES  |     | NULL    |       |
    // | INDEX_LENGTH    | bigint unsigned                                                    | YES  |     | NULL    |       |
    // | DATA_FREE       | bigint unsigned                                                    | YES  |     | NULL    |       |
    // | AUTO_INCREMENT  | bigint unsigned                                                    | YES  |     | NULL    |       |
    // | CREATE_TIME     | timestamp                                                          | NO   |     | NULL    |       |
    // | UPDATE_TIME     | datetime                                                           | YES  |     | NULL    |       |
    // | CHECK_TIME      | datetime                                                           | YES  |     | NULL    |       |
    // | TABLE_COLLATION | varchar(64)                                                        | YES  |     | NULL    |       |
    // | CHECKSUM        | bigint                                                             | YES  |     | NULL    |       |
    // | CREATE_OPTIONS  | varchar(256)                                                       | YES  |     | NULL    |       |
    // | TABLE_COMMENT   | text                                                               | YES  |     | NULL    |       |
    // +-----------------+--------------------------------------------------------------------+------+-----+---------+-------+
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let query = "SELECT * FROM (SELECT
            database AS table_catalog,
            database AS table_schema,
            name AS table_name,
            'BASE TABLE' AS table_type,
            engine AS engine,
            created_on AS create_time,
            dropped_on AS drop_time,
            data_size AS data_length,
            index_size AS index_length,
            num_rows AS table_rows,
            NULL AS auto_increment,
            NULL AS table_collation,
            NULL AS data_free,
            comment AS table_comment
        FROM system.tables union
        SELECT
            database AS table_catalog,
            database AS table_schema,
            name AS table_name,
            'VIEW' AS table_type,
            engine AS engine,
            created_on AS create_time,
            dropped_on AS drop_time,
            0 AS data_length,
            0 AS index_length,
            0 AS table_rows,
            NULL AS auto_increment,
            NULL AS table_collation,
            NULL AS data_free,
            comment AS table_comment
        FROM system.views) ORDER BY table_schema;";

        let mut options = BTreeMap::new();
        options.insert(QUERY.to_string(), query.to_string());
        let table_info = TableInfo {
            desc: "'information_schema'.'tables'".to_string(),
            name: "tables".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                options,
                engine: "VIEW".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        ViewTable::create(table_info)
    }
}
