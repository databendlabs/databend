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
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_basic::view_table::ViewTable;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_system::generate_catalog_meta;

const CHARACTER_SETS: &[(&str, &str, &str, u8)] = &[
    ("big5", "big5_chinese_ci", "Big5 Traditional Chinese", 2),
    ("dec8", "dec8_swedish_ci", "DEC West European", 1),
    ("cp850", "cp850_general_ci", "DOS West European", 1),
    ("hp8", "hp8_english_ci", "HP West European", 1),
    ("koi8r", "koi8r_general_ci", "KOI8-R Relcom Russian", 1),
    ("latin1", "latin1_swedish_ci", "cp1252 West European", 1),
    (
        "latin2",
        "latin2_general_ci",
        "ISO 8859-2 Central European",
        1,
    ),
    ("swe7", "swe7_swedish_ci", "7bit Swedish", 1),
    ("ascii", "ascii_general_ci", "US ASCII", 1),
    ("ujis", "ujis_japanese_ci", "EUC-JP Japanese", 3),
    ("sjis", "sjis_japanese_ci", "Shift-JIS Japanese", 2),
    ("hebrew", "hebrew_general_ci", "ISO 8859-8 Hebrew", 1),
    ("tis620", "tis620_thai_ci", "TIS620 Thai", 1),
    ("euckr", "euckr_korean_ci", "EUC-KR Korean", 2),
    ("koi8u", "koi8u_general_ci", "KOI8-U Ukrainian", 1),
    (
        "gb2312",
        "gb2312_chinese_ci",
        "GB2312 Simplified Chinese",
        2,
    ),
    ("greek", "greek_general_ci", "ISO 8859-7 Greek", 1),
    ("cp1250", "cp1250_general_ci", "Windows Central European", 1),
    ("gbk", "gbk_chinese_ci", "GBK Simplified Chinese", 2),
    ("latin5", "latin5_turkish_ci", "ISO 8859-9 Turkish", 1),
    ("armscii8", "armscii8_general_ci", "ARMSCII-8 Armenian", 1),
    ("utf8mb3", "utf8mb3_general_ci", "UTF-8 Unicode", 3),
    ("ucs2", "ucs2_general_ci", "UCS-2 Unicode", 2),
    ("cp866", "cp866_general_ci", "DOS Russian", 1),
    (
        "keybcs2",
        "keybcs2_general_ci",
        "DOS Kamenicky Czech-Slovak",
        1,
    ),
    ("macce", "macce_general_ci", "Mac Central European", 1),
    ("macroman", "macroman_general_ci", "Mac West European", 1),
    ("cp852", "cp852_general_ci", "DOS Central European", 1),
    ("latin7", "latin7_general_ci", "ISO 8859-13 Baltic", 1),
    ("cp1251", "cp1251_general_ci", "Windows Cyrillic", 1),
    ("utf16", "utf16_general_ci", "UTF-16 Unicode", 4),
    ("utf16le", "utf16le_general_ci", "UTF-16LE Unicode", 4),
    ("cp1256", "cp1256_general_ci", "Windows Arabic", 1),
    ("cp1257", "cp1257_general_ci", "Windows Baltic", 1),
    ("utf32", "utf32_general_ci", "UTF-32 Unicode", 4),
    ("binary", "binary", "Binary pseudo charset", 1),
    ("geostd8", "geostd8_general_ci", "GEOSTD8 Georgian", 1),
    ("cp932", "cp932_japanese_ci", "SJIS for Windows Japanese", 2),
    (
        "eucjpms",
        "eucjpms_japanese_ci",
        "UJIS for Windows Japanese",
        3,
    ),
    (
        "gb18030",
        "gb18030_chinese_ci",
        "China National Standard GB18030",
        4,
    ),
    ("utf8mb4", "utf8mb4_0900_ai_ci", "UTF-8 Unicode", 4),
];

pub struct CharacterSetsTable {}

impl CharacterSetsTable {
    pub fn create(table_id: u64, ctl_name: &str) -> Arc<dyn Table> {
        let rows_sql = CHARACTER_SETS
            .iter()
            .map(|(name, collate, description, maxlen)| {
                format!("('{}', '{}', '{}', {})", name, collate, description, maxlen)
            })
            .collect::<Vec<_>>()
            .join(",\n            ");

        let query = format!(
            "SELECT
                character_set_name,
                default_collate_name,
                description,
                maxlen
            FROM (VALUES
            {rows}
            ) AS t(character_set_name, default_collate_name, description, maxlen)",
            rows = rows_sql
        );

        let mut options = BTreeMap::new();
        options.insert(QUERY.to_string(), query);
        let table_info = TableInfo {
            desc: "'information_schema'.'character_sets'".to_string(),
            name: "character_sets".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                options,
                engine: "VIEW".to_string(),
                ..Default::default()
            },
            catalog_info: Arc::new(CatalogInfo {
                name_ident: CatalogNameIdent::new(Tenant::new_literal("dummy"), ctl_name).into(),
                meta: generate_catalog_meta(ctl_name),
                ..Default::default()
            }),
            ..Default::default()
        };

        ViewTable::create(table_info)
    }
}
