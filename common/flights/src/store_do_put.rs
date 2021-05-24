// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use tonic::metadata::MetadataMap;
use tonic::metadata::MetadataValue;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct AppendResult {
    pub summary: Summary,
    pub parts: Vec<PartitionInfo>,
    pub session_id: String,
    pub tx_id: String
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct Summary {
    pub rows: usize,
    pub wire_bytes: usize,
    pub disk_bytes: usize
}
impl Summary {
    pub(crate) fn increase(&mut self, rows: usize, wire_bytes: usize, disk_bytes: usize) {
        self.rows += rows;
        self.wire_bytes += wire_bytes;
        self.disk_bytes += disk_bytes;
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct PartitionInfo {
    pub rows: usize,
    pub cols: usize,
    pub wire_bytes: usize,
    pub disk_bytes: usize,
    pub location: String
}

impl AppendResult {
    pub fn append_part(
        &mut self,
        location: &str,
        rows: usize,
        cols: usize,
        wire_bytes: usize,
        disk_bytes: usize
    ) {
        let part = PartitionInfo {
            rows,
            cols,
            wire_bytes,
            disk_bytes,
            location: location.to_string()
        };
        self.parts.push(part);
        self.summary.increase(rows, wire_bytes, disk_bytes);
    }
}

pub const META_KEY_DB_NAME: &str = "fq-db-name-bin";
pub const META_KEY_TBL_NAME: &str = "fq-tbl-name-bin";

pub fn set_do_put_meta(meta: &mut MetadataMap, db_name: &str, tbl_name: &str) {
    meta.insert_bin(
        META_KEY_DB_NAME,
        MetadataValue::from_bytes(db_name.as_bytes())
    );
    meta.insert_bin(
        META_KEY_TBL_NAME,
        MetadataValue::from_bytes(tbl_name.as_bytes())
    );
}

pub fn get_do_put_meta(meta: &MetadataMap) -> anyhow::Result<(String, String)> {
    fn fetch_string(
        meta: &MetadataMap,
        key: &str,
        error_msg: &'static str
    ) -> anyhow::Result<String> {
        meta.get_bin(key)
            .and_then(|v| v.to_bytes().ok())
            .and_then(|b| String::from_utf8(b.to_vec()).ok())
            .ok_or_else(|| anyhow::anyhow!(error_msg))
    }
    let db_name = fetch_string(meta, META_KEY_DB_NAME, "invalid db_name meta data")?;
    let tbl_name = fetch_string(meta, META_KEY_TBL_NAME, "invalid tbl_name meta data")?;
    Ok((db_name, tbl_name))
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_get_set_meta() {
        let mut meta = MetadataMap::new();
        let test_db = "test_db";
        let test_tbl = "test_tbl";
        set_do_put_meta(&mut meta, test_db, test_tbl);
        let (db, tbl) = get_do_put_meta(&meta).unwrap();
        assert_eq!(test_db, db);
        assert_eq!(test_tbl, tbl);
    }
}
