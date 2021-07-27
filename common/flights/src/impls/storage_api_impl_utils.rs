// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use tonic::metadata::MetadataMap;
use tonic::metadata::MetadataValue;

pub const META_KEY_DB_NAME: &str = "fq-db-name-bin";
pub const META_KEY_TBL_NAME: &str = "fq-tbl-name-bin";

pub fn put_meta(meta: &mut MetadataMap, db_name: &str, tbl_name: &str) {
    meta.insert_bin(
        META_KEY_DB_NAME,
        MetadataValue::from_bytes(db_name.as_bytes()),
    );
    meta.insert_bin(
        META_KEY_TBL_NAME,
        MetadataValue::from_bytes(tbl_name.as_bytes()),
    );
}

pub fn get_meta(meta: &MetadataMap) -> anyhow::Result<(String, String)> {
    fn fetch_string(
        meta: &MetadataMap,
        key: &str,
        error_msg: &'static str,
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
