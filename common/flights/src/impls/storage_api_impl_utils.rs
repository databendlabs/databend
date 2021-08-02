// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use tonic::metadata::{MetadataMap, Binary};
use tonic::metadata::MetadataValue;
use common_exception::{Result, ErrorCode};

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

pub fn get_meta(meta: &MetadataMap) -> Result<(String, String)> {
    fn deserialize_meta(value: &MetadataValue<Binary>, error_msg: &'static str) -> Result<String> {
        match value.to_bytes() {
            Ok(bytes) => Ok(String::from_utf8(bytes.to_vec())?),
            Err(error) => Err(ErrorCode::InvalidMetaBinaryFormat(
                format!("{}, cause {}", error_msg, error)
            ))
        }
    }

    fn fetch_string(meta: &MetadataMap, key: &str, error_msg: &'static str) -> Result<String> {
        match meta.get_bin(key) {
            None => Err(ErrorCode::UnknownKey(format!("Unknown meta key {}", key))),
            Some(meta_binary) => deserialize_meta(meta_binary, error_msg)
        }
    }

    let db_name = fetch_string(meta, META_KEY_DB_NAME, "invalid db_name meta data")?;
    let tbl_name = fetch_string(meta, META_KEY_TBL_NAME, "invalid tbl_name meta data")?;
    Ok((db_name, tbl_name))
}
