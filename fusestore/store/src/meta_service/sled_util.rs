// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::path::Path;

use common_exception::ErrorCode;
use common_exception::ToErrorCode;

/// Same as sled::open except it returns common_exception::Result<sled::Db>.
/// `sled::open()` open an existent one, otherwise it creates a new one.
pub fn sled_open<P: AsRef<Path>>(p: P) -> common_exception::Result<sled::Db> {
    let db = sled::open(&p).map_err_to_code(ErrorCode::MetaStoreDamaged, || {
        format!("opening sled db: {}", p.as_ref().display())
    })?;

    Ok(db)
}
