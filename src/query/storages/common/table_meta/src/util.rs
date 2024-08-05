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

use std::path::Path;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

pub const V5_OBJET_KEY_PREFIX: char = 'g';

// Extracts the UUID part from the object key.
// For example, given a path like:
//   bucket/root/115/122/_b/g0191114d30fd78b89fae8e5c88327725_v2.parquet
//   bucket/root/115/122/_b/0191114d30fd78b89fae8e5c88327725_v2.parquet
// The function should return: 0191114d30fd78b89fae8e5c88327725
pub fn try_extract_uuid_str_from_path(path: &str) -> Result<&str> {
    if let Some(file_stem) = Path::new(path).file_stem() {
        let file_name = file_stem
            .to_str()
            .unwrap() // path is always valid utf8 string
            .split('_')
            .collect::<Vec<&str>>();
        let uuid = trim_v5_object_prefix(file_name[0]);
        Ok(uuid)
    } else {
        Err(ErrorCode::StorageOther(format!(
            "Illegal object key, no file stem found: {}",
            path
        )))
    }
}

#[inline]
pub fn trim_v5_object_prefix(key: &str) -> &str {
    key.strip_prefix(V5_OBJET_KEY_PREFIX).unwrap_or(key)
}
