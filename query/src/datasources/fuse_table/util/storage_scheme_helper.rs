//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_dal::StorageScheme;
use common_exception::ErrorCode;
use common_exception::Result;

pub type TableStorageScheme = StorageScheme;

pub fn parse_storage_scheme(value: Option<&String>) -> Result<StorageScheme> {
    if let Some(v) = value {
        let v = v.to_uppercase();
        match v.as_str() {
            "LOCAL_FS" | "LOCAL" => Ok(TableStorageScheme::LocalFs),
            "FuseDfs" => Ok(TableStorageScheme::FuseDfs),
            "S3" => Ok(TableStorageScheme::S3),
            _ => Err(ErrorCode::IllegalSchema(format!("unknown scheme {}", v))),
        }
    } else {
        Err(ErrorCode::IllegalSchema(
            "invalid table option for Fuse Table, no Storage Scheme provided",
        ))
    }
}
