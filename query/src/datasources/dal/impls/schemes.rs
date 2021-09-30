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

use std::convert::TryFrom;

use common_exception::ErrorCode;

use crate::datasources::dal::impls::schemes::StorageScheme::Disk;
use crate::datasources::dal::impls::schemes::StorageScheme::S3;

#[derive(Clone, Debug, PartialEq)]
pub enum StorageScheme {
    Disk,
    S3,
}

impl TryFrom<&str> for StorageScheme {
    type Error = common_exception::ErrorCode;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let v = value.to_uppercase();
        match v.as_str() {
            "DISK" => Ok(Disk),
            "S3" => Ok(S3),
            _ => Err(ErrorCode::UnknownStorageScheme(format!(
                "unknown storage scheme {}",
                value
            ))),
        }
    }
}
