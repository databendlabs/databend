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

use std::str::FromStr;

use common_exception::ErrorCode;

use self::StorageScheme::AzureStorageBlob;
use self::StorageScheme::LocalFs;
use self::StorageScheme::S3;

#[derive(Clone, Debug, PartialEq)]
pub enum StorageScheme {
    LocalFs,
    S3,
    AzureStorageBlob,
}

impl FromStr for StorageScheme {
    type Err = common_exception::ErrorCode;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_uppercase();
        match s.as_str() {
            "S3" => Ok(S3),
            "LOCAL" | "DISK" => Ok(LocalFs),
            "AZURESTORAGEBLOB" => Ok(AzureStorageBlob),
            _ => Err(ErrorCode::UnknownStorageSchemeName(format!(
                "unknown storage scheme [{}], supported schemes are S3 | Disk",
                s
            ))),
        }
    }
}
