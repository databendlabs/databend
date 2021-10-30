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

use crate::schemes::StorageScheme::AzureStorageBlob;
use crate::schemes::StorageScheme::LocalFs;
use crate::schemes::StorageScheme::S3;
use crate::StorageScheme;

#[test]
fn test_scheme_from_str() {
    let valid_schemes = vec![
        ("s3", S3),
        ("S3", S3),
        ("local", LocalFs),
        ("LOCAL", LocalFs),
        ("Disk", LocalFs),
        ("disk", LocalFs),
        ("azurestorageblob", AzureStorageBlob),
        ("AzureStorageBlob", AzureStorageBlob),
    ];
    valid_schemes.iter().for_each(|(str, scheme)| {
        let s = StorageScheme::from_str(str);
        assert!(s.is_ok());
        assert_eq!(&s.unwrap(), scheme)
    });

    let invalid_schemes = vec![("xxx")];
    invalid_schemes.iter().for_each(|str| {
        let s = StorageScheme::from_str(str);
        assert!(s.is_err());
        assert_eq!(
            s.unwrap_err().code(),
            ErrorCode::UnknownStorageSchemeName("").code()
        );
    })
}
