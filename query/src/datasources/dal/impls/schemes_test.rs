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

use crate::datasources::dal::StorageScheme;
use crate::datasources::dal::StorageScheme::Disk;
use crate::datasources::dal::StorageScheme::S3;

#[test]
fn test_storage_scheme_converter() {
    let valid_scheme_names = vec![("s3", S3), ("S3", S3), ("Disk", Disk), ("DISK", Disk)];

    valid_scheme_names.iter().for_each(|(v, s)| {
        let scheme = StorageScheme::try_from(*v);
        assert!(scheme.is_ok());
        assert_eq!(&scheme.unwrap(), s, "scheme name being tested {}", v);
    });

    let invalid_scheme_names = vec!["not exist"];

    invalid_scheme_names.iter().for_each(|v| {
        let scheme = StorageScheme::try_from(*v);
        assert!(scheme.is_err());
    });
}
