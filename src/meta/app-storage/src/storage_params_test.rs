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

use std::str::FromStr;

use crate::storage_params::S3StorageClass;

#[test]
fn test_s3_storage_class_from_str_valid() {
    // Test valid standard case
    assert_eq!(
        S3StorageClass::from_str("STANDARD").unwrap(),
        S3StorageClass::Standard
    );
    assert_eq!(
        S3StorageClass::from_str("standard").unwrap(),
        S3StorageClass::Standard
    );
    assert_eq!(
        S3StorageClass::from_str("Standard").unwrap(),
        S3StorageClass::Standard
    );

    // Test valid intelligent tiering case
    assert_eq!(
        S3StorageClass::from_str("INTELLIGENT_TIERING").unwrap(),
        S3StorageClass::IntelligentTiering
    );
    assert_eq!(
        S3StorageClass::from_str("intelligent_tiering").unwrap(),
        S3StorageClass::IntelligentTiering
    );
    assert_eq!(
        S3StorageClass::from_str("Intelligent_Tiering").unwrap(),
        S3StorageClass::IntelligentTiering
    );
}

#[test]
fn test_s3_storage_class_from_str_invalid() {
    // Test invalid cases
    let invalid_cases = vec![
        "",
        "invalid",
        "GLACIER",
        "DEEP_ARCHIVE",
        "STANDARD_IA",
        "ONEZONE_IA",
        "REDUCED_REDUNDANCY",
        "STANDARD_INTELLIGENT_TIERING", // typo
        "INTELLIGENT-TIERING",          // wrong separator
    ];

    for invalid_case in invalid_cases {
        let result = S3StorageClass::from_str(invalid_case);
        assert!(
            result.is_err(),
            "Expected error for input: {}",
            invalid_case
        );
        assert!(
            result.unwrap_err().contains("Unsupported S3 storage class"),
            "Error message should contain 'Unsupported S3 storage class' for input: {}",
            invalid_case
        );
    }
}

#[test]
fn test_s3_storage_class_display() {
    assert_eq!(S3StorageClass::Standard.to_string(), "STANDARD");
    assert_eq!(
        S3StorageClass::IntelligentTiering.to_string(),
        "INTELLIGENT_TIERING"
    );
}

#[test]
fn test_s3_storage_class_default() {
    assert_eq!(S3StorageClass::default(), S3StorageClass::Standard);
}

#[test]
fn test_s3_storage_class_round_trip() {
    // Test that parsing and formatting are consistent
    let classes = vec![S3StorageClass::Standard, S3StorageClass::IntelligentTiering];

    for class in classes {
        let serialized = class.to_string();
        let deserialized = S3StorageClass::from_str(&serialized).unwrap();
        assert_eq!(class, deserialized);
    }
}
