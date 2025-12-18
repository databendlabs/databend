// Copyright 2025 Datafuse Labs.
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

//! Tests for ErrorCodeResultExt trait functionality

use databend_common_exception::ErrorCode;
use databend_common_exception::ErrorCodeResultExt;
use databend_common_exception::Result;
use databend_common_exception::error_code_groups;

fn create_unknown_database_error() -> Result<String> {
    Err(ErrorCode::UnknownDatabase("test_db"))
}

fn create_unknown_table_error() -> Result<String> {
    Err(ErrorCode::UnknownTable("test_table"))
}

fn create_unknown_sequence_error() -> Result<String> {
    Err(ErrorCode::UnknownSequence("test_sequence"))
}

fn create_internal_error() -> Result<String> {
    Err(ErrorCode::Internal("internal error"))
}

fn create_success() -> Result<String> {
    Ok("success".to_string())
}

#[test]
fn test_or_error_codes_with_matching_error() {
    let result = create_unknown_database_error().or_error_codes(&[ErrorCode::UNKNOWN_DATABASE]);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), None);
}

#[test]
fn test_or_error_codes_with_non_matching_error() {
    let result = create_internal_error().or_error_codes(&[ErrorCode::UNKNOWN_DATABASE]);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::INTERNAL);
}

#[test]
fn test_or_error_codes_with_success() {
    let result = create_success().or_error_codes(&[ErrorCode::UNKNOWN_DATABASE]);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some("success".to_string()));
}

#[test]
fn test_or_error_codes_with_multiple_codes() {
    let codes = &[ErrorCode::UNKNOWN_DATABASE, ErrorCode::UNKNOWN_TABLE];

    let result1 = create_unknown_database_error().or_error_codes(codes);
    assert!(result1.is_ok());
    assert_eq!(result1.unwrap(), None);

    let result2 = create_unknown_table_error().or_error_codes(codes);
    assert!(result2.is_ok());
    assert_eq!(result2.unwrap(), None);

    let result3 = create_internal_error().or_error_codes(codes);
    assert!(result3.is_err());
    assert_eq!(result3.unwrap_err().code(), ErrorCode::INTERNAL);
}

#[test]
fn test_or_unknown_database() {
    let result1 = create_unknown_database_error().or_unknown_database();
    assert!(result1.is_ok());
    assert_eq!(result1.unwrap(), None);

    let result2 = create_unknown_table_error().or_unknown_database();
    assert!(result2.is_err());
    assert_eq!(result2.unwrap_err().code(), ErrorCode::UNKNOWN_TABLE);

    let result3 = create_success().or_unknown_database();
    assert!(result3.is_ok());
    assert_eq!(result3.unwrap(), Some("success".to_string()));
}

#[test]
fn test_or_unknown_table() {
    let result1 = create_unknown_table_error().or_unknown_table();
    assert!(result1.is_ok());
    assert_eq!(result1.unwrap(), None);

    let result2 = create_unknown_database_error().or_unknown_table();
    assert!(result2.is_err());
    assert_eq!(result2.unwrap_err().code(), ErrorCode::UNKNOWN_DATABASE);

    let result3 = create_success().or_unknown_table();
    assert!(result3.is_ok());
    assert_eq!(result3.unwrap(), Some("success".to_string()));
}

#[test]
fn test_or_unknown_resource() {
    // Test with database error (should convert to None)
    let result1 = create_unknown_database_error().or_unknown_resource();
    assert!(result1.is_ok());
    assert_eq!(result1.unwrap(), None);

    // Test with table error (should convert to None)
    let result2 = create_unknown_table_error().or_unknown_resource();
    assert!(result2.is_ok());
    assert_eq!(result2.unwrap(), None);

    // Test with sequence error (should convert to None)
    let result3 = create_unknown_sequence_error().or_unknown_resource();
    assert!(result3.is_ok());
    assert_eq!(result3.unwrap(), None);

    // Test with non-resource error (should propagate)
    let result4 = create_internal_error().or_unknown_resource();
    assert!(result4.is_err());
    assert_eq!(result4.unwrap_err().code(), ErrorCode::INTERNAL);

    // Test with success
    let result5 = create_success().or_unknown_resource();
    assert!(result5.is_ok());
    assert_eq!(result5.unwrap(), Some("success".to_string()));
}

#[test]
fn test_error_code_groups() {
    // Verify that the error code groups contain expected values
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_DATABASE));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_TABLE));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_SEQUENCE));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_CATALOG));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_VIEW));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_COLUMN));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_DICTIONARY));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_ROLE));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_NETWORK_POLICY));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_CONNECTION));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_FILE_FORMAT));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_USER));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_PASSWORD_POLICY));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_STAGE));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_WORKLOAD));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_VARIABLE));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_SESSION));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_QUERY));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_ROW_ACCESS_POLICY));
    assert!(error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_DATAMASK));

    // Verify that it doesn't contain unrelated error codes
    assert!(!error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::INTERNAL));
    assert!(!error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_DATABASE_ID));
    assert!(!error_code_groups::UNKNOWN_RESOURCE.contains(&ErrorCode::UNKNOWN_TABLE_ID));
}

#[test]
fn test_method_chaining() {
    // Test that methods can be chained with other Result operations
    let result = create_success().or_unknown_database().map(|opt| match opt {
        Some(val) => val.to_uppercase(),
        None => "DEFAULT".to_string(),
    });

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "SUCCESS");

    let result2 = create_unknown_database_error()
        .or_unknown_database()
        .map(|opt| match opt {
            Some(val) => val.to_uppercase(),
            None => "DEFAULT".to_string(),
        });

    assert!(result2.is_ok());
    assert_eq!(result2.unwrap(), "DEFAULT");
}

#[test]
fn test_precise_error_semantics() {
    // Verify that UNKNOWN_DATABASE and UNKNOWN_DATABASE_ID are treated differently
    let unknown_db_id_error: Result<String> = Err(ErrorCode::UnknownDatabaseId("test_id"));

    // or_unknown_database should NOT convert UNKNOWN_DATABASE_ID to None
    let result = unknown_db_id_error.or_unknown_database();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::UNKNOWN_DATABASE_ID);

    // Similarly for table IDs
    let unknown_table_id_error: Result<String> = Err(ErrorCode::UnknownTableId("test_id"));
    let result2 = unknown_table_id_error.or_unknown_table();
    assert!(result2.is_err());
    assert_eq!(result2.unwrap_err().code(), ErrorCode::UNKNOWN_TABLE_ID);
}
