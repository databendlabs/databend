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

use databend_common_storage::dummy_delete_layer::DummyDeleteLayer;
use opendal::services;
use opendal::ErrorKind;
use opendal::Operator;

fn build_s3_operator(with_dummy_delete: bool) -> Operator {
    let bucket = std::env::var("S3_TEST_BUCKET").expect("S3_TEST_BUCKET must be set");
    let region = std::env::var("S3_TEST_REGION").expect("S3_TEST_REGION must be set");
    let access_key_id = std::env::var("S3_TEST_ACCESS_KEY_ID").expect("S3_TEST_ACCESS_KEY_ID must be set");
    let secret_access_key = std::env::var("S3_TEST_SECRET_ACCESS_KEY").expect("S3_TEST_SECRET_ACCESS_KEY must be set");

    let builder = services::S3::default()
        .bucket(&bucket)
        .root("_dummy_delete_test")
        .region(&region)
        .access_key_id(&access_key_id)
        .secret_access_key(&secret_access_key);

    let op = Operator::new(builder).unwrap().finish();
    if with_dummy_delete {
        op.layer(DummyDeleteLayer)
    } else {
        op
    }
}

#[tokio::test]
async fn test_dummy_delete_layer_on_s3() {
    let op = build_s3_operator(true);
    let test_path = "test_dummy_delete.txt";
    let test_content = b"hello dummy delete layer";

    // 1. PUT — should succeed
    let put_result = op.write(test_path, test_content.to_vec()).await;
    assert!(put_result.is_ok(), "PUT should succeed, got: {:?}", put_result.err());
    println!("PUT: OK");

    // 2. GET — should succeed, content should match
    let data = op.read(test_path).await;
    assert!(data.is_ok(), "GET should succeed, got: {:?}", data.err());
    assert_eq!(data.unwrap().to_vec(), test_content.to_vec(), "GET content mismatch");
    println!("GET: OK");

    // 3. LIST — should succeed, should contain test file
    let entries = op.list("/").await;
    assert!(entries.is_ok(), "LIST should succeed, got: {:?}", entries.err());
    let entries = entries.unwrap();
    let found = entries.iter().any(|e| e.path().contains("test_dummy_delete"));
    assert!(found, "LIST should contain test file, entries: {:?}", entries.iter().map(|e| e.path()).collect::<Vec<_>>());
    println!("LIST: OK");

    // 4. DELETE — should fail with Unsupported
    let del_result = op.delete(test_path).await;
    assert!(del_result.is_err(), "DELETE should fail, but it succeeded");
    let err = del_result.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unsupported, "DELETE error should be Unsupported, got: {:?}", err.kind());
    println!("DELETE correctly rejected: {}", err);

    // 5. GET again — file should still exist
    let data = op.read(test_path).await;
    assert!(data.is_ok(), "GET after rejected DELETE should succeed, got: {:?}", data.err());
    assert_eq!(data.unwrap().to_vec(), test_content.to_vec());
    println!("GET after DELETE: OK — file still exists");

    // 6. Cleanup with a raw operator (no DummyDeleteLayer)
    let raw_op = build_s3_operator(false);
    let cleanup = raw_op.delete(test_path).await;
    assert!(cleanup.is_ok(), "Cleanup DELETE should succeed, got: {:?}", cleanup.err());
    println!("CLEANUP: OK");

    println!("\nAll tests passed!");
}