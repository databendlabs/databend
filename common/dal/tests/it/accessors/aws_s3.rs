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

use std::io::SeekFrom;

use common_base::tokio;
use common_dal::DataAccessor;
use common_dal::S3;
use common_exception::ErrorCode;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use rusoto_core::ByteStream;
use rusoto_core::Region;
use rusoto_s3::PutObjectRequest;
use rusoto_s3::S3Client;
use rusoto_s3::S3 as RusotoS3;

/// This test suite is not CI ready, and intended to be
/// used in local dev box, with
///
/// - properly configured S3 env vars
///    pls take a look at `scripts/ci/ci-run-stateful-tests-standalone-s3.sh`
/// - or mocked s3 service, like minio
///
/// To enable this suite, features `ut_mock_s3` should be enabled:
///
/// e.g. cargo test --features ut_mock_s3 -p common-dal --test it
struct TestFixture {
    region_name: String,
    endpoint_url: String,
    bucket_name: String,
    key_id: String,
    access_key: String,
    test_obj_key: String,
    test_obj_content: Vec<u8>,
}

impl TestFixture {
    fn new() -> Self {
        Self::with_test_obj(0, "")
    }

    fn with_test_obj(test_obj_size: usize, test_obj_key: impl Into<String>) -> Self {
        let random_bytes: Vec<u8> = (0..test_obj_size).map(|_| rand::random::<u8>()).collect();

        let endpoint_url = std::env::var("S3_STORAGE_ENDPOINT_URL").unwrap();
        let bucket_name = std::env::var("S3_STORAGE_BUCKET").unwrap();
        let region_name = std::env::var("S3_STORAGE_REGION").unwrap();
        let key_id = std::env::var("S3_STORAGE_ACCESS_KEY_ID").unwrap();
        let access_key = std::env::var("S3_STORAGE_SECRET_ACCESS_KEY").unwrap();

        Self {
            region_name,
            endpoint_url,
            bucket_name,
            key_id,
            access_key,
            test_obj_key: test_obj_key.into(),
            test_obj_content: random_bytes,
        }
    }

    fn region(&self) -> Region {
        Region::Custom {
            name: self.region_name.clone(),
            endpoint: self.endpoint_url.clone(),
        }
    }

    fn data_accessor(&self) -> common_exception::Result<S3> {
        S3::try_create(
            self.region_name.as_str(),
            self.endpoint_url.as_str(),
            self.bucket_name.as_str(),
            &self.key_id,
            &self.access_key,
            false,
        )
    }
}

impl TestFixture {
    async fn gen_test_obj(&self) -> common_exception::Result<()> {
        let rusoto_client = S3Client::new(self.region());
        let put_req = PutObjectRequest {
            bucket: self.bucket_name.clone(),
            key: self.test_obj_key.clone(),
            body: Some(ByteStream::from(self.test_obj_content.clone())),
            ..Default::default()
        };
        rusoto_client
            .put_object(put_req)
            .await
            .map(|_| ())
            .map_err(|e| ErrorCode::DalTransportError(e.to_string()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_s3_input_stream_api() -> common_exception::Result<()> {
    let test_key = "test_s3_input_stream".to_string();
    let fixture = TestFixture::with_test_obj(1024 * 10, test_key.clone());
    fixture.gen_test_obj().await?;

    let s3 = fixture.data_accessor()?;
    let mut input = s3.get_input_stream(&test_key, None)?;
    let mut buffer = vec![];
    input.read_to_end(&mut buffer).await?;
    assert_eq!(fixture.test_obj_content, buffer);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_s3_input_stream_seek_api() -> common_exception::Result<()> {
    let test_key = "test_s3_seek_stream_seek".to_string();
    let fixture = TestFixture::with_test_obj(1024 * 10, test_key.clone());
    fixture.gen_test_obj().await?;

    let s3 = fixture.data_accessor()?;
    let mut input = s3.get_input_stream(&test_key, None)?;
    let mut buffer = vec![];
    input.seek(SeekFrom::Current(1)).await?;
    input.read_to_end(&mut buffer).await?;
    assert_eq!(fixture.test_obj_content.len() - 1, buffer.len());
    let r = input.seek(SeekFrom::End(0)).await?;
    assert_eq!(fixture.test_obj_content.len() as u64, r);
    let r = input.seek(SeekFrom::End(1)).await;
    assert!(r.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_s3_input_stream_read_exception() -> common_exception::Result<()> {
    let test_key = "not_exist".to_string();
    let fixture = TestFixture::new();
    let s3 = fixture.data_accessor()?;
    let mut input = s3.get_input_stream(&test_key, None)?;
    let mut buffer = vec![];
    let r = input.read_to_end(&mut buffer).await;
    match r {
        Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::NotFound),
        Ok(_) => panic!("expecting error"),
    }
    Ok(())
}
