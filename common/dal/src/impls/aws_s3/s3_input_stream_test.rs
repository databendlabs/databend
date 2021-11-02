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

use std::io::SeekFrom;

use common_base::tokio;
use common_exception::ErrorCode;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use rusoto_core::ByteStream;
use rusoto_core::Region;
use rusoto_s3::PutObjectRequest;
use rusoto_s3::S3Client;
use rusoto_s3::S3 as RusotoS3;

use crate::DataAccessor;
use crate::S3;

struct TestFixture {
    region_name: String,
    endpoint_url: String,
    bucket_name: String,
    test_key: String,
    content: Vec<u8>,
}

impl TestFixture {
    fn new(size: usize, key: String) -> Self {
        let random_bytes: Vec<u8> = (0..size).map(|_| rand::random::<u8>()).collect();
        Self {
            region_name: "us-east-1".to_string(),
            endpoint_url: "http://localhost:9000".to_string(),
            bucket_name: "test-bucket".to_string(),
            test_key: key,
            content: random_bytes,
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
            "",
            "",
        )
    }
}

impl TestFixture {
    async fn gen_test_obj(&self) -> common_exception::Result<()> {
        let rusoto_client = S3Client::new(self.region());
        let put_req = PutObjectRequest {
            bucket: self.bucket_name.clone(),
            key: self.test_key.clone(),
            body: Some(ByteStream::from(self.content.clone())),
            ..Default::default()
        };
        rusoto_client
            .put_object(put_req)
            .await
            .map(|_| ())
            .map_err(|e| ErrorCode::DALTransportError(e.to_string()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[ignore]
async fn test_s3_input_stream_api() -> common_exception::Result<()> {
    let test_key = "test_s3_input_stream".to_string();
    let fixture = TestFixture::new(1024 * 10, test_key.clone());
    fixture.gen_test_obj().await?;

    let s3 = fixture.data_accessor()?;
    let mut input = s3.get_input_stream(&test_key, None)?;
    let mut buffer = vec![];
    input.read_to_end(&mut buffer).await?;
    assert_eq!(fixture.content, buffer);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[ignore]
async fn test_s3_input_stream_seek_api() -> common_exception::Result<()> {
    let test_key = "test_s3_seek_stream_seek".to_string();
    let fixture = TestFixture::new(1024 * 10, test_key.clone());
    fixture.gen_test_obj().await?;

    let s3 = fixture.data_accessor()?;
    let mut input = s3.get_input_stream(&test_key, None)?;
    let mut buffer = vec![];
    input.seek(SeekFrom::Current(1)).await?;
    input.read_to_end(&mut buffer).await?;
    assert_eq!(fixture.content.len() - 1, buffer.len());
    let r = input.seek(SeekFrom::End(0)).await?;
    assert_eq!(fixture.content.len() as u64, r);
    let r = input.seek(SeekFrom::End(1)).await;
    assert!(r.is_err());
    Ok(())
}
