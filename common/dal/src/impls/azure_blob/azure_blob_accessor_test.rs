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
use std::str;

use common_base::tokio;
use common_exception::Result;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;

use crate::AzureBlobAccessor;
use crate::DataAccessor;

fn generate_test_data() -> String {
    let mut s = "0123456789".to_string();
    s.push_str("abcdefghijklmnopqrstuvwxyz");
    s.push_str("ABCDEFGHIJKLMNIPQRSTUVWXYZ");
    s
}

fn get_accessor() -> AzureBlobAccessor {
    AzureBlobAccessor::with_credentials(
        "YOUR_AZURE_STORAGE_ACCOUNT",
        "YOUR_AZURE_BLOB_CONTAINER",
        "YOUR_AZURE_BLOB_MASTER_KEY",
    )
}

//  Need to set azure account and master key to run the test
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_put_get() -> Result<()> {
    let accessor = get_accessor();
    let data = generate_test_data();
    let data_vec: Vec<u8> = data.as_bytes().to_vec();
    let blob_name = "testblob.txt";

    accessor.put(blob_name, data_vec.clone()).await?;
    let result = accessor.get(blob_name).await;

    assert!(result.is_ok());

    let fetched_data = result.unwrap();
    assert_eq!(data_vec, fetched_data);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_get_input_stream() -> Result<()> {
    let accessor = get_accessor();
    let data = generate_test_data();
    let data_vec: Vec<u8> = data.as_bytes().to_vec();
    let blob_name = "testblob.txt";

    accessor.put(blob_name, data_vec).await.unwrap();
    let stream_res = accessor.get_input_stream(blob_name, None);

    assert!(stream_res.is_ok());

    let mut stream = stream_res.unwrap();
    let buf: &mut [u8; 128] = &mut [0_u8; 128];

    // first read
    let len = stream.read(buf).await?;
    assert_eq!(len, data.len());

    let read_str = str::from_utf8(&buf[0..len]).unwrap();
    assert_eq!(data, read_str);

    // second read, should return 0 length
    let len = stream.read(buf).await?;
    assert_eq!(0, len);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_stream_seek_from_start() -> Result<()> {
    let accessor = get_accessor();
    let data = generate_test_data();
    let data_vec: Vec<u8> = data.as_bytes().to_vec();
    let blob_name = "testblob.txt";

    accessor.put(blob_name, data_vec).await.unwrap();
    let stream_res = accessor.get_input_stream(blob_name, None);

    assert!(stream_res.is_ok());

    let mut stream = stream_res.unwrap();
    let buf: &mut [u8; 1] = &mut [0_u8; 1];

    // seek 10 characters and read 1 character, should have 'a'
    let pos = stream.seek(SeekFrom::Start(10)).await?;
    assert_eq!(pos, 10);
    let len = stream.read(buf).await?;
    assert_eq!(len, 1);
    assert_eq!(buf[0], b'a');

    // seek 25 characters and read 1 character, should have 'A'
    let pos = stream.seek(SeekFrom::Current(25)).await?;
    assert_eq!(pos, 36);
    let len = stream.read(buf).await?;
    assert_eq!(len, 1);
    assert_eq!(buf[0], b'A');

    // do seek 10 characters from start again and read 1 character, should have 'a'
    // should have zero length returned
    let pos = stream.seek(SeekFrom::Start(10)).await?;
    assert_eq!(pos, 10);
    let len = stream.read(buf).await?;
    assert_eq!(len, 1);
    assert_eq!(buf[0], b'a');

    // do seek 100 characters from current and read 1 character, should have zero length returned
    let pos = stream.seek(SeekFrom::Current(100)).await?;
    assert_eq!(pos, 111);
    let len = stream.read(buf).await?;
    assert_eq!(0, len);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn test_stream_seek_from_end() -> Result<()> {
    let accessor = get_accessor();
    let data = generate_test_data();
    let data_vec: Vec<u8> = data.as_bytes().to_vec();
    let blob_name = "testblob.txt";

    accessor.put(blob_name, data_vec).await.unwrap();
    let stream_res = accessor.get_input_stream(blob_name, None);

    assert!(stream_res.is_ok());

    let mut stream = stream_res.unwrap();
    let buf: &mut [u8; 1] = &mut [0_u8; 1];

    // seek to end-1 and read, should have 'Z'
    let pos = stream.seek(SeekFrom::End(-1)).await?;
    assert_eq!(pos, 61);
    let len = stream.read(buf).await?;
    assert_eq!(len, 1);
    assert_eq!(buf[0], b'Z');

    // seek to end and read, should have empty result
    let pos = stream.seek(SeekFrom::End(0)).await?;
    assert_eq!(pos, 62);
    let len = stream.read(buf).await?;
    assert_eq!(len, 0);

    Ok(())
}
