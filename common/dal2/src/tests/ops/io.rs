// Copyright 2022 Datafuse Labs.
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

use std::io::SeekFrom;
use std::str::from_utf8;

use futures::io::copy;
use futures::io::BufReader;
use futures::io::Cursor;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use futures::StreamExt;

use crate::ops::io::CallbackReader;
use crate::ops::io::HeaderRange;
use crate::ops::io::SeekableReader;
use crate::ops::ReaderStream;
use crate::services::fs;
use crate::Operator;

#[tokio::test]
async fn reader_stream() {
    let reader = Box::new(Cursor::new("Hello, world!"));
    let mut s = ReaderStream::new(reader);

    let mut bs = Vec::new();
    while let Some(chunk) = s.next().await {
        bs.extend_from_slice(&chunk.unwrap());
    }

    assert_eq!(&bs[..], "Hello, world!".as_bytes());
}

#[tokio::test]
async fn callback_reader() {
    let mut size = 0;

    let reader = CallbackReader::new(Box::new(Cursor::new("Hello, world!")), |n| size += n);

    let mut bs = Vec::new();
    let n = copy(reader, &mut bs).await.unwrap();

    assert_eq!(size, 13);
    assert_eq!(n, 13);
}

#[tokio::test]
async fn test_seekable_reader() {
    let f = Operator::new(fs::Backend::build().finish());

    // Create a test file.
    let x = f
        .write("/tmp/x", 13)
        .run(Box::new(Cursor::new("Hello, world!")))
        .await
        .unwrap();
    assert_eq!(x, 13);

    let o = f.stat("/tmp/x").run().await.unwrap();
    assert_eq!(o.size, 13);

    let mut r = BufReader::with_capacity(
        4 * 1024 * 1024, // 4 MiB
        SeekableReader::new(f, "tmp/x", o.size),
    );

    let n = r.seek(SeekFrom::Current(3)).await.expect("seek");
    assert_eq!(n, 3);

    let mut bs = Vec::with_capacity(5);
    let n = r.read_to_end(&mut bs).await.expect("read_to_end");
    assert_eq!("lo, world!", from_utf8(&bs).unwrap());
    assert_eq!(n, 10);
}

#[test]
fn test_header_range() {
    let h = HeaderRange::new(None, Some(1024));
    assert_eq!(h.to_string(), "bytes=0-1023");

    let h = HeaderRange::new(Some(1024), None);
    assert_eq!(h.to_string(), "bytes=1024-");

    let h = HeaderRange::new(Some(1024), Some(1024));
    assert_eq!(h.to_string(), "bytes=1024-2047");
}
