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

use futures::io::copy;
use futures::io::Cursor;
use futures::StreamExt;

use crate::ops::io::CallbackReader;
use crate::ops::ReaderStream;

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
