// Copyright 2021 Datafuse Labs.
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

use std::str;

use common_dal2::services::fs;
use common_dal2::Operator;
use futures::io::AsyncReadExt;
use futures::io::Cursor;

#[tokio::test]
async fn normal() {
    let f = Operator::new(fs::Backend::build().finish().unwrap());

    let path = format!("/tmp/{}", uuid::Uuid::new_v4());

    // Test write
    let x = f
        .write(&path, 13)
        .run(Box::new(Cursor::new("Hello, world!")))
        .await
        .unwrap();
    assert_eq!(13, x);

    // Test read
    let mut buf: Vec<u8> = Vec::new();
    let mut x = f.read(&path).run().await.unwrap();
    x.read_to_end(&mut buf).await.unwrap();
    assert_eq!("Hello, world!", str::from_utf8(&buf).unwrap());

    // Test stat
    let o = f.stat(&path).run().await.unwrap();
    assert_eq!(13, o.size);

    // Test delete
    f.delete(&path).run().await.unwrap();
}
