// Copyright 2023 Datafuse Labs.
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

use databend_driver::new_connection;

use crate::common::DEFAULT_DSN;

#[tokio::test]
async fn trait_with_clone() {
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
    let conn = new_connection(dsn).unwrap();
    let row = conn.query_row("select 'hello'").await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (String,) = row.try_into().unwrap();
    assert_eq!(val, "hello");

    let conn2 = conn.clone();
    let row = conn2.query_row("select 'world'").await.unwrap();
    assert!(row.is_some());
    let row = row.unwrap();
    let (val,): (String,) = row.try_into().unwrap();
    assert_eq!(val, "world");
}
