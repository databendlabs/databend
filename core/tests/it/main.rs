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

#[tokio::test]
async fn simple_select() {
    let dsn = option_env!("TEST_DATABEND_DSN")
        .unwrap_or("databend://root:@localhost:8000/default?sslmode=disable");
    let client = databend_client::APIClient::from_dsn(dsn).unwrap();
    let resp = client.query("select 15532".into()).await.unwrap();
    assert_eq!(resp.data.len(), 1);
    assert_eq!(resp.data[0].len(), 1);
    assert_eq!(resp.data[0][0], "15532");
}
