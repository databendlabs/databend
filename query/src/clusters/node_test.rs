// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;
use common_runtime::tokio;

use crate::clusters::address::Address;
use crate::clusters::Node;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_serialize_node() -> Result<()> {
    let node = Node::create(
        String::from("name"),
        1,
        Address::create(&String::from("localhost:9090"))?,
        true,
        2,
    )?;

    let node_json = "{\"name\":\"name\",\"priority\":1,\"address\":\"localhost:9090\",\"local\":true,\"sequence\":2}";

    assert_eq!(serde_json::to_string(&node)?, node_json.clone());
    assert_eq!(serde_json::from_str::<Node>(node_json.clone())?, node);

    Ok(())
}
