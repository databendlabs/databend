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

use crate::meta_service::NodeId;
use crate::sled_store::sled_serde::SledOrderedSerde;

#[test]
fn test_serde_node_id() -> anyhow::Result<()> {
    let ids: Vec<NodeId> = vec![9, 10, 11];

    let want: Vec<sled::IVec> = vec![
        sled::IVec::from(vec![0, 0, 0, 0, 0, 0, 0, 9]),
        sled::IVec::from(vec![0, 0, 0, 0, 0, 0, 0, 10]),
        sled::IVec::from(vec![0, 0, 0, 0, 0, 0, 0, 11]),
    ];
    let got = ids.iter().map(|id| id.ser().unwrap()).collect::<Vec<_>>();
    assert_eq!(want, got);
    Ok(())
}
