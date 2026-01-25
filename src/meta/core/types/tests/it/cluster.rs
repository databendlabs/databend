// Copyright 2021 Datafuse Labs
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

use databend_common_meta_types::NodeInfo;

#[test]
fn test_node_info_ip_port() -> anyhow::Result<()> {
    let n = NodeInfo {
        id: "".to_string(),
        secret: "".to_string(),
        cpu_nums: 1,
        version: 1,
        http_address: "7.8.9.10:987".to_string(),
        flight_address: "1.2.3.4:123".to_string(),
        discovery_address: "4.5.6.7:456".to_string(),
        binary_version: "v0.8-binary-version".to_string(),
        node_type: Default::default(),
        node_group: None,
        cluster_id: "".to_string(),
        warehouse_id: "".to_string(),
        runtime_node_group: None,
        cache_id: "".to_string(),
    };

    let (ip, port) = n.ip_port()?;
    assert_eq!("1.2.3.4".to_string(), ip);
    assert_eq!(123, port);
    assert_eq!("v0.8-binary-version".to_string(), n.binary_version);

    Ok(())
}

#[test]
fn test_serde_node_info() {
    let mut info = NodeInfo {
        id: "test_id".to_string(),
        secret: "test_secret".to_string(),
        version: 1,
        cpu_nums: 1,
        http_address: "7.8.9.10:987".to_string(),
        flight_address: "1.2.3.4:123".to_string(),
        discovery_address: "4.5.6.7:456".to_string(),
        binary_version: "v0.8-binary-version".to_string(),
        node_type: Default::default(),
        node_group: None,
        cluster_id: String::new(),
        warehouse_id: String::new(),
        runtime_node_group: None,
        cache_id: "test_id".to_string(),
    };

    let json_str = serde_json::to_string(&info).unwrap();
    assert_eq!(info, serde_json::from_str::<NodeInfo>(&json_str).unwrap());
    assert!(!json_str.contains("cluster"));
    assert!(!json_str.contains("warehouse"));

    info.cluster_id = String::from("test-cluster-id");
    info.warehouse_id = String::from("test-warehouse-id");

    assert_eq!(
        info,
        serde_json::from_slice::<NodeInfo>(&serde_json::to_vec(&info).unwrap()).unwrap()
    );
}
