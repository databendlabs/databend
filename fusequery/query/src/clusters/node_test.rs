use common_exception::Result;
use crate::clusters::address::Address;
use crate::clusters::Node;

#[test]
fn test_serialize_node() -> Result<()> {
    let node = Node {
        name: String::from("name"),
        priority: 1,
        address: Address::create(&String::from("localhost:9090"))?,
        local: true,
        sequence: 2,
    };

    let node_json = "{\"name\":\"name\",\"priority\":1,\"address\":\"localhost:9090\",\"local\":true,\"sequence\":2}";

    assert_eq!(serde_json::to_string(&node)?, node_json.clone());
    assert_eq!(serde_json::from_str::<Node>(node_json.clone())?, node);

    Ok(())
}