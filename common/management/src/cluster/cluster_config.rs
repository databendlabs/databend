use std::net::SocketAddr;

pub struct ClusterConfig {
    pub version: String,
    pub namespace: String,
    pub local_address: SocketAddr,
}
