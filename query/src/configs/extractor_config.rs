use std::net::SocketAddr;

use common_management::cluster::ClusterConfig;

use crate::configs::Config;

/// Used to extract some type of configuration in config
/// e.g: extract_cluster
pub trait ConfigExtractor {
    fn extract_cluster(&self) -> ClusterConfig;
}

impl ConfigExtractor for Config {
    fn extract_cluster(&self) -> ClusterConfig {
        // ClusterConfig {
        //     version: format!(
        //         "FuseQuery v-{}",
        //         *crate::configs::config::FUSE_COMMIT_VERSION
        //     ),
        //     local_address: "".parse::<SocketAddr>()?,
        // }
        unimplemented!()
    }
}
