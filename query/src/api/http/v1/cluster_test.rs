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

#[tokio::test]
async fn test_cluster() -> Result<()> {
    use pretty_assertions::assert_eq;

    use crate::api::http::v1::cluster::*;
    use crate::clusters::Cluster;
    use crate::configs::Config;

    let conf = Config::default();
    let cluster = Cluster::create_global(conf.clone())?;
    let filter = cluster_handler(cluster);

    // Add node.
    {
        let res = warp::test::request()
            .method("POST")
            .path("/v1/cluster/add")
            .json(&ClusterNodeRequest {
                name: "9090".to_string(),
                priority: 8,
                address: "127.0.0.1:9090".to_string(),
            })
            .reply(&filter);
        assert_eq!(200, res.await.status());

        // Add node.
        let res = warp::test::request()
            .method("POST")
            .path("/v1/cluster/add")
            .json(&ClusterNodeRequest {
                name: "9091".to_string(),
                priority: 4,
                address: "127.0.0.1:9091".to_string(),
            })
            .reply(&filter);
        assert_eq!(200, res.await.status());
    }

    // Remove.
    {
        // Add node.
        let res = warp::test::request()
            .method("POST")
            .path("/v1/cluster/remove")
            .json(&ClusterNodeRequest {
                name: "9091".to_string(),
                priority: 4,
                address: "127.0.0.1:9091".to_string(),
            })
            .reply(&filter);
        assert_eq!(200, res.await.status());
    }

    // Check.
    {
        let res = warp::test::request()
            .path("/v1/cluster/list")
            .reply(&filter);
        assert_eq!(
            "[{\"name\":\"9090\",\"priority\":8,\"address\":\"127.0.0.1:9090\",\"local\":true,\"sequence\":0}]",
            res.await.body()
        );
    }

    Ok(())
}
