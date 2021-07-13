// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_runtime::tokio;

#[tokio::test]
async fn test_cluser() -> common_exception::Result<()> {
    use common_management::cluster::ClusterClient;

    use crate::api::http::v1::cluster::*;
    use crate::configs::Config;

    let conf = Config::default();
    let cluster_client = ClusterClient::create(conf.clone().cluster_meta_server_uri);
    let filter = cluster_handler(conf, cluster_client);

    // Add node.
    {
        let res = warp::test::request()
            .method("POST")
            .path("/v1/cluster/add")
            .json(&ClusterNodeRequest {})
            .reply(&filter);
        assert_eq!(200, res.await.status());
    }

    // List.
    {
        let res = warp::test::request()
            .method("GET")
            .path("/v1/cluster/list")
            .json(&ClusterNodeRequest {})
            .reply(&filter);
        assert_eq!("\"v1\"", res.await.body());
    }

    Ok(())
}
