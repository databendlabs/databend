// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_management::cluster::ClusterClient;
use common_runtime::tokio;

use crate::api::http::v1::cluster::*;
use crate::configs::Config;

#[tokio::test]
async fn test_cluster() -> common_exception::Result<()> {
    let mut conf = Config::default();
    conf.cluster_namespace = "n1".to_string();
    conf.cluster_executor_name = "e1".to_string();
    // make the backend uri to local sled store.
    conf.cluster_registry_uri = "local://xx".to_string();

    let cluster_client = ClusterClient::create(conf.clone().cluster_registry_uri);
    let filter = cluster_handler(conf, cluster_client);

    // Register.
    {
        let res = warp::test::request()
            .method("POST")
            .path("/v1/cluster/register")
            .reply(&filter);
        assert_eq!(200, res.await.status());
    }

    // List.
    {
        let res = warp::test::request()
            .method("GET")
            .path("/v1/cluster/list")
            .reply(&filter);
        assert_eq!("[{\"name\":\"e1\",\"priority\":0,\"address\":\"127.0.0.1:9090\",\"local\":false,\"sequence\":0}]", res.await.body());
    }

    // unregister.
    {
        let res = warp::test::request()
            .method("POST")
            .path("/v1/cluster/unregister")
            .reply(&filter);
        assert_eq!(200, res.await.status());
    }

    // List.
    {
        let res = warp::test::request()
            .method("GET")
            .path("/v1/cluster/list")
            .reply(&filter);
        assert_eq!("[]", res.await.body());
    }

    Ok(())
}
