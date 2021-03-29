// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test]
async fn test_cluster() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    use crate::api::http::v1::cluster::*;
    use crate::clusters::Cluster;
    use crate::configs::Config;

    let conf = Config::default();
    let cluster = Cluster::create(conf.clone());
    let filter = cluster_handler(cluster);

    // Add node.
    {
        let res = warp::test::request()
            .method("POST")
            .path("/v1/cluster/add")
            .json(&ClusterNodeRequest {
                name: "9090".to_string(),
                cpus: 4,
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
                cpus: 4,
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
                cpus: 4,
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
            "[{\"name\":\"9090\",\"cpus\":4,\"priority\":8,\"address\":\"127.0.0.1:9090\",\"local\":true}]",
            res.await.body()
        );
    }

    Ok(())
}
