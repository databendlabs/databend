// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_runtime::tokio;

use crate::api::http::v1::kv::kv_handler;
use crate::api::http::v1::kv::KvRequest;
use crate::api::http::v1::kv::KvStore;

#[tokio::test]
async fn test_kvs() -> common_exception::Result<()> {
    let store = KvStore::create();
    let filter = kv_handler(store);

    // Add node.
    {
        let res = warp::test::request()
            .method("POST")
            .path("/v1/kv/put")
            .json(&KvRequest {
                key: "n1_k1".to_string(),
                value: "v1".to_string(),
            })
            .reply(&filter);
        assert_eq!(200, res.await.status());

        let res = warp::test::request()
            .method("POST")
            .path("/v1/kv/put")
            .json(&KvRequest {
                key: "n1_k2".to_string(),
                value: "v2".to_string(),
            })
            .reply(&filter);
        assert_eq!(200, res.await.status());
    }

    // Get.
    {
        let res = warp::test::request()
            .method("GET")
            .path("/v1/kv/get/n1_k1")
            .reply(&filter);
        assert_eq!("\"v1\"", res.await.body());
    }

    // List.
    {
        let res = warp::test::request()
            .method("GET")
            .path("/v1/kv/list/n1")
            .reply(&filter);
        assert_eq!("[[\"n1_k1\",\"v1\"],[\"n1_k2\",\"v2\"]]", res.await.body());
    }

    // Del.
    {
        let res = warp::test::request()
            .method("POST")
            .path("/v1/kv/remove")
            .json(&KvRequest {
                key: "n1_k1".to_string(),
                value: "".to_string(),
            })
            .reply(&filter);
        assert_eq!(200, res.await.status());
    }

    // List.
    {
        let res = warp::test::request()
            .method("GET")
            .path("/v1/kv/list/n1")
            .reply(&filter);
        assert_eq!("[[\"n1_k2\",\"v2\"]]", res.await.body());
    }

    Ok(())
}
