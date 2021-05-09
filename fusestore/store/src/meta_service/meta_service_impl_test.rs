// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[allow(unused_imports)]
use log::info;
use pretty_assertions::assert_eq;

use crate::meta_service::GetReq;
use crate::meta_service::MetaNode;
use crate::meta_service::MetaServiceClient;
use crate::meta_service::MetaServiceImpl;
use crate::meta_service::MetaServiceServer;
use crate::meta_service::SetReq;
use crate::tests::rand_local_addr;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_server_set_get() -> anyhow::Result<()> {
    let addr = rand_local_addr();

    let mn = MetaNode::new(0).await;

    let meta_srv_impl = MetaServiceImpl::create(mn).await;
    let meta_srv = MetaServiceServer::new(meta_srv_impl);

    serve_grpc!(addr, meta_srv);

    let mut client = MetaServiceClient::connect(format!("http://{}", addr)).await?;

    {
        // add: ok
        let req = tonic::Request::new(SetReq {
            key: "foo".into(),
            value: "bar".into(),
            if_absent: true
        });
        let rst = client.set(req).await?.into_inner();
        assert_eq!(true, rst.ok);

        // get the stored value

        let req = tonic::Request::new(GetReq { key: "foo".into() });
        let rst = client.get(req).await?.into_inner();
        assert_eq!("bar", rst.value);
    }

    {
        // add: conflict with existent.
        let req = tonic::Request::new(SetReq {
            key: "foo".into(),
            value: "bar".into(),
            if_absent: true
        });
        let rst = client.set(req).await?.into_inner();
        assert_eq!(false, rst.ok);
    }
    {
        // set: overrde. ok.
        let req = tonic::Request::new(SetReq {
            key: "foo".into(),
            value: "bar2".into(),
            if_absent: false
        });
        let rst = client.set(req).await?.into_inner();
        assert_eq!(true, rst.ok);

        // get the stored value

        let req = tonic::Request::new(GetReq { key: "foo".into() });
        let rst = client.get(req).await?.into_inner();
        assert_eq!("bar2", rst.value);
    }

    Ok(())
}
