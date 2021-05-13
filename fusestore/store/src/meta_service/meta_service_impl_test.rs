// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[allow(unused_imports)]
use log::info;
use pretty_assertions::assert_eq;

use crate::meta_service::ClientRequest;
use crate::meta_service::ClientResponse;
use crate::meta_service::Cmd;
use crate::meta_service::GetReq;
use crate::meta_service::MetaNode;
use crate::meta_service::MetaServiceClient;
use crate::meta_service::MetaServiceImpl;
use crate::meta_service::MetaServiceServer;
use crate::tests::rand_local_addr;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_server_set_get() -> anyhow::Result<()> {
    let addr = rand_local_addr();

    let mn = MetaNode::new(0).await;
    let rst = mn.boot(addr.clone()).await;
    assert!(rst.is_ok());

    let meta_srv_impl = MetaServiceImpl::create(mn).await;
    let meta_srv = MetaServiceServer::new(meta_srv_impl);

    serve_grpc!(addr, meta_srv);

    let mut client = MetaServiceClient::connect(format!("http://{}", addr)).await?;

    {
        // add: ok
        let req = ClientRequest {
            txid: None,
            cmd: Cmd::AddFile {
                key: "foo".to_string(),
                value: "bar".to_string()
            }
        };
        let rst = client.write(req).await?.into_inner();
        let resp: ClientResponse = rst.into();
        match resp {
            ClientResponse::String { prev: _, result } => {
                assert_eq!("bar".to_string(), result.unwrap());
            }
            _ => {
                panic!("not string")
            }
        }

        // get the stored value

        let req = tonic::Request::new(GetReq { key: "foo".into() });
        let rst = client.get(req).await?.into_inner();
        assert_eq!("bar", rst.value);
    }

    {
        // add: conflict with existent.
        let req = ClientRequest {
            txid: None,
            cmd: Cmd::AddFile {
                key: "foo".to_string(),
                value: "bar".to_string()
            }
        };
        let rst = client.write(req).await?.into_inner();
        let resp: ClientResponse = rst.into();
        match resp {
            ClientResponse::String { prev: _, result } => {
                assert!(result.is_none());
            }
            _ => {
                panic!("not string")
            }
        }
    }
    {
        // set: overrde. ok.
        let req = ClientRequest {
            txid: None,
            cmd: Cmd::SetFile {
                key: "foo".to_string(),
                value: "bar2".to_string()
            }
        };
        let rst = client.write(req).await?.into_inner();
        let resp: ClientResponse = rst.into();
        match resp {
            ClientResponse::String { prev: _, result } => {
                assert_eq!(Some("bar2".to_string()), result);
            }
            _ => {
                panic!("not string")
            }
        }

        // get the stored value

        let req = tonic::Request::new(GetReq { key: "foo".into() });
        let rst = client.get(req).await?.into_inner();
        assert_eq!("bar2", rst.value);
    }

    Ok(())
}
