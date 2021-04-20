// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::meta_service::GetReply;
use crate::meta_service::GetReq;
use crate::meta_service::Meta;
use crate::meta_service::MetaService;
use crate::meta_service::SetReply;
use crate::meta_service::SetReq;

pub struct MetaServiceImpl {
    pub metadata: Arc<Mutex<Meta>>
}

impl MetaServiceImpl {
    pub fn create() -> Self {
        Self {
            metadata: Arc::new(Mutex::new(Meta::empty()))
        }
    }
}

#[async_trait::async_trait]
impl MetaService for MetaServiceImpl {
    async fn set(
        &self,
        request: tonic::Request<SetReq>
    ) -> Result<tonic::Response<SetReply>, tonic::Status> {
        let req = request.into_inner();
        let mut m = self.metadata.lock().await;
        if req.if_absent && m.keys.contains_key(&req.key) {
            return Ok(tonic::Response::new(SetReply { ok: false }));
        }
        m.keys.insert(req.key, req.value);
        return Ok(tonic::Response::new(SetReply { ok: true }));
    }
    async fn get(
        &self,
        request: tonic::Request<GetReq>
    ) -> Result<tonic::Response<GetReply>, tonic::Status> {
        let req = request.into_inner();
        let m = self.metadata.lock().await;
        let v = m.keys.get(&req.key);
        let rst = match v {
            Some(v) => GetReply {
                ok: true,
                key: req.key,
                value: v.clone()
            },
            None => GetReply {
                ok: false,
                key: req.key,
                value: "".into()
            }
        };

        Ok(tonic::Response::new(rst))
    }
}
