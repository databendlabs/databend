// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use common_exception::Result;

use crate::backends::Backend;
use crate::backends::Lock;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct Request {
    key: String,
    value: String,
}

#[allow(dead_code)]
pub struct MemoryBackend {
    addr: String,
}

impl MemoryBackend {
    pub fn create(addr: String) -> Self {
        let addr = format!("http://{}/{}", addr, "/v1/kv");
        Self { addr }
    }
}

#[async_trait]
impl Backend for MemoryBackend {
    async fn get(&self, key: String) -> Result<Option<String>> {
        let req = Request {
            key,
            value: "".to_owned(),
        };
        let res: String = reqwest::Client::new()
            .post(format!("{}/get", self.addr))
            .json(&&req)
            .send()
            .await?
            .json()
            .await?;
        Ok(Some(res))
    }

    async fn get_from_prefix(&self, _prefix: String) -> Result<Vec<(String, String)>> {
        todo!()
    }

    async fn put(&self, key: String, value: String) -> Result<()> {
        let req = Request { key, value };
        reqwest::Client::new()
            .post(format!("{}/put", self.addr))
            .json(&req)
            .send()
            .await?;
        Ok(())
    }

    async fn remove(&self, key: String) -> Result<()> {
        let req = Request {
            key,
            value: "".to_string(),
        };
        reqwest::Client::new()
            .post(format!("{}/remove", self.addr))
            .json(&req)
            .send()
            .await?;
        Ok(())
    }

    async fn lock(&self, _key: String) -> Result<Box<dyn Lock>> {
        todo!()
    }
}
