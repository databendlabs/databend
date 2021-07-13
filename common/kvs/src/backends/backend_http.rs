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

pub struct HttpBackend {
    addr: String,
}

impl HttpBackend {
    pub fn create(addr: String) -> Self {
        let addr = format!("http://{}/v1/kv", addr);
        Self { addr }
    }
}

#[async_trait]
impl Backend for HttpBackend {
    async fn get(&self, key: String) -> Result<Option<String>> {
        let req = Request {
            key,
            value: "".to_owned(),
        };
        let res: String = reqwest::Client::new()
            .post(format!("{}/get", self.addr))
            .json(&req)
            .send()
            .await?
            .json()
            .await?;
        Ok(Some(res))
    }

    async fn get_from_prefix(&self, prefix: String) -> Result<Vec<(String, String)>> {
        let req = Request {
            key: prefix,
            value: "".to_string(),
        };
        let res: Vec<(String, String)> = reqwest::Client::new()
            .post(format!("{}/list", self.addr))
            .json(&req)
            .send()
            .await?
            .json()
            .await?;
        Ok(res)
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
