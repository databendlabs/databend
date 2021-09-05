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
        let res = ureq::get(format!("{}/get/{}", self.addr, key).as_str())
            .call()?
            .into_string()?;
        Ok(Some(res))
    }

    async fn get_from_prefix(&self, prefix: String) -> Result<Vec<(String, String)>> {
        let body: Vec<(String, String)> =
            ureq::get(format!("{}/list/{}", self.addr, prefix).as_str())
                .call()?
                .into_json()?;
        Ok(body)
    }

    async fn put(&self, key: String, value: String) -> Result<()> {
        let req = Request { key, value };
        ureq::post(format!("{}/put", self.addr).as_str()).send_json(ureq::json!(req))?;
        Ok(())
    }

    async fn remove(&self, key: String) -> Result<()> {
        let req = Request {
            key,
            value: "".to_string(),
        };
        ureq::post(format!("{}/remove", self.addr).as_str()).send_json(ureq::json!(req))?;
        Ok(())
    }

    async fn lock(&self, _key: String) -> Result<Box<dyn Lock>> {
        todo!()
    }
}
