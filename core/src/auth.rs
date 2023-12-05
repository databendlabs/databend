// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use reqwest::RequestBuilder;

use crate::error::{Error, Result};

#[async_trait::async_trait]
pub trait Auth: Sync + Send {
    async fn wrap(&self, builder: RequestBuilder) -> Result<RequestBuilder>;
    fn username(&self) -> String;
}

#[derive(Clone)]
pub struct BasicAuth {
    username: String,
    password: Option<String>,
}

impl BasicAuth {
    pub fn new(username: String, password: Option<String>) -> Self {
        Self { username, password }
    }
}

#[async_trait::async_trait]
impl Auth for BasicAuth {
    async fn wrap(&self, builder: RequestBuilder) -> Result<RequestBuilder> {
        Ok(builder.basic_auth(&self.username, self.password.as_deref()))
    }

    fn username(&self) -> String {
        self.username.clone()
    }
}

#[derive(Clone)]
pub struct AccessTokenAuth {
    token: String,
}

impl AccessTokenAuth {
    pub fn new(token: String) -> Self {
        Self { token }
    }
}

#[async_trait::async_trait]
impl Auth for AccessTokenAuth {
    async fn wrap(&self, builder: RequestBuilder) -> Result<RequestBuilder> {
        Ok(builder.bearer_auth(&self.token))
    }

    fn username(&self) -> String {
        "token".to_string()
    }
}

#[derive(Clone)]
pub struct AccessTokenFileAuth {
    token_file: String,
}

impl AccessTokenFileAuth {
    pub fn new(token_file: String) -> Self {
        Self { token_file }
    }
}

#[async_trait::async_trait]
impl Auth for AccessTokenFileAuth {
    async fn wrap(&self, builder: RequestBuilder) -> Result<RequestBuilder> {
        let token = tokio::fs::read_to_string(&self.token_file)
            .await
            .map_err(|e| {
                Error::IO(format!(
                    "cannot read access token from file {}: {}",
                    self.token_file, e
                ))
            })?;
        Ok(builder.bearer_auth(token.trim()))
    }

    fn username(&self) -> String {
        "token".to_string()
    }
}
