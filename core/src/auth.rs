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
    password: SensitiveString,
}

impl BasicAuth {
    pub fn new(username: impl ToString, password: impl ToString) -> Self {
        Self {
            username: username.to_string(),
            password: SensitiveString(password.to_string()),
        }
    }
}

#[async_trait::async_trait]
impl Auth for BasicAuth {
    async fn wrap(&self, builder: RequestBuilder) -> Result<RequestBuilder> {
        Ok(builder.basic_auth(&self.username, Some(self.password.inner())))
    }

    fn username(&self) -> String {
        self.username.clone()
    }
}

#[derive(Clone)]
pub struct AccessTokenAuth {
    token: SensitiveString,
}

impl AccessTokenAuth {
    pub fn new(token: impl ToString) -> Self {
        Self {
            token: SensitiveString::from(token.to_string()),
        }
    }
}

#[async_trait::async_trait]
impl Auth for AccessTokenAuth {
    async fn wrap(&self, builder: RequestBuilder) -> Result<RequestBuilder> {
        Ok(builder.bearer_auth(self.token.inner()))
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
    pub fn new(token_file: impl ToString) -> Self {
        let token_file = token_file.to_string();
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

#[derive(::serde::Deserialize, ::serde::Serialize)]
#[serde(from = "String", into = "String")]
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SensitiveString(String);

impl From<String> for SensitiveString {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for SensitiveString {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<SensitiveString> for String {
    fn from(value: SensitiveString) -> Self {
        value.0
    }
}

impl std::fmt::Display for SensitiveString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "**REDACTED**")
    }
}

impl std::fmt::Debug for SensitiveString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // we keep the double quotes here to keep the String behavior
        write!(f, "\"**REDACTED**\"")
    }
}

impl SensitiveString {
    #[must_use]
    pub fn inner(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialization() {
        let json_value = "\"foo\"";
        let value: SensitiveString = serde_json::from_str(json_value).unwrap();
        let result: String = serde_json::to_string(&value).unwrap();
        assert_eq!(result, json_value);
    }

    #[test]
    fn hide_content() {
        let value = SensitiveString("hello world".to_string());
        let display = format!("{value}");
        assert_eq!(display, "**REDACTED**");
        let debug = format!("{value:?}");
        assert_eq!(debug, "\"**REDACTED**\"");
    }
}
