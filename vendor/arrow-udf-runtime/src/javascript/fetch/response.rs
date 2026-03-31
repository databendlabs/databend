// Copyright 2025 RisingWave Labs
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

use rquickjs::prelude::*;
use rquickjs::Exception;
use rquickjs::Result;

/// The Response interface of the Fetch API represents the response to a request.
///
/// See also https://developer.mozilla.org/en-US/docs/Web/API/Response
#[derive(rquickjs::class::Trace, Default, Debug)]
#[rquickjs::class(rename_all = "camelCase")]
pub struct Response {
    /// The status code of the response. (This will be 200 for a success).
    #[qjs(get)]
    status: u16,

    /// The status message corresponding to the status code. (e.g., OK for 200).
    #[qjs(get)]
    status_text: String,

    /// A boolean indicating whether the response was successful (status in the range 200 – 299) or not.
    #[qjs(get)]
    ok: bool,

    #[qjs(skip_trace)]
    response: Option<reqwest::Response>,
}

impl Response {
    pub fn new(response: reqwest::Response) -> Self {
        Self {
            status: response.status().as_u16(),
            status_text: response
                .status()
                .canonical_reason()
                .unwrap_or("")
                .to_string(),
            ok: response.status().is_success(),
            response: Some(response),
        }
    }
}

#[rquickjs::methods(rename_all = "camelCase")]
impl Response {
    /// Stores a boolean value that declares whether the body has been used in a response yet.
    #[qjs(get)]
    pub fn body_used(&self) -> bool {
        self.response.is_none()
    }

    /// Read and convert response body to text
    #[qjs(rename = "text")]
    pub async fn text(&mut self, ctx: Ctx<'_>) -> Result<String> {
        let response = self
            .response
            .take()
            .ok_or_else(|| Exception::throw_type(&ctx, "Body is unusable"))?;

        response
            .text()
            .await
            .map_err(|e| Exception::throw_message(&ctx, &e.to_string()))
    }

    /// Read and convert response body to JSON
    #[qjs(rename = "json")]
    pub async fn json<'js>(&mut self, ctx: Ctx<'js>) -> Result<rquickjs::Value<'js>> {
        let text = self.text(ctx.clone()).await?;
        ctx.json_parse(text)
    }
}
