//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::time::Duration;

use async_openai::types::CreateCompletionRequest;
use async_openai::types::CreateCompletionRequestArgs;
use async_openai::Client;
use common_exception::ErrorCode;
use common_exception::Result;

pub enum AIModel {
    CodeDavinci002,
}

// https://platform.openai.com/examples
impl ToString for AIModel {
    fn to_string(&self) -> String {
        match self {
            AIModel::CodeDavinci002 => "code-davinci-002".to_string(),
        }
    }
}

pub struct OpenAI {
    api_key: String,
    model: AIModel,
}

impl OpenAI {
    pub fn create(api_key: String, model: AIModel) -> Self {
        OpenAI { api_key, model }
    }

    pub fn client(&self) -> Result<Client> {
        let timeout = Duration::from_secs(30);
        // Client
        let http_client = reqwest::ClientBuilder::new()
            .user_agent("databend")
            .timeout(timeout)
            .build()
            .map_err(|e| ErrorCode::Internal(format!("openai http error: {:?}", e)))?;

        Ok(Client::new()
            .with_api_key(self.api_key.clone())
            .with_http_client(http_client))
    }

    pub fn completion_request(&self, prompt: String) -> Result<CreateCompletionRequest> {
        CreateCompletionRequestArgs::default()
            .model(self.model.to_string())
            .prompt(prompt)
            .temperature(0.0)
            .max_tokens(150_u16)
            .top_p(1.0)
            .frequency_penalty(0.0)
            .presence_penalty(0.0)
            .stop(["#", ";"])
            .build()
            .map_err(|e| ErrorCode::Internal(format!("openai completion request error: {:?}", e)))
    }
}
