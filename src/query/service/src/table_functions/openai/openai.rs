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

use common_exception::ErrorCode;
use common_exception::Result;
use openai_api_rust::completions::Completion;
use openai_api_rust::completions::CompletionsApi;
use openai_api_rust::completions::CompletionsBody;
use openai_api_rust::Auth;

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
    api_base: String,
    api_org: String,
    model: AIModel,
}

impl OpenAI {
    pub fn create(api_key: String, model: AIModel) -> Self {
        OpenAI {
            api_key,
            api_base: "https://api.openai.com/v1/".to_string(),
            api_org: "databend".to_string(),
            model,
        }
    }

    pub fn completion_request(&self, prompt: String) -> Result<Completion> {
        let openai = openai_api_rust::OpenAI::new(
            Auth {
                api_key: self.api_key.clone(),
                organization: Some(self.api_org.clone()),
            },
            &self.api_base,
        );
        let body = CompletionsBody {
            model: self.model.to_string(),
            prompt: Some(vec![prompt]),
            suffix: None,
            max_tokens: Some(150),
            temperature: Some(0_f32),
            top_p: Some(1_f32),
            n: Some(2),
            stream: Some(false),
            logprobs: None,
            echo: None,
            stop: Some(vec!["#".to_string(), ";".to_string()]),
            presence_penalty: None,
            frequency_penalty: None,
            best_of: None,
            logit_bias: None,
            user: None,
        };
        openai
            .completion_create(&body)
            .map_err(|e| ErrorCode::Internal(format!("openai completion request error: {:?}", e)))
    }
}
