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
use openai_api_rust::chat::ChatApi;
use openai_api_rust::chat::ChatBody;
use openai_api_rust::Auth;
use openai_api_rust::Message;
use openai_api_rust::Role;

use crate::metrics::metrics_completion_count;
use crate::metrics::metrics_completion_token;
use crate::AIModel;
use crate::OpenAI;

impl OpenAI {
    pub fn completion_text_request(&self, prompt: String) -> Result<(String, Option<u32>)> {
        let openai = openai_api_rust::OpenAI::new(
            Auth {
                api_key: self.api_key.clone(),
                organization: None,
            },
            &self.api_base,
        );

        let (max_tokens, stop) = (Some(512), None);

        let body = ChatBody {
            model: AIModel::GPT35Turbo.to_string(),
            temperature: Some(0_f32),
            top_p: Some(1_f32),
            n: None,
            stream: None,
            stop,
            max_tokens,
            presence_penalty: None,
            frequency_penalty: None,
            logit_bias: None,
            user: None,
            messages: vec![Message {
                role: Role::User,
                content: prompt,
            }],
        };

        let resp = openai.chat_completion_create(&body).map_err(|e| {
            ErrorCode::Internal(format!("openai completion text request error: {:?}", e))
        })?;

        let usage = resp.usage.total_tokens;
        let sql = if resp.choices.is_empty() {
            "".to_string()
        } else {
            resp.choices[0].text.clone().unwrap_or("".to_string())
        };

        // perf.
        {
            metrics_completion_count(1);
            metrics_completion_token(usage.unwrap_or(0));
        }

        Ok((sql, usage))
    }
}
