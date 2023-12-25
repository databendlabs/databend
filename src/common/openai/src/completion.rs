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

use std::time::Duration;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::openai::*;
use log::trace;
use openai_api_rust::chat::ChatApi;
use openai_api_rust::chat::ChatBody;
use openai_api_rust::Auth;
use openai_api_rust::Message;
use openai_api_rust::Role;

use crate::OpenAI;

#[derive(Debug)]
enum CompletionMode {
    Sql,
    Text,
}

impl OpenAI {
    pub fn completion_text_request(&self, prompt: String) -> Result<(String, Option<u32>)> {
        self.completion_request(CompletionMode::Text, prompt)
    }

    pub fn completion_sql_request(&self, prompt: String) -> Result<(String, Option<u32>)> {
        self.completion_request(CompletionMode::Sql, prompt)
    }

    fn completion_request(
        &self,
        mode: CompletionMode,
        prompt: String,
    ) -> Result<(String, Option<u32>)> {
        let openai = openai_api_rust::OpenAI::new(
            Auth {
                api_key: self.api_key.clone(),
                organization: None,
            },
            &self.api_base,
            &self.api_version,
            Duration::from_secs(120),
        );

        let (max_tokens, stop) = match mode {
            CompletionMode::Sql => (Some(150), Some(vec!["#".to_string(), ";".to_string()])),
            CompletionMode::Text => (Some(800), None),
        };

        let body = ChatBody {
            model: self.completion_model.to_string(),
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

        trace!("openai {:?} completion request: {:?}", mode, body);

        let resp = openai.chat_completion_create(&body).map_err(|e| {
            ErrorCode::Internal(format!(
                "openai {:?} completion request error: {:?}",
                mode, e
            ))
        })?;
        trace!("openai {:?} completion response: {:?}", mode, resp);

        let usage = resp.usage.total_tokens;
        let result = if resp.choices.is_empty() {
            "".to_string()
        } else {
            let message = resp
                .choices
                .first()
                .and_then(|choice| choice.message.as_ref());

            match message {
                Some(msg) => msg.content.clone(),
                _ => "".to_string(),
            }
        };

        // perf.
        {
            metrics_completion_count(1);
            metrics_completion_token(usage.unwrap_or(0));
        }

        Ok((result, usage))
    }
}
