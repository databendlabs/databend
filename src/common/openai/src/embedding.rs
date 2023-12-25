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
use openai_api_rust::embeddings::EmbeddingsApi;
use openai_api_rust::embeddings::EmbeddingsBody;
use openai_api_rust::Auth;

use crate::OpenAI;

impl OpenAI {
    #[allow(clippy::type_complexity)]
    pub fn embedding_request(&self, input: &[String]) -> Result<(Vec<Vec<f32>>, Option<u32>)> {
        let openai = openai_api_rust::OpenAI::new(
            Auth {
                api_key: self.api_key.clone(),
                organization: None,
            },
            &self.api_base,
            &self.api_version,
            Duration::from_secs(120),
        );
        let body = EmbeddingsBody {
            model: self.embedding_model.to_string(),
            input: input.to_vec(),
            user: None,
        };
        let resp = openai
            .embeddings_create(&body)
            .map_err(|e| ErrorCode::Internal(format!("openai embedding request error: {:?}", e)))?;

        let usage = resp.usage.total_tokens;
        let embeddings = resp.data.unwrap_or(vec![]);

        let mut res = Vec::with_capacity(embeddings.len());
        for data in embeddings {
            if let Some(vec) = data.embedding {
                res.push(vec);
            } else {
                res.push(vec![]);
            }
        }

        // perf.
        {
            metrics_embedding_count(1);
            metrics_embedding_token(usage.unwrap_or(0));
        }

        Ok((res, usage))
    }
}
