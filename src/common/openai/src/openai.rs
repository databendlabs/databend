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

pub enum AIModel {
    TextDavinci003,
    TextEmbeddingAda003,
}

// https://platform.openai.com/examples
impl ToString for AIModel {
    fn to_string(&self) -> String {
        match self {
            AIModel::TextDavinci003 => "text-davinci-003".to_string(),
            AIModel::TextEmbeddingAda003 => "text-embedding-ada-002".to_string(),
        }
    }
}

pub struct OpenAI {
    pub(crate) api_key: String,
    pub(crate) api_base: String,
    pub(crate) model: AIModel,
}

impl OpenAI {
    pub fn create(api_key: String, model: AIModel) -> Self {
        OpenAI {
            api_key,
            api_base: "https://api.openai.com/v1/".to_string(),
            model,
        }
    }
}
