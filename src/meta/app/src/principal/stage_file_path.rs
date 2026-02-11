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

use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::KeyCodec;

/// Identify a file path belonging to a stage.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StageFilePath {
    stage: String,
    path: String,
}

impl StageFilePath {
    pub fn new(stage: impl ToString, path: impl ToString) -> Self {
        Self {
            stage: stage.to_string(),
            path: path.to_string(),
        }
    }

    pub fn stage_name(&self) -> &str {
        &self.stage
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

impl KeyCodec for StageFilePath {
    fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
        b.push_str(&self.stage).push_str(&self.path)
    }

    fn decode_key(p: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
        let stage = p.next_str()?;
        let path = p.next_str()?;
        Ok(StageFilePath::new(stage, path))
    }
}
