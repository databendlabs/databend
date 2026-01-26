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

use anyerror::AnyError;

/// Error occurs when creating a meta client.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum CreationError {
    #[error("meta-client dedicated runtime error: {0}")]
    RuntimeError(AnyError),

    #[error("meta-client config error: {0}")]
    ConfigError(AnyError),
}

impl CreationError {
    pub fn new_runtime_error(msg: impl ToString) -> Self {
        Self::RuntimeError(AnyError::error(msg))
    }

    pub fn new_config_error(msg: impl ToString) -> Self {
        Self::ConfigError(AnyError::error(msg))
    }

    pub fn context(self, ctx: impl ToString) -> Self {
        let ctx = ctx.to_string();

        match self {
            Self::RuntimeError(e) => Self::RuntimeError(e.add_context(|| ctx)),
            Self::ConfigError(e) => Self::ConfigError(e.add_context(|| ctx)),
        }
    }
}
