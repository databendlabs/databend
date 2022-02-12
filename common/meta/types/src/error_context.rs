// Copyright 2021 Datafuse Labs.
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

use std::error::Error;

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, thiserror::Error)]
#[error("{err} while {context}")]
pub struct ErrorWithContext<E> {
    #[source]
    pub err: E,
    pub context: String,
}

impl<E> ErrorWithContext<E> {
    pub fn new(err: E) -> Self {
        Self {
            err,
            context: "".to_string(),
        }
    }

    pub fn with_context<S, F>(err: E, context: F) -> Self
    where
        S: ToString,
        F: FnOnce() -> S,
    {
        Self {
            err,
            context: context().to_string(),
        }
    }
}

pub trait WithContext<T, E>
where E: Error
{
    fn context<S, F>(self, f: F) -> Result<T, ErrorWithContext<E>>
    where
        S: ToString,
        F: FnOnce() -> S;
}

impl<T, E> WithContext<T, E> for Result<T, E>
where E: Error
{
    fn context<S, F>(self, f: F) -> Result<T, ErrorWithContext<E>>
    where
        S: ToString,
        F: FnOnce() -> S,
    {
        self.map_err(|e| ErrorWithContext::with_context(e, f))
    }
}
