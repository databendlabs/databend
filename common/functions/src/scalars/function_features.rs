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

use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FunctionFeatures {
    pub is_deterministic: bool,
    pub negative_function_name: Option<String>,
    pub is_context_func: bool,
    pub maybe_monotonic: bool,

    /// Whether the function passes through null input.
    /// True if the function just return null with any given null input.
    /// False if the function may return non-null with null input.
    ///
    /// For example, arithmetic plus('+') will output null for any null input, like '12 + null = null'.
    /// It has no idea of how to handle null, but just pass through.
    ///
    /// While IS_NULL function  treats null input as a valid one. For example IS_NULL(NULL, 'test') will return 'test'.
    pub passthrough_null: bool,

    // The number of arguments the function accepts.
    pub num_arguments: usize,
    // (1, 2) means we only accept [1, 2] arguments
    // None means it's not variadic function.
    pub variadic_arguments: Option<(usize, usize)>,
}

impl FunctionFeatures {
    pub fn default() -> FunctionFeatures {
        FunctionFeatures {
            is_deterministic: false,
            negative_function_name: None,
            is_context_func: false,
            maybe_monotonic: false,
            passthrough_null: true,
            num_arguments: 0,
            variadic_arguments: None,
        }
    }

    pub fn deterministic(mut self) -> FunctionFeatures {
        self.is_deterministic = true;
        self
    }

    pub fn negative_function(mut self, negative_name: &str) -> FunctionFeatures {
        self.negative_function_name = Some(negative_name.to_string());
        self
    }

    pub fn context_function(mut self) -> FunctionFeatures {
        self.is_context_func = true;
        self
    }

    pub fn monotonicity(mut self) -> FunctionFeatures {
        self.maybe_monotonic = true;
        self
    }

    pub fn disable_passthrough_null(mut self) -> FunctionFeatures {
        self.passthrough_null = false;
        self
    }

    pub fn variadic_arguments(mut self, min: usize, max: usize) -> FunctionFeatures {
        self.variadic_arguments = Some((min, max));
        self
    }

    pub fn num_arguments(mut self, num_arguments: usize) -> FunctionFeatures {
        self.num_arguments = num_arguments;
        self
    }
}
