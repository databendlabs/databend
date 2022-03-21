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

#[derive(Clone, Debug, PartialEq, Serialize, Default)]
pub struct FunctionDocs {
    // Function Category
    pub category: &'static str,
    // Introduce the function in brief.
    pub description: &'static str,
    // The definition of the function, aka: syntax.
    pub definition: &'static str,
    pub args: Vec<(&'static str, &'static str)>,
    pub return_type: &'static str,

    // Example SQL of the function that can be run directly in query.
    pub examples: Vec<&'static str>,

    pub more: &'static str,
}

// set properties for descriptions
macro_rules! sp {
    ($p :ident, $type: ty) => {
        pub fn $p(mut self, $p: $type) -> FunctionDocs {
            self.$p = $p;
            self
        }
    };
}

impl FunctionDocs {
    sp!(category, &'static str);
    sp!(description, &'static str);
    sp!(definition, &'static str);
    sp!(return_type, &'static str);

    pub fn add_arg(mut self, name: &'static str, description: &'static str) -> FunctionDocs {
        self.args.push((name, description));
        self
    }

    pub fn add_example(mut self, example: &'static str) -> FunctionDocs {
        self.examples.push(example);
        self
    }
}

#[cfg(test)]
mod test {}
