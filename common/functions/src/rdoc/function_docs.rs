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
    pub category: String,
    // Introduce the function in brief.
    pub description: String,
    // The syntax of the function, aka: syntax.
    pub syntax: String,
    pub args: Vec<(String, String)>,
    pub return_type: String,
    // Example SQL of the function that can be run directly in query.
    pub example: String,

    pub markdown: String,
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
    sp!(category, String);
    sp!(description, String);
    sp!(syntax, String);
    sp!(return_type, String);

    pub fn add_arg(mut self, name: String, description: String) -> FunctionDocs {
        self.args.push((name, description));
        self
    }

    pub fn markdown(mut self, markdown: String) -> FunctionDocs {
        self.markdown = markdown;
        self
    }
}

#[allow(dead_code)]
#[macro_export]
macro_rules! doc {
    ( $($key:ident => $value:expr),+ ) => {
        {
            let mut m = FunctionDocs::default();
            $(
                $crate::set_doc!(&mut m, $key, $value);
            )+
            m
        }
     };
}

#[allow(dead_code)]
#[macro_export]
macro_rules! set_doc {
    ($doc: expr, category, $v: expr) => {{
        $doc.category = $v.to_string();
    }};

    ($doc: expr, description, $v: expr) => {{
        $doc.description = $v.to_string();
    }};

    ($doc: expr, syntax, $v: expr) => {{
        $doc.syntax = $v.to_string();
    }};

    ($doc: expr, markdown, $v: expr) => {{
        $doc.markdown = $v.to_string();
    }};

    ($doc: expr, return_type, $v: expr) => {{
        $doc.return_type = $v.to_string();
    }};

    ($doc: expr, arg, $v: expr) => {{
        $doc.args.push(($v.0.to_string(), $v.1.to_string()));
    }};

    ($doc: expr, example, $v: expr) => {{
        $doc.example = $v.to_string();
    }};
}
