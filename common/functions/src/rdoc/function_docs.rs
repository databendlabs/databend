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
    // The syntax of the function, aka: syntax.
    pub syntax: &'static str,
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
    sp!(syntax, &'static str);
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

#[macro_export]
macro_rules! doc {
    ( $($key:ident => $value:expr),+ ) => {
        {
            let mut m = FunctionDocs::default();
            $(
                crate::set_doc!(&mut m, $key, $value);
            )+
            m
        }
     };
}

#[macro_export]
macro_rules! set_doc {
    ($doc: expr, category, $v: expr) => {{
        $doc.category = $v;
    }};

    ($doc: expr, description, $v: expr) => {{
        $doc.description = $v;
    }};

    ($doc: expr, syntax, $v: expr) => {{
        $doc.syntax = $v;
    }};

    ($doc: expr, return_type, $v: expr) => {{
        $doc.syntax = $v;
    }};

    ($doc: expr, arg, $v: expr) => {{
        $doc.args.push(($v.0, $v.1));
    }};

    ($doc: expr, example, $v: expr) => {{
        $doc.examples.push($v);
    }};
}

#[cfg(test)]
mod test {
    use super::FunctionDocs;

    #[test]
    fn iw() {
        let doc = doc! {
            category => "Conditional",
            description => "If x is null, then y is returned.",
            syntax => "ifnull(x, y, z)",
            return_type => "Combination type of y and z",
            arg => ("x", "any type"),
            arg => ("y", "any type"),
            arg => ("z", "any type"),
            example =>  "ifnull(3, 2, 1)",
            example =>  "ifnull(null, 2, 1)"
        };

        println!("c {:?}", doc);

        let doc = FunctionDocs::default().description("If expr1 is TRUE, IF() returns expr2. Otherwise, it returns expr3.")
                .syntax("IF(expr1,expr2,expr3)")
                .add_arg(
                    "expr1",
                    "The condition for evaluation that can be true or false.",
                )
                .add_arg("expr2", "The expression to return if condition is met.")
                .add_arg("expr3", "The expression to return if condition is not met.")
                .return_type("The return type is determined by expr2 and expr3, they must have the lowest common type.")
                .add_example("select if(number=0, true, false) from numbers(1);");

        println!("{:?}", doc);
        println!("{:?}", serde_json::to_string(&doc).unwrap());
    }
}
