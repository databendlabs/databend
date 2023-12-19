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

use xorf::BinaryFuse8;
use databend_common_expression::{DataBlock, Evaluator, Expr, FunctionContext, Value};
use databend_common_expression::types::AnyType;
use databend_common_exception::Result;

#[derive(Clone, Debug)]
pub struct RuntimeFilterInfo {
    inlist: Vec<Box<Expr<String>>>,
    bloom: Vec<Box<(String, BinaryFuse8)>>,
}


impl RuntimeFilterInfo {
    pub fn new() -> Self {
        RuntimeFilterInfo {
            inlist: vec![],
            bloom: vec![],
        }
    }

    pub fn add_inlist(&mut self, expr: Expr<String>) {
        self.inlist.push(Box::new(expr));
    }

    pub fn add_bloom(&mut self, bloom: Box<(String, BinaryFuse8)>) {
        self.bloom.push(bloom);
    }

    pub fn get_inlist(&self) -> &Vec<Box<Expr<String>>> {
        &self.inlist
    }

    pub fn get_bloom(&self) -> &Vec<Box<(String, BinaryFuse8)>> {
        &self.bloom
    }

    pub fn blooms(self) -> Vec<Box<(String, BinaryFuse8)>> {
        self.bloom
    }

    pub fn is_empty(&self) -> bool {
        self.inlist.is_empty() && self.bloom.is_empty()
    }
}