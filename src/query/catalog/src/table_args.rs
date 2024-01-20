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

use std::collections::HashMap;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use log::debug;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TableArgs {
    pub positioned: Vec<Scalar>,
    pub named: HashMap<String, Scalar>,
}

impl TableArgs {
    pub fn is_empty(&self) -> bool {
        self.named.is_empty() && self.positioned.is_empty()
    }

    pub fn new_positioned(args: Vec<Scalar>) -> Self {
        Self {
            positioned: args,
            named: HashMap::new(),
        }
    }

    pub fn new_named(args: HashMap<String, Scalar>) -> Self {
        Self {
            positioned: vec![],
            named: args,
        }
    }

    /// Check TableArgs only contain positioned args.
    /// Also check num of positioned if num is not None.
    ///
    /// Returns the vec of positioned args.
    pub fn expect_all_positioned(
        &self,
        func_name: &str,
        num: Option<usize>,
    ) -> Result<Vec<Scalar>> {
        if !self.named.is_empty() {
            Err(ErrorCode::BadArguments(format!(
                "{} accept positioned args only",
                func_name
            )))
        } else if let Some(n) = num {
            if n != self.positioned.len() {
                Err(ErrorCode::BadArguments(format!(
                    "{} must accept exactly {} positioned args",
                    func_name, n
                )))
            } else {
                Ok(self.positioned.clone())
            }
        } else {
            Ok(self.positioned.clone())
        }
    }

    pub fn expect_all_strings(args: Vec<Scalar>) -> Result<Vec<String>> {
        args.into_iter()
            .map(|arg| {
                let arg = arg
                    .into_string()
                    .map_err(|_| ErrorCode::BadArguments("Expected string argument"))?;

                Ok(String::from_utf8(arg)?)
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Check TableArgs only contain named args.
    ///
    /// Returns the map of named args.
    pub fn expect_all_named(&self, func_name: &str) -> Result<HashMap<String, Scalar>> {
        if !self.positioned.is_empty() {
            debug!("{:?} accept named args only", self.positioned);
            Err(ErrorCode::BadArguments(format!(
                "{} accept named args only",
                func_name
            )))
        } else {
            Ok(self.named.clone())
        }
    }
}
