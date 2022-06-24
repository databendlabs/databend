// Copyright 2022 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::DataSchemaRefExt;
use once_cell::sync::Lazy;
use regex::Regex;

use super::CLICKHOUSE_VERSION;
use crate::servers::federated_helper::FederatedHelper;

pub struct CLickHouseFederated {}

static FORMAT_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r".*(?i)FORMAT\s*([[:alpha:]]*)\s*;?$").unwrap());

impl CLickHouseFederated {
    // Build block for select function.
    // Format:
    // |function_name()|
    // |value|
    fn select_function_block(name: &str, value: &str) -> Option<DataBlock> {
        Some(DataBlock::create(
            DataSchemaRefExt::create(vec![DataField::new(name, StringType::new_impl())]),
            vec![Series::from_data(vec![value])],
        ))
    }

    pub fn get_format(query: &str) -> Option<String> {
        match FORMAT_REGEX.captures(query) {
            Some(x) => x.get(1).map(|s| s.as_str().to_owned()),
            None => None,
        }
    }

    pub fn check(query: &str) -> Option<DataBlock> {
        let rules: Vec<(&str, Option<DataBlock>)> = vec![(
            "(?i)^(SELECT VERSION()(.*))",
            Self::select_function_block("version()", CLICKHOUSE_VERSION),
        )];
        FederatedHelper::block_match_rule(query, rules)
    }
}
