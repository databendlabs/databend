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

// The servers module used for external communication with user, such as MySQL wired protocol, etc.

use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use regex::Regex;

pub type LazyBlockFunc = fn(&str) -> Option<(TableSchemaRef, DataBlock)>;

pub struct FederatedHelper {}

impl FederatedHelper {
    pub(crate) fn block_match_rule(
        query: &str,
        rules: &[(Regex, Option<(TableSchemaRef, DataBlock)>)],
    ) -> Option<(TableSchemaRef, DataBlock)> {
        for (regex, data) in rules.iter() {
            if regex.is_match(query) {
                return match data {
                    None => Some((TableSchemaRefExt::create(vec![]), DataBlock::empty())),
                    Some((schema, data_block)) => Some((schema.clone(), data_block.clone())),
                };
            }
        }

        None
    }

    pub fn lazy_block_match_rule(
        query: &str,
        rules: &[(Regex, LazyBlockFunc)],
    ) -> Option<(TableSchemaRef, DataBlock)> {
        for (regex, func) in rules.iter() {
            if regex.is_match(query) {
                return match func(query) {
                    None => Some((TableSchemaRefExt::create(vec![]), DataBlock::empty())),
                    Some((schema, data_block)) => Some((schema, data_block)),
                };
            }
        }
        None
    }
}
