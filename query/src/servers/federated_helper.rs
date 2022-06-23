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

// The servers module used for external communication with user, such as MySQL wired protocol, etc.

use common_datablocks::DataBlock;
use regex::bytes::RegexSet;
pub type LazyBlockFunc = fn(&str) -> Option<DataBlock>;

pub struct FederatedHelper {}

impl FederatedHelper {
    pub(crate) fn block_match_rule(
        query: &str,
        rules: Vec<(&str, Option<DataBlock>)>,
    ) -> Option<DataBlock> {
        let regex_rules = rules.iter().map(|x| x.0).collect::<Vec<_>>();
        let regex_set = RegexSet::new(&regex_rules).unwrap();
        let matches = regex_set.matches(query.as_ref());
        for (index, (_regex, data_block)) in rules.iter().enumerate() {
            if matches.matched(index) {
                return match data_block {
                    None => Some(DataBlock::empty()),
                    Some(data_block) => Some(data_block.clone()),
                };
            }
        }

        None
    }

    pub fn lazy_block_match_rule(
        query: &str,
        rules: Vec<(&str, LazyBlockFunc)>,
    ) -> Option<DataBlock> {
        let regex_rules = rules.iter().map(|x| x.0).collect::<Vec<_>>();
        let regex_set = RegexSet::new(&regex_rules).unwrap();
        let matches = regex_set.matches(query.as_ref());
        for (index, (_regex, func)) in rules.iter().enumerate() {
            if matches.matched(index) {
                return match func(query) {
                    None => Some(DataBlock::empty()),
                    Some(data_block) => Some(data_block),
                };
            }
        }
        None
    }
}
