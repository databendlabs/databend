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

use databend_common_exception::Result;
use similar::ChangeTag;
use similar::TextDiff;

use crate::Metadata;
use crate::optimizer::ir::SExpr;
use crate::planner::format::FormatOptions;
use crate::planner::format::MetadataIdHumanizer;

impl SExpr {
    /// Compares this SExpr with another SExpr and returns a diff of their string representations.
    ///
    /// # Arguments
    /// * `other` - The other SExpr to compare with.
    /// * `metadata` - The metadata used for humanizing IDs.
    ///
    /// # Returns
    /// A string containing the diff between the two SExpr instances.
    pub fn diff(&self, other: &SExpr, metadata: &Metadata) -> Result<String> {
        let self_str = self.pretty_format(metadata)?;
        let other_str = other.pretty_format(metadata)?;

        if self_str == other_str {
            return Ok("No differences found.".to_string());
        }

        let diff = TextDiff::from_lines(&self_str, &other_str);

        let mut diff_output = String::new();
        for change in diff.iter_all_changes() {
            let sign = match change.tag() {
                ChangeTag::Delete => "-",
                ChangeTag::Insert => "+",
                ChangeTag::Equal => " ",
            };
            diff_output.push_str(&format!("{}{}", sign, change));
        }

        Ok(diff_output)
    }

    /// Formats this SExpr into a pretty-printed string using metadata.
    ///
    /// # Arguments
    /// * `metadata` - The metadata used for humanizing IDs.
    ///
    /// # Returns
    /// A Result containing the formatted SExpr string.
    pub fn pretty_format(&self, metadata: &Metadata) -> Result<String> {
        let options = FormatOptions { verbose: false };
        let humanizer = MetadataIdHumanizer::new(metadata, options);
        Ok(self.to_format_tree(&humanizer)?.format_pretty()?)
    }
}
