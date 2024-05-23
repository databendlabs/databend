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

use std::fmt::Display;
use std::fmt::Write;

use super::FormatTreeNode;
use crate::Result;

static REGULAR_PREFIX: &str = "├── ";
static CHILD_PREFIX: &str = "│   ";
static LAST_REGULAR_PREFIX: &str = "└── ";
static LAST_CHILD_PREFIX: &str = "    ";

impl<T> FormatTreeNode<T>
where T: Display + Clone
{
    /// format TreeNode in a pretty way
    pub fn format_pretty(&self) -> Result<String> {
        let mut buf = String::new();
        self.format_pretty_impl("".to_string(), "".to_string(), &mut buf)?;
        Ok(buf)
    }

    /// Format TreeNode in a pretty way, with given prefix.
    pub fn format_pretty_with_prefix(&self, prefix: &str) -> Result<String> {
        let mut buf = String::new();
        self.format_pretty_impl(prefix.to_string(), prefix.to_string(), &mut buf)?;
        Ok(buf)
    }

    fn format_pretty_impl(
        &self,
        prefix: String,
        child_prefix: String,
        f: &mut String,
    ) -> Result<()> {
        writeln!(f, "{}{}", prefix, &self.payload).unwrap();
        if let Some((last_child, children)) = self.children.split_last() {
            for child in children {
                child.format_pretty_impl(
                    (child_prefix.clone() + REGULAR_PREFIX).clone(),
                    (child_prefix.clone() + CHILD_PREFIX).clone(),
                    f,
                )?;
            }
            last_child.format_pretty_impl(
                child_prefix.clone() + LAST_REGULAR_PREFIX,
                child_prefix + LAST_CHILD_PREFIX,
                f,
            )?;
        }
        Ok(())
    }
}
