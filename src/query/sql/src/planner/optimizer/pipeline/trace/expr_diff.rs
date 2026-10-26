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

use similar::ChangeTag;
use similar::TextDiff;

pub(super) fn diff_text(before: &str, after: &str) -> String {
    if before == after {
        return "No differences found.".to_string();
    }
    let diff = TextDiff::from_lines(before, after);
    let mut output = String::new();
    for change in diff.iter_all_changes() {
        let sign = match change.tag() {
            ChangeTag::Delete => "-",
            ChangeTag::Insert => "+",
            ChangeTag::Equal => " ",
        };
        output.push_str(&format!("{}{}", sign, change));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_diff_preserves_trace_diagnostics() {
        assert_eq!(diff_text("same\n", "same\n"), "No differences found.");
        assert_eq!(
            diff_text("old\nsame\n", "new\nsame\n"),
            "-old\n+new\n same\n"
        );
    }
}
