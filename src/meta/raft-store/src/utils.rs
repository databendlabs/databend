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

/// Return the right bound of the prefix, so that `p..right` will cover all strings with prefix `p`.
pub fn prefix_right_bound(p: &str) -> Option<String> {
    let last = p.chars().last()?;
    let mut next_str = p[..p.len() - last.len_utf8()].to_owned();
    let next_char = char::from_u32(last as u32 + 1)?;
    next_str.push(next_char);
    Some(next_str)
}

#[cfg(test)]
mod tests {
    use super::prefix_right_bound;

    #[test]
    fn test_next_string() {
        assert_eq!(None, prefix_right_bound(""));
        assert_eq!(Some(s("b")), prefix_right_bound("a"));
        assert_eq!(Some(s("{")), prefix_right_bound("z"));
        assert_eq!(Some(s("foo0")), prefix_right_bound("foo/"));
        assert_eq!(Some(s("foo💰")), prefix_right_bound("foo💯"));
    }

    fn s(s: impl ToString) -> String {
        s.to_string()
    }
}
