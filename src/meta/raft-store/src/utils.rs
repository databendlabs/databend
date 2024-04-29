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
///
/// If the right bound can not be built, return None.
pub fn prefix_right_bound(p: &str) -> Option<String> {
    let mut chars = p.chars().collect::<Vec<_>>();

    // Start from the end of the character list and look for the first character that is not \u{10FFFF}
    for i in (0..chars.len()).rev() {
        if chars[i] as u32 != 0x10FFFF {
            // Try to increment the character
            if let Some(next_char) = char::from_u32(chars[i] as u32 + 1) {
                chars[i] = next_char;
                // Remove all characters after the incremented one
                chars.truncate(i + 1);
                return Some(chars.iter().collect());
            } else {
                // If incrementing results in an invalid character, return None
                return None;
            }
        }
    }

    // If all characters are \u{10FFFF} or the string is empty, return None
    None
}

#[cfg(test)]
mod tests {
    use super::prefix_right_bound;

    #[test]
    fn test_prefix_right_bound_last_unicode() {
        // Test with the highest possible Unicode character
        assert_eq!(prefix_right_bound("\u{10FFFF}"), None);
        assert_eq!(prefix_right_bound("\u{10FFFF}\u{10FFFF}"), None);
        assert_eq!(prefix_right_bound("a\u{10FFFF}"), Some(s("b")));
        assert_eq!(prefix_right_bound("a\u{10FFFF}\u{10FFFF}"), Some(s("b")));
        assert_eq!(prefix_right_bound("aa\u{10FFFF}"), Some(s("ab")));
        assert_eq!(prefix_right_bound("aa\u{10FFFF}\u{10FFFF}"), Some(s("ab")));
        assert_eq!(
            prefix_right_bound("aa\u{10FFFF}\u{10FFFF}\u{10FFFF}"),
            Some(s("ab"))
        );
    }

    #[test]
    fn test_next_string() {
        assert_eq!(Some(s("b")), prefix_right_bound("a"));
        assert_eq!(Some(s("{")), prefix_right_bound("z"));
        assert_eq!(Some(s("foo0")), prefix_right_bound("foo/"));
        assert_eq!(Some(s("foo💰")), prefix_right_bound("foo💯"));
    }

    #[test]
    fn test_prefix_right_bound_basic() {
        // Basic functionality test
        assert_eq!(prefix_right_bound("abc"), Some(s("abd")));
    }

    #[test]
    fn test_prefix_right_bound_empty() {
        // Test with an empty string
        assert_eq!(prefix_right_bound(""), None);
    }

    #[test]
    fn test_prefix_right_bound_unicode() {
        // Test with Unicode characters
        assert_eq!(prefix_right_bound("😀"), Some(s("😁")));
    }

    #[test]
    fn test_prefix_right_bound_increment() {
        // Test the boundary condition where the last character increments to the next logical Unicode character
        assert_eq!(prefix_right_bound("a"), Some(s("b")));
        assert_eq!(prefix_right_bound("z"), Some(s("{"))); // Note: 'z' + 1 = '{' in ASCII
    }

    #[test]
    fn test_prefix_right_bound_non_ascii() {
        // Test with non-ASCII characters
        assert_eq!(prefix_right_bound("ñ"), Some(s("\u{00f2}")));
    }

    #[test]
    fn test_prefix_right_bound_complex_string() {
        // Test with strings that require more complex boundary adjustments
        assert_eq!(prefix_right_bound("hello!"), Some(s("hello\"")));
    }

    fn s(s: impl ToString) -> String {
        s.to_string()
    }
}
