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

use std::ops::Bound;

/// Builds a key range from a starting key and an optional end key.
///
/// # Arguments
/// * `key` - The starting key of the range (inclusive bound)
/// * `key_end` - The optional end key of the range (exclusive bound if provided)
///
/// # Returns
/// `(Bound<K>, Bound<K>)` as a range of keys.
///
/// # Examples
/// ```
/// // Single key range
/// let range = build_key_range(&"a", &None)?; // (Included("a"), Included("a"))
///
/// // Key range from "a" to "b" (excluding "b")
/// let range = build_key_range(&"a", &Some("b"))?; // (Included("a"), Excluded("b"))
/// ```
pub fn build_key_range<K>(
    key: &K,
    key_end: &Option<K>,
) -> Result<(Bound<K>, Bound<K>), &'static str>
where
    K: Clone + Ord,
{
    let left = Bound::Included(key.clone());

    match key_end {
        Some(key_end) => {
            if key >= key_end {
                return Err("empty range");
            }
            Ok((left, Bound::Excluded(key_end.clone())))
        }
        None => Ok((left.clone(), left)),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_build_key_range() -> Result<(), &'static str> {
        let x = build_key_range(&s("a"), &None)?;
        assert_eq!(x, (Bound::Included(s("a")), Bound::Included(s("a"))));

        let x = build_key_range(&s("a"), &Some(s("b")))?;
        assert_eq!(x, (Bound::Included(s("a")), Bound::Excluded(s("b"))));

        let x = build_key_range(&s("a"), &Some(s("a")));
        assert_eq!(x, Err("empty range"));

        Ok(())
    }

    fn s(x: impl ToString) -> String {
        x.to_string()
    }
}
