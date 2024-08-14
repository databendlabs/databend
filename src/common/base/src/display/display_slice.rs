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

use std::fmt;

/// Implement `Display` for `&[T]` if T is `Display`.
///
/// It outputs at most `MAX` elements, excluding those from the 5th to the second-to-last one:
/// - `DisplaySlice(&[1,2,3,4,5,6])` outputs: `"[1,2,3,4,...,6]"`.
pub struct DisplaySlice<'a, T: fmt::Display, const MAX: usize = 5>(pub &'a [T]);

impl<'a, T: fmt::Display, const MAX: usize> fmt::Display for DisplaySlice<'a, T, MAX> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let slice = self.0;
        let len = slice.len();

        write!(f, "[")?;

        if len > MAX {
            for (i, t) in slice[..(MAX - 1)].iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }

                write!(f, "{}", t)?;
            }

            write!(f, ",..,")?;
            write!(f, "{}", slice.last().unwrap())?;
        } else {
            for (i, t) in slice.iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }

                write!(f, "{}", t)?;
            }
        }

        write!(f, "]")
    }
}

pub trait DisplaySliceExt<'a, T: fmt::Display> {
    fn display(&'a self) -> DisplaySlice<'a, T>;

    /// Display at most `MAX` elements.
    fn display_n<const MAX: usize>(&'a self) -> DisplaySlice<'a, T, MAX>;
}

impl<T> DisplaySliceExt<'_, T> for [T]
where T: fmt::Display
{
    fn display(&self) -> DisplaySlice<T> {
        DisplaySlice(self)
    }

    fn display_n<const MAX: usize>(&'_ self) -> DisplaySlice<'_, T, MAX> {
        DisplaySlice(self)
    }
}

#[cfg(test)]
mod tests {
    use super::DisplaySlice;

    #[test]
    fn test_display_slice() {
        let a = vec![1, 2, 3, 4];
        assert_eq!("[1,2,3,4]", DisplaySlice::<_>(&a).to_string());

        let a = vec![1, 2, 3, 4, 5];
        assert_eq!("[1,2,3,4,5]", DisplaySlice::<_>(&a).to_string());

        let a = vec![1, 2, 3, 4, 5, 6];
        assert_eq!("[1,2,3,4,..,6]", DisplaySlice::<_>(&a).to_string());

        let a = vec![1, 2, 3, 4, 5, 6, 7];
        assert_eq!("[1,2,3,4,..,7]", DisplaySlice::<_>(&a).to_string());

        let a = vec![1, 2, 3, 4, 5, 6, 7];
        assert_eq!("[1,..,7]", DisplaySlice::<_, 2>(&a).to_string());
    }
}
