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

/// Implement `Display` for `Option<T>` if T is `Display`.
///
/// It outputs a literal string `"None"` if it is None. Otherwise it invokes the Display
/// implementation for T.
pub struct DisplayOption<'a, T: fmt::Display>(pub &'a Option<T>);

impl<'a, T: fmt::Display> fmt::Display for DisplayOption<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            None => {
                write!(f, "None")
            }
            Some(x) => x.fmt(f),
        }
    }
}

pub trait DisplayOptionExt<'a, T: fmt::Display> {
    fn display(&'a self) -> DisplayOption<'a, T>;
}

impl<T> DisplayOptionExt<'_, T> for Option<T>
where T: fmt::Display
{
    fn display(&self) -> DisplayOption<T> {
        DisplayOption(self)
    }
}
