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

use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;

pub struct FormatVec(pub Vec<String>);

impl Display for FormatVec {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        let mut comma_separated = String::new();

        for s in &self.0[0..self.0.len() - 1] {
            comma_separated.push_str(s);
            comma_separated.push_str(", ");
        }

        comma_separated.push_str(&self.0[self.0.len() - 1]);
        write!(f, "{}", comma_separated)
    }
}
