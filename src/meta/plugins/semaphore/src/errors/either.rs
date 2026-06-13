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
use std::io;

/// Either is a type that can be either one of two types.
///
/// This is used to represent errors that can be either an io::Error or a String.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Either<A, B> {
    A(A),
    B(B),
}

impl fmt::Display for Either<io::Error, String> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Either::A(e) => write!(f, "{}", e),
            Either::B(s) => write!(f, "{}", s),
        }
    }
}

impl From<io::Error> for Either<io::Error, String> {
    fn from(e: io::Error) -> Self {
        Either::A(e)
    }
}

impl From<String> for Either<io::Error, String> {
    fn from(s: String) -> Self {
        Either::B(s)
    }
}

impl From<&str> for Either<io::Error, String> {
    fn from(s: &str) -> Self {
        Either::B(s.to_string())
    }
}
