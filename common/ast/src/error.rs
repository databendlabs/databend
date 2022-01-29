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

use thiserror::Error;

// TODO: implement From<Result> for `common_exception::Result`.s
pub type Result<T> = std::result::Result<T, Error>;

/// Error is the error type for the common-ast crate.
#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
    #[error("unable to recognise the token from SQL: (rest {rest}, position {position})")]
    UnrecognisedToken { rest: String, position: usize },
}
