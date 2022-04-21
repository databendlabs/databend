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

use std::ops::Range;

use thiserror::Error;

// TODO: implement From<Result> for `common_exception::Result`.s
pub type Result<T> = std::result::Result<T, Error>;

/// Error is the error type for the common-ast crate.
#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
    UnrecognisedToken {
        source: String,
        span: Range<usize>,
    },
    InvalidSyntax {
        source: String,
        lables: Vec<(Range<usize>, String)>,
    },
}

pub fn pretty_print_error<'a>(source: &'a str, lables: Vec<(Range<usize>, String)>) -> String {
    let mut writer = Buffer::no_color();
    let file = SimpleFile::new("SQL", source);
    let config = Config {
        chars: Chars::ascii(),
        before_label_lines: 3,
        ..Default::default()
    };

    let error = match error {
        nom::Err::Error(error) | nom::Err::Failure(error) => error,
        nom::Err::Incomplete(_) => unreachable!(),
    };

    let diagnostic = Diagnostic::error().with_labels(lables);

    term::emit(&mut writer, &config, &file, &diagnostic).unwrap();

    std::str::from_utf8(&writer.into_inner())
        .unwrap()
        .to_string()
}
