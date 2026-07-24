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

macro_rules! try_dispatch {
    ($input:expr_2021, true, $($($kind:ident)|+ => $body:expr_2021),+ $(,)?) => {{
        let mut dispatch_error = None;
        if let Some(token_0) = $input.tokens.first() {
            if let Some(result) = match token_0.kind {
                $($($kind)|+ => Some($body),)+
                _ => None,
            } {
                match result {
                    Ok(output) => return Ok(output),
                    Err(nom::Err::Error(error) | nom::Err::Failure(error)) => {
                        dispatch_error = Some(error);
                    }
                    Err(nom::Err::Incomplete(needed)) => {
                        return Err(nom::Err::Incomplete(needed));
                    }
                }
            }
        }
        dispatch_error
    }};
    ($input:expr_2021, false, $($($kind:ident)|+ => $body:expr_2021),+ $(,)?) => {{
        if let Some(token_0) = $input.tokens.first() {
            if let Some(result) = match token_0.kind {
                $($($kind)|+ => Some($body),)+
                _ => None,
            } {
                return result;
            }
        }
        Err(nom::Err::Error(try_dispatch!(
            @error $input, $($($kind)|+),+
        )))
    }};
    (@error $input:expr_2021, $($($kind:ident)|+),+ $(,)?) => {{
        const EXPECTED_TOKENS: &[$crate::parser::token::TokenKind] = &[$($($kind,)+)+];
        $crate::parser::Error::from_expected_tokens($input, EXPECTED_TOKENS)
    }};
}

mod comment;
mod common;
mod copy;
mod data_mask;
pub mod dynamic_table;
mod error;
mod error_suggestion;
pub mod expr;
mod input;
#[allow(clippy::module_inception)]
mod parser;
pub mod query;
pub mod script;
mod sequence;
mod stage;
pub mod statement;
pub mod stream;
pub mod token;

pub use common::IResult;
pub use common::match_text;
pub use common::match_token;
pub use error::Error;
pub use error::ErrorKind;
pub use error::display_parser_error;
pub use error_suggestion::suggest_correction;
pub use input::Dialect;
pub use input::Input;
pub use input::ParseMode;
pub use parser::*;
pub use token::all_reserved_keywords;
