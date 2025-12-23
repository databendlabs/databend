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
    ($input:expr_2021, $return_if_ok:literal, $($pat:pat => $body:expr_2021),+ $(,)?) => {{
        if let Some(token_0) = $input.tokens.first() {
            use TokenKind::*;

            if let Some(result) = match token_0.kind {
                $($pat => Some($body),)+
                _ => None,
            } {
                if !$return_if_ok || result.is_ok() {
                    return result;
                }
            }
        }
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
pub use error::Backtrace;
pub use error::Error;
pub use error::ErrorKind;
pub use error::display_parser_error;
pub use error_suggestion::suggest_correction;
pub use input::Dialect;
pub use input::Input;
pub use input::ParseMode;
pub use parser::*;
pub use token::all_reserved_keywords;
