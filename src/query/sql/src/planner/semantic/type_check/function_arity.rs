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

use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

pub(super) fn check_function_arity(
    span: Span,
    func_name: &str,
    len: usize,
    min: usize,
    max: Option<usize>,
) -> Result<()> {
    if len >= min && max.map(|max| len <= max).unwrap_or(true) {
        return Ok(());
    }

    let expected = match max {
        Some(max) if min == max => format!("{min} {}", argument_word(min)),
        Some(max) => format!("{min} to {max} arguments"),
        None => format!("at least {min} {}", argument_word(min)),
    };
    Err(
        ErrorCode::SemanticError(format!("{func_name} expects {expected}, but got {len}"))
            .set_span(span),
    )
}

fn argument_word(n: usize) -> &'static str {
    if n == 1 { "argument" } else { "arguments" }
}
