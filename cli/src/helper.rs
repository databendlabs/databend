// Copyright 2023 Datafuse Labs.
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

use std::borrow::Cow;

use rustyline::completion::Completer;
use rustyline::completion::FilenameCompleter;
use rustyline::completion::Pair;
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::ValidationContext;
use rustyline::validate::ValidationResult;
use rustyline::validate::Validator;
use rustyline::Context;
use rustyline::Helper;
use rustyline::Result;

use crate::ast::all_reserved_keywords;
use crate::ast::tokenize_sql;
use crate::ast::TokenKind;

pub struct CliHelper {
    completer: FilenameCompleter,
}

impl CliHelper {
    pub fn new() -> Self {
        Self {
            completer: FilenameCompleter::new(),
        }
    }
}

impl Highlighter for CliHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        let tokens = tokenize_sql(line);
        let mut line = line.to_owned();

        if let Ok(tokens) = tokens {
            for token in tokens.iter().rev() {
                if TokenKind::is_keyword(&token.kind)
                    || TokenKind::is_reserved_ident(&token.kind, false)
                    || TokenKind::is_reserved_function_name(&token.kind)
                {
                    line.replace_range(
                        token.span.clone(),
                        &format!("\x1b[1;32m{}\x1b[0m", token.text()),
                    );
                } else if TokenKind::is_literal(&token.kind) {
                    line.replace_range(
                        token.span.clone(),
                        &format!("\x1b[1;33m{}\x1b[0m", token.text()),
                    );
                }
            }
        }

        Cow::Owned(line)
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> std::borrow::Cow<'b, str> {
        let _ = default;
        std::borrow::Cow::Borrowed(prompt)
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> std::borrow::Cow<'h, str> {
        std::borrow::Cow::Owned("\x1b[1m".to_owned() + hint + "\x1b[m")
    }

    fn highlight_candidate<'c>(
        &self,
        candidate: &'c str, // FIXME should be Completer::Candidate
        completion: rustyline::CompletionType,
    ) -> std::borrow::Cow<'c, str> {
        let _ = completion;
        std::borrow::Cow::Borrowed(candidate)
    }

    fn highlight_char(&self, line: &str, _pos: usize) -> bool {
        !line.is_empty()
    }
}

impl Hinter for CliHelper {
    type Hint = String;
}

impl Completer for CliHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &Context<'_>,
    ) -> std::result::Result<(usize, Vec<Pair>), ReadlineError> {
        let keyword_candidates = KeyWordCompleter::complete(line, pos);
        if !keyword_candidates.1.is_empty() {
            return Ok(keyword_candidates);
        }
        self.completer.complete(line, pos, ctx)
    }
}

impl Validator for CliHelper {
    fn validate(&self, ctx: &mut ValidationContext<'_>) -> Result<ValidationResult> {
        let input = ctx.input().trim_end();
        if input.strip_suffix('\\').is_some() {
            Ok(ValidationResult::Incomplete)
        } else {
            Ok(ValidationResult::Valid(None))
        }
    }
}

impl Helper for CliHelper {}

struct KeyWordCompleter {}

impl KeyWordCompleter {
    fn complete(s: &str, pos: usize) -> (usize, Vec<Pair>) {
        let hint = s.split(|p: char| p.is_whitespace()).last().unwrap_or(s);
        let all_keywords = all_reserved_keywords();
        let res: (usize, Vec<Pair>) = (
            pos - hint.len(),
            all_keywords
                .iter()
                .filter(|keyword| keyword.starts_with(&hint.to_ascii_uppercase()))
                .map(|keyword| Pair {
                    display: keyword.to_string(),
                    replacement: keyword.to_string(),
                })
                .collect(),
        );
        res
    }
}
