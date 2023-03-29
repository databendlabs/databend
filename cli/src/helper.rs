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
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        let mut pos = pos;

        let mut line = line.to_owned();
        while pos > 0 {
            match find_last_word(&line, pos) {
                Span::Keyword(start, end) => {
                    line.replace_range(
                        start..end,
                        &format!("\x1b[1;32m{}\x1b[0m", &line[start..end]),
                    );
                    pos = start;
                }
                Span::Literal(start, end) => {
                    line.replace_range(
                        start..end,
                        &format!("\x1b[1;37m{}\x1b[0m", &line[start..end]),
                    );
                    pos = start;
                }
                Span::None(start, _) => {
                    pos = start;
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

enum Span {
    Keyword(usize, usize),
    Literal(usize, usize),
    None(usize, usize),
}

fn find_last_word(line: &str, pos: usize) -> Span {
    if line.is_empty() {
        return Span::None(0, 0);
    }
    let mut pos = pos;
    if pos >= line.len() {
        pos = line.len();
    }

    while pos > 0 {
        if line.as_bytes()[pos - 1].is_ascii_whitespace() {
            pos -= 1;
        } else {
            break;
        }
    }

    let end = pos;
    while pos > 0 {
        if !line.as_bytes()[pos - 1].is_ascii_whitespace() {
            pos -= 1;
        } else {
            break;
        }
    }
    let keyword = &line[pos..end];
    if KEYWORDS
        .split('\n')
        .any(|s| s.eq_ignore_ascii_case(keyword))
    {
        Span::Keyword(pos, end)
    } else if (keyword.starts_with('\'') && keyword.starts_with('"'))
        || keyword.parse::<f64>().is_ok()
    {
        Span::Literal(pos, end)
    } else {
        Span::None(pos, end)
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

static KEYWORDS: &str = include_str!("keywords.txt");

impl KeyWordCompleter {
    fn complete(s: &str, pos: usize) -> (usize, Vec<Pair>) {
        let hint = s.split(|p: char| p.is_whitespace()).last().unwrap_or(s);
        let res: (usize, Vec<Pair>) = (
            pos - hint.len(),
            KEYWORDS
                .split('\n')
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
