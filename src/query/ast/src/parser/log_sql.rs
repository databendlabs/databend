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

use std::ops::Range;

use super::token::Token;
use super::token::TokenKind;

pub(super) const LITERAL_BUDGET: usize = 1024;
const KEEP: usize = 3;

/// One comma-separated run. A closed group of literals is itself one item
/// in its parent (e.g. one VALUES row, regardless of its column count).
#[derive(Default)]
struct Run {
    start: usize,
    count: usize,
    keep_end: usize,
    end: usize,
    has_item: bool,
    signed: bool,
    mixed: bool,
}

impl Run {
    fn item(&mut self, start: usize, end: usize) {
        if self.has_item {
            self.interrupt();
        }
        if self.count == 0 && !self.signed {
            self.start = start;
        }
        self.count += 1;
        self.end = end;
        if self.count == KEEP {
            self.keep_end = end;
        }
        self.has_item = true;
        self.signed = false;
    }

    fn interrupt(&mut self) {
        *self = Self {
            mixed: true,
            ..Self::default()
        };
    }
}

#[derive(Default)]
pub(super) struct LiteralRuns {
    run: Run,
    parents: Vec<(Run, TokenKind, usize)>,
    edits: Vec<(Range<usize>, String)>,
}

impl LiteralRuns {
    fn replace(&mut self, range: Range<usize>, marker: String) {
        // A list edit replaces edits it contains, but not edits in the mixed
        // item that ended the list. Most edits append to the end.
        if self.edits.last().is_none_or(|(r, _)| r.end <= range.start) {
            self.edits.push((range, marker));
        } else {
            let start = self.edits.partition_point(|(r, _)| r.start < range.start);
            let end = self.edits.partition_point(|(r, _)| r.start < range.end);
            self.edits.splice(start..end, [(range, marker)]);
        }
    }

    fn finish_run(&mut self) {
        let run = &self.run;
        if run.count > KEEP && run.end - run.start > LITERAL_BUDGET {
            self.replace(
                run.keep_end..run.end,
                format!(", ... (+{} more)", run.count - KEEP),
            );
        }
    }

    pub(super) fn observe(&mut self, token: &Token<'_>) {
        use TokenKind::*;
        let start = token.span.start();
        let end = token.span.end();
        match token.kind {
            LParen | LBracket => {
                self.parents
                    .push((std::mem::take(&mut self.run), token.kind, start));
            }
            RParen | RBracket => {
                let Some((parent, open, start)) = self.parents.pop() else {
                    self.run.interrupt();
                    return;
                };
                let matched = matches!((open, token.kind), (LParen, RParen) | (LBracket, RBracket));
                let literal = matched && !self.run.mixed && self.run.has_item;
                if matched {
                    self.finish_run();
                }
                self.run = parent;
                if literal {
                    self.run.item(start, end);
                } else {
                    if matched && !self.run.has_item {
                        self.finish_run();
                    }
                    self.run.interrupt();
                }
            }
            Comma if self.run.has_item => self.run.has_item = false,
            Plus | Minus if !self.run.has_item && !self.run.signed => {
                if self.run.count == 0 {
                    self.run.start = start;
                }
                self.run.signed = true;
            }
            LiteralInteger | LiteralFloat | PGLiteralHex | MySQLLiteralHex | NULL | TRUE
            | FALSE | LiteralString
                if token.kind != LiteralString || token.text().starts_with('\'') =>
            {
                self.run.item(start, end);
                if end - start > LITERAL_BUDGET {
                    let marker = if token.kind == LiteralString {
                        format!("'…' /* {} bytes */", end - start - 2)
                    } else {
                        format!("… /* {} bytes */", end - start)
                    };
                    self.replace(start..end, marker);
                }
            }
            _ => {
                // At a comma the previous items are complete, even if the
                // following item is a column or function rather than a literal.
                if !self.run.has_item
                    || token.kind.is_keyword()
                    || matches!(token.kind, EOI | SemiColon)
                {
                    self.finish_run();
                }
                self.run.interrupt();
            }
        }
    }

    pub(super) fn interrupt(&mut self) {
        self.parents.clear();
        self.run.interrupt();
    }

    /// A credential replacement takes precedence, even if it reaches back to
    /// the preceding option name. Drop any overlapping abbreviations.
    pub(super) fn drain_before(
        &mut self,
        end: usize,
    ) -> impl Iterator<Item = (Range<usize>, String)> + '_ {
        self.interrupt();
        self.edits.drain(..).filter(move |(r, _)| r.end <= end)
    }
}
