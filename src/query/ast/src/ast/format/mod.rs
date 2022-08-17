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

mod syntax;

use pretty::RcDoc;
pub use syntax::pretty_statement;

pub(crate) const NEST_FACTOR: isize = 4;

pub(crate) fn interweave_comma<'a, D>(docs: D) -> RcDoc<'a>
where D: Iterator<Item = RcDoc<'a>> {
    RcDoc::intersperse(docs, RcDoc::text(",").append(RcDoc::line()))
}

pub(crate) fn inline_comma<'a, D>(docs: D) -> RcDoc<'a>
where D: Iterator<Item = RcDoc<'a>> {
    RcDoc::intersperse(docs, RcDoc::text(",").append(RcDoc::space()))
}

pub(crate) fn inline_dot<'a, D>(docs: D) -> RcDoc<'a>
where D: Iterator<Item = RcDoc<'a>> {
    RcDoc::intersperse(docs, RcDoc::text("."))
}

pub(crate) fn parenthenized(doc: RcDoc<'_>) -> RcDoc<'_> {
    RcDoc::text("(")
        .append(RcDoc::line_())
        .append(doc)
        .nest(NEST_FACTOR)
        .append(RcDoc::line_())
        .append(RcDoc::text(")"))
        .group()
}
