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

mod ddl;
mod dml;
mod expr;
mod query;

use ddl::*;
use dml::*;
use pretty::RcDoc;
use query::*;

use crate::ast::Statement;
use crate::ParseError;
use crate::Result;

pub fn pretty_statement(stmt: Statement, max_width: usize) -> Result<String> {
    let pretty_stmt = match stmt {
        // Format and beautify large SQL statements to make them easy to read.
        Statement::Query(query) => pretty_query(*query),
        Statement::Insert(insert_stmt) => pretty_insert(insert_stmt),
        Statement::Delete(delete_stmt) => pretty_delete(delete_stmt),
        Statement::CopyIntoTable(copy_stmt) => pretty_copy_into_table(copy_stmt),
        Statement::CopyIntoLocation(copy_stmt) => pretty_copy_into_location(copy_stmt),
        Statement::Update(update_stmt) => pretty_update(update_stmt),
        Statement::CreateTable(create_table_stmt) => pretty_create_table(create_table_stmt),
        Statement::AlterTable(alter_table_stmt) => pretty_alter_table(alter_table_stmt),
        Statement::CreateView(create_view_stmt) => pretty_create_view(create_view_stmt),
        Statement::AlterView(alter_view_stmt) => pretty_alter_view(alter_view_stmt),
        Statement::CreateStream(create_stream_stmt) => pretty_create_stream(create_stream_stmt),
        // create dictionary,新添加的
        Statement::CreateDictionary(create_dictionary_stmt) => pretty_create_dictionary(create_dictionary_stmt),
        // Other SQL statements are relatively short and don't need extra format.
        _ => RcDoc::text(stmt.to_string()),
    };

    let mut bs = Vec::new();
    pretty_stmt
        .render(max_width, &mut bs)
        .map_err(|err| ParseError(None, err.to_string()))?;
    String::from_utf8(bs).map_err(|err| ParseError(None, err.to_string()))
}

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

pub(crate) fn parenthesized(doc: RcDoc<'_>) -> RcDoc<'_> {
    RcDoc::text("(")
        .append(RcDoc::line_())
        .append(doc)
        .nest(NEST_FACTOR)
        .append(RcDoc::line_())
        .append(RcDoc::text(")"))
        .group()
}
