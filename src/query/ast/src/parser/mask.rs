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

use crate::ast::Expr;
use crate::ast::Literal;
use crate::ast::Statement;
use crate::ast::quote::QuotedString;
use crate::parser::Dialect;
use crate::parser::ParseMode;
use crate::parser::run_parser;
use crate::parser::script::ScriptBlockOrStmt;
use crate::parser::script::script_block_or_stmt;
use crate::parser::statement::statement;
use crate::parser::token::TokenKind;
use crate::parser::token::Tokenizer;
use crate::parser::tokenize_sql;

/// Name fragments marking an option as a credential, e.g. `secret_access_key`,
/// `"s3.session-token"`, `AUTHORIZATION_HEADER`, `"gcs.credentials"`.
const SECRET_NAME_PARTS: &[&str] = &[
    "secret",
    "token",
    "passw",
    "pwd",
    "credential",
    "authorization",
    "account_name",
    "external_id",
    "delegation",
    // e.g. `SET GLOBAL enterprise_license = '...'`
    "license",
];

/// Whether an option name holds a credential. Matching ignores case and
/// treats `.` and `-` as `_`. A `key` component counts (`access_key_id`,
/// `"adls.account-key"`) unless it is the whole name, like `PRIMARY KEY`.
pub(crate) fn is_secret_name(name: &str) -> bool {
    // Match the Unicode normalization used by connection/dictionary binders.
    let name = name.to_lowercase().replace(['.', '-'], "_");
    SECRET_NAME_PARTS.iter().any(|part| name.contains(part))
        || (name.contains('_') && name.split('_').any(|part| part == "key"))
}

/// Original values hidden by masking, used to scrub diagnostics that quote
/// the original SQL or echo its values, such as parser source excerpts.
#[derive(Clone, Default)]
pub struct SqlSecrets(std::collections::BTreeSet<String>);

impl std::fmt::Debug for SqlSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SqlSecrets({} values)", self.0.len())
    }
}

impl SqlSecrets {
    /// Records a hidden value. Lines are also recorded, since source excerpts
    /// show multi-line values (e.g. private keys) one line at a time.
    pub fn push(&mut self, value: &str) {
        let value = value.trim();
        for v in std::iter::once(value).chain(value.lines().map(str::trim)) {
            if !v.is_empty() && !self.0.contains(v) {
                self.0.insert(v.to_string());
            }
        }
    }

    /// Records a quoted token both as written and unescaped.
    fn push_literal(&mut self, text: &str) {
        if text.len() >= 2 {
            self.push(&text[1..text.len() - 1]);
        }
        self.push(&unquote(text));
    }

    fn push_token(&mut self, kind: TokenKind, text: &str) {
        match kind {
            TokenKind::LiteralString => self.push_literal(text),
            TokenKind::LiteralCodeString if text.len() >= 4 => self.push(&text[2..text.len() - 2]),
            _ => self.push(text),
        }
    }

    pub fn extend(&mut self, other: SqlSecrets) {
        for value in other.0 {
            self.push(&value);
        }
    }

    /// Replaces recorded values in `text` with `***`. Values shorter than
    /// four characters are replaced only where quoted, so that they cannot
    /// garble the surrounding diagnostic.
    pub fn scrub(&self, text: &str) -> String {
        let mut values: Vec<&str> = self.0.iter().map(String::as_str).collect();
        values.sort_by_key(|v| std::cmp::Reverse(v.len()));
        let mut text = text.to_string();
        for value in values {
            if value.chars().count() >= 4 {
                text = text.replace(value, "***");
            } else {
                for q in ['\'', '"', '`'] {
                    text = text.replace(&format!("{q}{value}{q}"), &format!("{q}***{q}"));
                }
            }
        }
        text
    }
}

/// Progress through a `CREATE [OR REPLACE] USER` / `ALTER USER` statement.
#[derive(Clone, Copy, PartialEq)]
enum UserAuth {
    Outside,
    Statement,
    Identified,
    By,
}

/// Masks credentials in raw SQL text, for logs written before parsing.
///
/// The input is tokenized, so string literals and comments never trigger
/// masking, and a replacement never extends past the secret it covers. This
/// keeps an executed query's structure visible even if its literals mention
/// keywords such as `IDENTIFIED` or `PASSWORD=`. Masked:
/// - the password of `CREATE [OR REPLACE] USER` / `ALTER USER`: string
///   literals after `IDENTIFIED` and the token after `BY`;
/// - `<key> = '<value>'` pairs whose key (quoted or not) satisfies
///   [`is_secret_name`], and credentials of URL literals. In queries and DML
///   (`SELECT`, `INSERT`, `UPDATE`, ...) only inside `CONNECTION = (...)` /
///   `CONNECTION => (...)`: elsewhere they compare or write user data, e.g.
///   `ON f.date_key = d.date_key`;
/// - every value inside `HEADERS = ( ... )`.
///
/// Masked values keep their first and last two characters. SQL inside
/// `$$ ... $$` is masked recursively. If tokenization fails at a secret
/// position (e.g. an unterminated literal), everything from there to the end
/// is masked.
///
/// SQL that fails to parse may hide a secret outside these positions; use
/// [`mask_raw_sql_secrets`] for SQL that has not been parsed successfully.
pub fn mask_sql_secrets(sql: &str) -> String {
    mask_sql_inner(sql).0
}

/// Masks SQL nested in literals, e.g. `EXECUTE IMMEDIATE '...'`.
type NestedMasker = fn(&str) -> (String, SqlSecrets);

pub(crate) fn mask_sql_inner(sql: &str) -> (String, SqlSecrets) {
    mask_sql_with(sql, mask_raw_sql_with_secrets)
}

fn mask_sql_with(sql: &str, nested: NestedMasker) -> (String, SqlSecrets) {
    let mut masked = Masked::new(sql);
    let mut secrets = SqlSecrets::default();
    let mut auth = UserAuth::Outside;
    // Parenthesis depth inside `HEADERS = (...)`; 0 when outside.
    let mut headers_depth = 0usize;
    let mut webhook_depth = 0usize;
    let mut dynamic_sql = false;
    let mut dynamic_expression = false;
    let mut procedure = false;
    // The kind of the current statement, see `statement_head`.
    let mut head: Option<TokenKind> = None;
    let mut depth = 0usize;
    // Parenthesis depth inside `CONNECTION = (...)`; 0 when outside.
    let mut connection_depth = 0usize;
    // The last three tokens as (kind, span), most recent last.
    let mut prev: [Option<(TokenKind, Range<usize>)>; 3] = [None, None, None];

    let mut tokens = Tokenizer::new(sql).peekable();
    while let Some(token) = tokens.next() {
        // Option values may hold credentials; query expressions hold data.
        let options = connection_depth > 0 || !head.is_some_and(is_query_head);
        let after_eq = matches!(&prev[2], Some((TokenKind::Eq, _)));
        let pending_key = match &prev[1] {
            Some((kind, key)) if options && after_eq && is_secret_key(sql, *kind, key) => {
                Some(key.clone())
            }
            _ => None,
        };
        let pending_header = after_eq && headers_depth > 0;
        let url_key = after_eq
            && prev[1].as_ref().is_some_and(|(_, key)| {
                is_url_option(sql[key.clone()].trim_matches(['\'', '"', '`']))
            });
        let token = match token {
            Ok(token) => token,
            Err(err) => {
                let start = err.0.map_or(sql.len(), |span| span.start());
                if let Some(key) = pending_key {
                    secrets.push(sql[start..].trim_start_matches(['\'', '"']));
                    let replacement = format!("{} = '***'", &sql[key.clone()]);
                    masked.replace(key.start..sql.len(), &replacement);
                    break;
                }
                if matches!(auth, UserAuth::Identified | UserAuth::By) || pending_header {
                    secrets.push(sql[start..].trim_start_matches(['\'', '"']));
                    masked.replace(start..sql.len(), "'***'");
                    break;
                }
                continue;
            }
        };
        let kind = token.kind;
        if kind == TokenKind::EOI {
            break;
        }
        let span: Range<usize> = token.span.into();
        let text = &sql[span.clone()];

        if kind == TokenKind::SemiColon {
            head = None;
            depth = 0;
        } else if head.is_none() {
            head = statement_head(kind, depth);
        }
        if kind == TokenKind::LParen {
            depth += 1;
            if connection_depth > 0 {
                connection_depth += 1;
            } else if matches!(&prev[2], Some((TokenKind::Eq | TokenKind::FatRArrow, _)))
                && matches!(&prev[1], Some((TokenKind::CONNECTION, _)))
            {
                connection_depth = 1;
            }
            if headers_depth > 0 {
                headers_depth += 1;
            } else if after_eq && matches!(&prev[1], Some((TokenKind::HEADERS, _))) {
                headers_depth = 1;
            }
            if webhook_depth > 0 {
                webhook_depth += 1;
            } else if after_eq && matches!(&prev[1], Some((TokenKind::WEBHOOK, _))) {
                webhook_depth = 1;
            }
        } else if kind == TokenKind::RParen {
            depth = depth.saturating_sub(1);
            connection_depth = connection_depth.saturating_sub(1);
            headers_depth = headers_depth.saturating_sub(1);
            webhook_depth = webhook_depth.saturating_sub(1);
        }

        if kind == TokenKind::SemiColon {
            dynamic_sql = false;
            dynamic_expression = false;
            procedure = false;
        } else if kind == TokenKind::IMMEDIATE && matches!(&prev[2], Some((TokenKind::EXECUTE, _)))
        {
            dynamic_sql = true;
        } else if dynamic_sql
            && !matches!(
                kind,
                TokenKind::LiteralString | TokenKind::LiteralCodeString
            )
        {
            // The raw logger cannot evaluate an expression. Hide its literal
            // arguments; binding records the evaluated, precisely masked script.
            dynamic_expression = true;
        }
        if kind == TokenKind::PROCEDURE {
            procedure = true;
        }

        match (auth, kind) {
            (_, TokenKind::SemiColon) => auth = UserAuth::Outside,
            (UserAuth::By, _) => {
                secrets.push_token(kind, text);
                masked.replace(span.clone(), "'***'");
                auth = UserAuth::Outside;
            }
            (UserAuth::Identified, TokenKind::BY) => auth = UserAuth::By,
            (UserAuth::Identified, TokenKind::LiteralString | TokenKind::LiteralCodeString) => {
                secrets.push_token(kind, text);
                masked.replace(span.clone(), "'***'");
            }
            (UserAuth::Statement, TokenKind::IDENTIFIED) => auth = UserAuth::Identified,
            (UserAuth::Outside, TokenKind::USER) if starts_user_statement(&prev) => {
                auth = UserAuth::Statement;
            }
            (_, TokenKind::LiteralString) => {
                let value = unquote(text);
                let replacement = if dynamic_expression {
                    secrets.push_literal(text);
                    Some("***".to_string())
                } else if dynamic_sql {
                    let (script, hidden) = nested(&value);
                    secrets.extend(hidden);
                    Some(script)
                } else if pending_key.is_some() || pending_header {
                    secrets.push_literal(text);
                    Some(mask_partial(&value))
                } else if options {
                    let url = mask_url(&value, url_key && webhook_depth > 0);
                    if url != value {
                        secrets.push_literal(text);
                        secrets.extend(url_secrets(&value));
                    }
                    (url != value).then_some(url)
                } else {
                    None
                };
                if let Some(value) = replacement {
                    let quoted = QuotedString(&value, text.as_bytes()[0] as char).to_string();
                    if let Some(key) = pending_key {
                        masked.replace(
                            key.start..span.end,
                            &format!("{} = {quoted}", &sql[key.clone()]),
                        );
                    } else if value != unquote(text) {
                        masked.replace(span.clone(), &quoted);
                    }
                }
            }
            (_, TokenKind::LiteralCodeString) => {
                let code = &text[2..text.len() - 2];
                let (masked_code, hidden) = if dynamic_sql || procedure {
                    nested(code)
                } else {
                    // Python/JS UDF bodies are not SQL; do not truncate them
                    // merely because the SQL parser rejects their language.
                    mask_sql_with(code, nested)
                };
                secrets.extend(hidden);
                if masked_code != code {
                    masked.replace(span.clone(), &format!("$${masked_code}$$"));
                }
            }
            _ if dynamic_expression
                && matches!(
                    kind,
                    TokenKind::LiteralInteger
                        | TokenKind::LiteralFloat
                        | TokenKind::PGLiteralHex
                        | TokenKind::MySQLLiteralHex
                        | TokenKind::TRUE
                        | TokenKind::FALSE
                ) =>
            {
                if !matches!(kind, TokenKind::TRUE | TokenKind::FALSE) {
                    secrets.push(text);
                }
                masked.replace(span.clone(), "'***'");
            }
            _ if pending_key.is_some() || pending_header => {
                // Dictionary options also accept bare identifiers, numbers and
                // booleans. Do not consume delimiters, function names or
                // qualified column names.
                let function = tokens.peek().is_some_and(|next| {
                    next.as_ref()
                        .is_ok_and(|next| matches!(next.kind, TokenKind::LParen | TokenKind::Dot))
                });
                if !function
                    && !matches!(
                        kind,
                        TokenKind::LParen
                            | TokenKind::RParen
                            | TokenKind::Comma
                            | TokenKind::SemiColon
                            | TokenKind::Eq
                    )
                {
                    secrets.push(text);
                    if let Some(key) = pending_key {
                        masked.replace(
                            key.start..span.end,
                            &format!("{} = '***'", &sql[key.clone()]),
                        );
                    } else {
                        masked.replace(span.clone(), "'***'");
                    }
                }
            }
            _ => {}
        }

        prev = [prev[1].take(), prev[2].take(), Some((kind, span))];
    }
    (masked.finish(), secrets)
}

/// The kind of a statement from its first token, or `None` for a prefix such
/// as `EXPLAIN`, `SETTINGS (...)` or a script's `BEGIN`.
fn statement_head(kind: TokenKind, depth: usize) -> Option<TokenKind> {
    match kind {
        TokenKind::EXPLAIN
        | TokenKind::ANALYZE
        | TokenKind::SETTINGS
        | TokenKind::BEGIN
        | TokenKind::LParen
        | TokenKind::RParen => None,
        // e.g. `(SELECT ...) UNION ...`
        TokenKind::SELECT | TokenKind::WITH => Some(kind),
        // Inside a prefix group, e.g. `EXPLAIN (VERBOSE)`.
        _ if depth > 0 => None,
        _ => Some(kind),
    }
}

fn is_query_head(kind: TokenKind) -> bool {
    matches!(
        kind,
        TokenKind::SELECT
            | TokenKind::WITH
            | TokenKind::FROM
            | TokenKind::VALUES
            | TokenKind::INSERT
            | TokenKind::REPLACE
            | TokenKind::UPDATE
            | TokenKind::DELETE
            | TokenKind::MERGE
    )
}

/// Masks SQL for request logs written before planning, without parsing it.
///
/// Statements are masked as [`mask_sql_secrets`] does, and script blocks
/// (`BEGIN` / `DECLARE`) hide all literal values, which keeps their structure.
///
/// TODO: unlike [`mask_raw_sql_secrets`], this does not parse, so these logs
/// may still contain credentials of:
/// - SQL that fails to parse, e.g. a misspelled `IDENTIFIED`, a missing quote
///   or an unquoted URL;
/// - options with an unrecognized name, e.g. a misspelled `PASSWORD`;
/// - `CALL PROCEDURE` arguments and scripts in `CREATE TASK ... AS BEGIN`.
///
/// Query history and error messages are not affected: they are masked from
/// the AST, or by [`mask_raw_sql_secrets`] when parsing fails.
pub fn mask_log_sql_secrets(sql: &str) -> String {
    mask_log_inner(sql).0
}

fn mask_log_inner(sql: &str) -> (String, SqlSecrets) {
    if starts_script_block(sql) {
        mask_program_literals_with(sql, mask_log_inner)
    } else {
        mask_sql_with(sql, mask_log_inner)
    }
}

fn starts_script_block(sql: &str) -> bool {
    Tokenizer::new(sql).next().is_some_and(|token| {
        token.is_ok_and(|token| matches!(token.kind, TokenKind::BEGIN | TokenKind::DECLARE))
    })
}

/// [`mask_sql_secrets`] for SQL that may not parse, such as raw requests.
///
/// A statement that parses in any dialect is masked from its AST, see
/// `Statement::to_mask_sql_with_secrets`, so masking cannot hide the
/// structure of a valid query. Tokens of SQL that fails to
/// parse can be misaligned with its intent: a misspelled `IDENTIFIED`, or a
/// missing quote turning `KEY='s3 SECRET='x'` into a literal followed by the
/// bare secret. Names cannot identify credentials there, so all values are
/// hidden while keywords, identifiers and punctuation are kept for diagnosis.
pub fn mask_raw_sql_secrets(sql: &str) -> String {
    mask_raw_sql_with_secrets(sql).0
}

/// [`mask_raw_sql_secrets`], also returning the hidden values.
pub fn mask_raw_sql_with_secrets(sql: &str) -> (String, SqlSecrets) {
    let block = starts_script_block(sql);
    // Bound the cost of logging large statements, typically INSERT ... VALUES,
    // which are parsed only if they mention a credential-like word.
    if !block && sql.len() > RAW_PARSE_LIMIT && first_secret_mention(sql).is_none() {
        return mask_sql_inner(sql);
    }
    match parse_for_masking(sql) {
        Some(ParsedSql::Statement(stmt, format)) => {
            let (mut masked, secrets) = stmt.to_mask_sql_with_secrets();
            if let Some(format) = format {
                masked = format!("{masked} FORMAT {format}");
            }
            (masked, secrets)
        }
        Some(ParsedSql::Script) => {
            // A declaration may feed a credential into a later dynamic query.
            // Without data-flow evaluation, literals in a program are opaque;
            // preserve control-flow/SQL structure, but not their values. Child
            // queries record their evaluated SQL separately.
            mask_program_literals(sql)
        }
        None => mask_unparsed(sql),
    }
}

const RAW_PARSE_LIMIT: usize = 64 * 1024;

fn first_secret_mention(sql: &str) -> Option<usize> {
    let is_word = |c: char| c.is_alphanumeric() || matches!(c, '_' | '.' | '-');
    let mut start = None;
    for (i, c) in sql.char_indices().chain([(sql.len(), ' ')]) {
        match (start, is_word(c)) {
            (None, true) => start = Some(i),
            (Some(s), false) => {
                let word = &sql[s..i];
                if is_secret_name(word)
                    || word.eq_ignore_ascii_case("identified")
                    || word.eq_ignore_ascii_case("headers")
                    || word.eq_ignore_ascii_case("execute")
                {
                    return Some(s);
                }
                start = None;
            }
            _ => {}
        }
    }
    None
}

/// Hides every literal of a script; masks nested `$$` SQL recursively.
pub(crate) fn mask_program_literals(sql: &str) -> (String, SqlSecrets) {
    mask_program_literals_with(sql, mask_raw_sql_with_secrets)
}

fn mask_program_literals_with(sql: &str, nested: NestedMasker) -> (String, SqlSecrets) {
    let mut program = Masked::new(sql);
    let mut secrets = SqlSecrets::default();
    for token in Tokenizer::new(sql).flatten() {
        let text = &sql[std::ops::Range::from(token.span)];
        match token.kind {
            TokenKind::LiteralCodeString => {
                let (code, hidden) = nested(&text[2..text.len() - 2]);
                secrets.extend(hidden);
                program.replace(token.span.into(), &format!("$${code}$$"));
            }
            kind if kind.is_literal() => {
                secrets.push_token(kind, text);
                program.replace(token.span.into(), "'***'");
            }
            _ => {}
        }
    }
    let (masked, hidden) = mask_sql_with(&program.finish(), nested);
    secrets.extend(hidden);
    (masked, secrets)
}

/// Hides the values of SQL that fails to parse: quoted strings, comments,
/// tokens after `=` or `BY`, and everything from a tokenization error.
fn mask_unparsed(sql: &str) -> (String, SqlSecrets) {
    const HIDDEN: &str = "'***'";
    let mut secrets = SqlSecrets::default();
    // Replacements in order; glued hidden tokens merge into one.
    let mut hidden: Vec<(Range<usize>, String)> = vec![];
    let mut hide = |range: Range<usize>, text: String| match hidden.last_mut() {
        Some((last, last_text))
            if last.end == range.start && *last_text == HIDDEN && text == HIDDEN =>
        {
            last.end = range.end;
        }
        _ => hidden.push((range, text)),
    };
    // (kind, end) of the previous token.
    let mut prev: Option<(TokenKind, usize)> = None;
    let mut in_value = false;
    for token in Tokenizer::new(sql) {
        let prev_end = prev.map_or(0, |(_, end)| end);
        let token = match token {
            Ok(token) => token,
            Err(_) => {
                secrets.push(&sql[prev_end..]);
                let start = prev_end + (sql[prev_end..].len() - sql[prev_end..].trim_start().len());
                hide(start..sql.len(), HIDDEN.to_string());
                break;
            }
        };
        let span: Range<usize> = token.span.into();
        // Comments are not tokens, but may still hold a misplaced value.
        let gap = &sql[prev_end..span.start];
        if !gap.trim().is_empty() {
            secrets.push(gap);
            hide(prev_end..span.start, " /* *** */ ".to_string());
        }
        let kind = token.kind;
        if kind == TokenKind::EOI {
            break;
        }
        let text = &sql[span.clone()];
        let delimiter = matches!(
            kind,
            TokenKind::LParen | TokenKind::RParen | TokenKind::Comma | TokenKind::SemiColon
        );
        // A bare value, e.g. `ADDRESS=https://u:pw@host`, spans glued tokens.
        in_value = !delimiter
            && match prev.map(|(kind, _)| kind) {
                Some(TokenKind::Eq | TokenKind::BY) => !kind.is_keyword(),
                _ => in_value && prev_end == span.start,
            };
        if kind == TokenKind::LiteralCodeString {
            let (code, nested) = mask_raw_sql_with_secrets(&text[2..text.len() - 2]);
            secrets.extend(nested);
            hide(span.clone(), format!("$${code}$$"));
        } else if in_value
            || (kind == TokenKind::LiteralString && text.starts_with('\''))
            || matches!(kind, TokenKind::PGLiteralHex | TokenKind::MySQLLiteralHex)
        {
            secrets.push_token(kind, text);
            hide(span.clone(), HIDDEN.to_string());
        }
        prev = Some((kind, span.end));
    }
    let mut masked = Masked::new(sql);
    for (range, text) in hidden {
        if text == HIDDEN {
            // Also as a whole, as a source excerpt shows glued tokens.
            secrets.push(&sql[range.clone()]);
        }
        masked.replace(range, &text);
    }
    (masked.finish(), secrets)
}

enum ParsedSql {
    Statement(Box<Statement>, Option<String>),
    Script,
}

fn parse_for_masking(sql: &str) -> Option<ParsedSql> {
    let tokens = tokenize_sql(sql).ok()?;
    [
        Dialect::PostgreSQL,
        Dialect::MySQL,
        Dialect::Hive,
        Dialect::PRQL,
        Dialect::Experimental,
    ]
    .into_iter()
    .find_map(|dialect| {
        // Do not use parse_sql's debug round-trip diagnostic: its warning may
        // include the unmasked source, and this is the log sanitization path.
        if let Ok(stmt) = run_parser(&tokens, dialect, ParseMode::Default, false, statement) {
            return Some(ParsedSql::Statement(Box::new(stmt.stmt), stmt.format));
        }
        match run_parser(
            &tokens,
            dialect,
            ParseMode::Template,
            false,
            script_block_or_stmt,
        ) {
            Ok(ScriptBlockOrStmt::ScriptBlock(_)) => Some(ParsedSql::Script),
            Ok(ScriptBlockOrStmt::Statement(stmt)) => {
                Some(ParsedSql::Statement(Box::new(stmt), None))
            }
            Err(_) => None,
        }
    })
}

fn unquote(text: &str) -> String {
    text.parse::<QuotedString<String>>()
        .map(|quoted| quoted.0)
        .unwrap_or_else(|_| text[1..text.len() - 1].to_string())
}

fn is_url_option(name: &str) -> bool {
    let name = name.to_lowercase();
    matches!(name.as_str(), "url" | "uri" | "address")
        || name.ends_with("_url")
        || name.ends_with("_uri")
}

/// The credentials of a URL hidden by [`mask_url`].
pub(crate) fn url_secrets(value: &str) -> SqlSecrets {
    let mut secrets = SqlSecrets::default();
    if let Ok(url) = url::Url::parse(value) {
        secrets.push(url.username());
        if let Some(password) = url.password() {
            secrets.push(password);
        }
        for (_, v) in url.query_pairs() {
            secrets.push(&v);
        }
        if let Some(fragment) = url.fragment() {
            secrets.push(fragment);
        }
    }
    secrets
}

/// Webhook paths can be bearer credentials themselves. Other URLs retain
/// their target, but never userinfo, query values or fragments.
pub fn mask_url(value: &str, webhook: bool) -> String {
    let Ok(mut url) = url::Url::parse(value) else {
        return if webhook || (value.contains("://") && value.contains(['@', '?', '#'])) {
            "***".to_string()
        } else {
            value.to_string()
        };
    };
    if webhook {
        return format!("{}/***", url.origin().ascii_serialization());
    }
    let mut changed = false;
    if !url.username().is_empty() {
        let _ = url.set_username("***");
        changed = true;
    }
    if url.password().is_some() {
        let _ = url.set_password(Some("***"));
        changed = true;
    }
    if url.query().is_some() {
        let query = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(url.query_pairs().map(|(key, _)| (key, "***")))
            .finish();
        url.set_query(Some(&query));
        changed = true;
    }
    if url.fragment().is_some() {
        url.set_fragment(Some("***"));
        changed = true;
    }
    if changed {
        url.into()
    } else {
        value.to_string()
    }
}

#[derive(Default, derive_visitor::VisitorMut)]
#[visitor(Literal(enter))]
struct HideLiterals(SqlSecrets);

impl HideLiterals {
    fn enter_literal(&mut self, literal: &mut Literal) {
        match &*literal {
            Literal::String(value) => self.0.push(value),
            Literal::Boolean(_) | Literal::Null => {}
            other => self.0.push(&other.to_string()),
        }
        *literal = Literal::String("***".to_string());
    }
}

/// Hides all literal values in `node`, e.g. procedure arguments.
pub(crate) fn hide_literals<T: derive_visitor::DriveMut>(node: &mut T) -> SqlSecrets {
    let mut visitor = HideLiterals::default();
    node.drive_mut(&mut visitor);
    visitor.0
}

pub(crate) fn mask_dynamic_sql_expr(expr: &mut Expr) -> SqlSecrets {
    if let Expr::Literal {
        value: Literal::String(script),
        ..
    } = expr
    {
        let (masked, secrets) = mask_raw_sql_with_secrets(script);
        *script = masked;
        secrets
    } else {
        // Only the binder can evaluate arbitrary expressions. The history is
        // populated from its evaluated script, not these redacted arguments.
        hide_literals(expr)
    }
}

fn starts_user_statement(prev: &[Option<(TokenKind, Range<usize>)>; 3]) -> bool {
    let kind = |i: usize| prev[i].as_ref().map(|(kind, _)| *kind);
    matches!(kind(2), Some(TokenKind::CREATE | TokenKind::ALTER))
        || (kind(0), kind(1), kind(2))
            == (
                Some(TokenKind::CREATE),
                Some(TokenKind::OR),
                Some(TokenKind::REPLACE),
            )
}

fn is_secret_key(sql: &str, kind: TokenKind, span: &Range<usize>) -> bool {
    let text = &sql[span.clone()];
    let key = match kind {
        // Quoted names, e.g. "s3.secret-access-key", `password` or 'token'.
        TokenKind::LiteralString => return is_secret_name(&unquote(text)),
        TokenKind::LiteralCodeString => return false,
        _ => text,
    };
    is_secret_name(key)
}

/// Shows the first and last two characters; values of four or fewer
/// characters are fully masked.
pub(crate) fn mask_partial(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= 4 {
        "***".to_string()
    } else {
        let head: String = chars[..2].iter().collect();
        let tail: String = chars[chars.len() - 2..].iter().collect();
        format!("{head}***{tail}")
    }
}

/// Copies `sql` while applying non-overlapping, in-order replacements.
struct Masked<'a> {
    sql: &'a str,
    out: String,
    copied: usize,
}

impl<'a> Masked<'a> {
    fn new(sql: &'a str) -> Self {
        Self {
            sql,
            out: String::new(),
            copied: 0,
        }
    }

    fn replace(&mut self, range: Range<usize>, text: &str) {
        if range.start < self.copied {
            return;
        }
        self.out.push_str(&self.sql[self.copied..range.start]);
        self.out.push_str(text);
        self.copied = range.end;
    }

    fn finish(mut self) -> String {
        if self.copied == 0 {
            return self.sql.to_string();
        }
        self.out.push_str(&self.sql[self.copied..]);
        self.out
    }
}

#[cfg(test)]
mod tests {
    use super::mask_log_sql_secrets;
    use super::mask_raw_sql_secrets;
    use super::mask_raw_sql_with_secrets;
    use super::mask_sql_secrets;
    use crate::parser::Dialect;
    use crate::parser::parse_sql;
    use crate::parser::tokenize_sql;

    #[test]
    fn test_mask_sql_secrets() {
        for (sql, expected) in [
            // User passwords.
            (
                "CREATE USER IF NOT EXISTS u IDENTIFIED WITH sha256_password BY 'pw' WITH DEFAULT_ROLE = 'r'",
                "CREATE USER IF NOT EXISTS u IDENTIFIED WITH sha256_password BY '***' WITH DEFAULT_ROLE = 'r'",
            ),
            (
                "create /* c */ or replace user u identified by 'pw'",
                "create /* c */ or replace user u identified by '***'",
            ),
            (
                "EXPLAIN ALTER USER USER() IDENTIFIED -- c\nBY \"pw\"",
                "EXPLAIN ALTER USER USER() IDENTIFIED -- c\nBY '***'",
            ),
            (
                "CREATE USER u IDENTIFIED BY 'pw'; SELECT 'pw'",
                "CREATE USER u IDENTIFIED BY '***'; SELECT 'pw'",
            ),
            (
                "CREATE USER u IDENTIFIED BY 'pw\nrest",
                "CREATE USER u IDENTIFIED BY '***'",
            ),
            // Nested SQL that parses is masked from its AST, including
            // procedure arguments, like a top-level `CALL PROCEDURE`.
            (
                "EXECUTE IMMEDIATE $$CALL PROCEDURE p('leak_arg', 42)$$",
                "EXECUTE IMMEDIATE $$CALL PROCEDURE p('***', '***')$$",
            ),
            (
                "EXECUTE IMMEDIATE $$CREATE USER u IDENTIFIED BY 'pw'$$",
                "EXECUTE IMMEDIATE $$CREATE USER 'u'@'%' IDENTIFIED BY '***'$$",
            ),
            (
                "EXECUTE IMMEDIATE 'CREATE USER u IDENTIFIED BY ''leak_pw'''",
                "EXECUTE IMMEDIATE 'CREATE USER \\'u\\'@\\'%\\' IDENTIFIED BY \\'***\\''",
            ),
            (
                "EXECUTE IMMEDIATE concat('CREATE USER u IDENTIFIED BY ', '''leak_pw''')",
                "EXECUTE IMMEDIATE concat('***', '***')",
            ),
            // Secret options, matched by name.
            (
                "CREATE STAGE s URL='s3://b' CONNECTION=(ACCESS_KEY_ID='akid123' SECRET_ACCESS_KEY = 'secret456')",
                "CREATE STAGE s URL='s3://b' CONNECTION=(ACCESS_KEY_ID = 'ak***23' SECRET_ACCESS_KEY = 'se***56')",
            ),
            (
                "CREATE CONNECTION c STORAGE_TYPE='azblob' SAS_TOKEN='sas_value' CLIENT_SECRET='cs_value' CLIENT_ID='client'",
                "CREATE CONNECTION c STORAGE_TYPE='azblob' SAS_TOKEN = 'sa***ue' CLIENT_SECRET = 'cs***ue' CLIENT_ID='client'",
            ),
            (
                r#"CONNECTION=("s3.secret-access-key"='ice_secret' 'adls.account-key'='adls_key' address='http://x')"#,
                r#"CONNECTION=("s3.secret-access-key" = 'ic***et' 'adls.account-key' = 'ad***ey' address='http://x')"#,
            ),
            (
                r#"PASSWORD = 'it''s_secret' SECRET_KEY = 'a\'b' password = "pass\"word" SECRET_ID = ''"#,
                r#"PASSWORD = 'it***et' SECRET_KEY = '***' password = "pa***rd" SECRET_ID = '***'"#,
            ),
            (
                "CONNECTION = (`password` /* c */ = -- c\n 'my_password')",
                "CONNECTION = (`password` = 'my***rd')",
            ),
            ("PASSWORD = 'my_password", "PASSWORD = '***'"),
            ("PASSWORD=79256133", "PASSWORD = '***'"),
            ("PASSWORD=bare_value", "PASSWORD = '***'"),
            ("ACCESS_KEY_ID='key_value'", "ACCESS_KEY_ID = 'ke***ue'"),
            // Webhook and UDF headers.
            (
                "WEBHOOK=(url='http://x' authorization_header='Bearer webhook')",
                "WEBHOOK=(url='http://x/***' authorization_header = 'Be***ok')",
            ),
            (
                "HANDLER='h' HEADERS=('Authorization'='Bearer udf_token', 'X-Org'='org_value') ADDRESS='http://x'",
                "HANDLER='h' HEADERS=('Authorization' = 'Be***en', 'X-Org'='or***ue') ADDRESS='http://x'",
            ),
            (
                "WEBHOOK=(url='https://u:leak_pwd@example.test/hook/leak_path?sig=leak_sig')",
                "WEBHOOK=(url='https://example.test/***')",
            ),
            (
                "ADDRESS='https://u:leak_pwd@example.test/f?sig=leak_sig#leak_fragment'",
                "ADDRESS='https://***:***@example.test/f?sig=***#***'",
            ),
            // Keywords inside literals or comments must not hide the query.
            (
                "SELECT 'CREATE USER x IDENTIFIED' AS a, * FROM secret_table",
                "SELECT 'CREATE USER x IDENTIFIED' AS a, * FROM secret_table",
            ),
            (
                r#"SELECT 'password="' AS a, * FROM secret_table"#,
                r#"SELECT 'password="' AS a, * FROM secret_table"#,
            ),
            (
                "SELECT /* CREATE USER x IDENTIFIED */ * FROM secret_table",
                "SELECT /* CREATE USER x IDENTIFIED */ * FROM secret_table",
            ),
            // Query and DML expressions are user data, even if a column is
            // named like a credential; only their connections are masked.
            (
                "CREATE TABLE t (user INT, identified INT, a INT PRIMARY KEY); SELECT token FROM t WHERE token = 'abc'",
                "CREATE TABLE t (user INT, identified INT, a INT PRIMARY KEY); SELECT token FROM t WHERE token = 'abc'",
            ),
            (
                "SELECT * FROM f JOIN d ON f.date_key = d.date_key WHERE sort_key = 'abcdef' AND token_count = 5 AND has_secret = TRUE",
                "SELECT * FROM f JOIN d ON f.date_key = d.date_key WHERE sort_key = 'abcdef' AND token_count = 5 AND has_secret = TRUE",
            ),
            (
                "EXPLAIN (SELECT a FROM t WHERE customer_key = customer_id) UNION ALL SELECT 1",
                "EXPLAIN (SELECT a FROM t WHERE customer_key = customer_id) UNION ALL SELECT 1",
            ),
            (
                "INSERT INTO t VALUES ('https://u:leak@x.test/f?sig=1'); UPDATE t SET password = 'p'",
                "INSERT INTO t VALUES ('https://u:leak@x.test/f?sig=1'); UPDATE t SET password = 'p'",
            ),
            (
                "SELECT * FROM 's3://b/x.csv' (CONNECTION => (ACCESS_KEY_ID = 'akid123', SECRET_ACCESS_KEY = 'secret456')) WHERE token = 'abc'",
                "SELECT * FROM 's3://b/x.csv' (CONNECTION => (ACCESS_KEY_ID = 'ak***23', SECRET_ACCESS_KEY = 'se***56')) WHERE token = 'abc'",
            ),
            (
                "SETTINGS (enterprise_license = 'leak_lic') SELECT 1; SELECT 2",
                "SETTINGS (enterprise_license = 'le***ic') SELECT 1; SELECT 2",
            ),
            // Option values outside queries, e.g. a dictionary source.
            (
                "CREATE DICTIONARY d (a INT) PRIMARY KEY a SOURCE(MYSQL(password = leak_bare))",
                "CREATE DICTIONARY d (a INT) PRIMARY KEY a SOURCE(MYSQL(password = '***'))",
            ),
        ] {
            let masked = mask_sql_secrets(sql);
            assert_eq!(masked, expected, "{sql}");
            assert_eq!(mask_sql_secrets(&masked), masked, "{sql}");
        }
    }

    #[test]
    fn test_mask_raw_sql_secrets() {
        for (sql, expected) in [
            // SQL that fails to parse keeps its structure, but no values: a
            // missing quote moves the secret outside any literal, and a
            // misspelled keyword or option name hides its role.
            (
                "CREATE CONNECTION c STORAGE_TYPE='s3 SECRET_ACCESS_KEY='leak'",
                "CREATE CONNECTION c STORAGE_TYPE='***'",
            ),
            (
                "CREATE USER u IDENTIFIED BY # 'leak'",
                "CREATE USER u IDENTIFIED BY '***' '***'",
            ),
            (
                "CREATE USER u IDNETIFIED BY 'leak' WITH DEFAULT_ROLE='r'",
                "CREATE USER u IDNETIFIED BY '***' WITH DEFAULT_ROLE='***'",
            ),
            (
                "CREATE FUNCTION f(INT) RETURNS INT LANGUAGE python HANDLER='f' ADDRESS=https://u:leak@x.test",
                "CREATE FUNCTION f(INT) RETURNS INT LANGUAGE python HANDLER='***' ADDRESS='***'",
            ),
            (
                "CREATE CONNECTION c SECERT=leak_bare STORAGE_TYPE='s3",
                "CREATE CONNECTION c SECERT='***' STORAGE_TYPE='***'",
            ),
            // Invalid SQL without credential names is kept for diagnostics.
            (
                "CREATE TABLE t (a INT PRIMARY KEY",
                "CREATE TABLE t (a INT PRIMARY KEY",
            ),
        ] {
            assert_eq!(mask_raw_sql_secrets(sql), expected, "{sql}");
        }
        let (masked, secrets) = mask_raw_sql_with_secrets("CALL PROCEDURE p('leak_arg')");
        assert_eq!(masked, "CALL PROCEDURE p('***')");
        assert_eq!(secrets.scrub("leak_arg"), "***");

        // Exceeding a parsing budget must not turn valid SQL into a truncated
        // audit record, even with a credential-looking literal at the front.
        let large = format!(
            "SELECT 'password=' AS a, * FROM audit_table /*{}*/",
            "x".repeat(1024 * 1024)
        );
        assert_eq!(
            mask_raw_sql_secrets(&large),
            "SELECT 'password=' AS a, * FROM audit_table"
        );
        let nested = "EXECUTE IMMEDIATE $$CREATE USER u\nIDENTIFIED\nBY 'leak_pw' invalid$$";
        assert!(!mask_raw_sql_secrets(nested).contains("leak_pw"));
        let block = "DECLARE p STRING DEFAULT 'leak_pw'; BEGIN EXECUTE IMMEDIATE concat('SELECT ', p); END;";
        for masked_block in [mask_raw_sql_secrets(block), mask_log_sql_secrets(block)] {
            assert!(!masked_block.contains("leak_pw"));
            assert!(masked_block.contains("EXECUTE IMMEDIATE"), "{masked_block}");
        }
        // Parser diagnostics quote the original SQL; the recorded values let
        // callers keep the diagnostic while scrubbing the credential.
        let sql = "CREATE USER u IDNETIFIED BY 'leak''pw'";
        let (_, secrets) = mask_raw_sql_with_secrets(sql);
        let tokens = tokenize_sql(sql).unwrap();
        let err = parse_sql(&tokens, Dialect::PostgreSQL).unwrap_err().1;
        let scrubbed = secrets.scrub(&err);
        assert!(!scrubbed.contains("leak"), "{scrubbed}");
        assert!(scrubbed.contains("IDNETIFIED"), "{scrubbed}");
    }

    #[test]
    fn test_to_mask_sql_keeps_user_data() {
        // Columns named like credentials are data: the text is kept and no
        // value is recorded, so diagnostics are not scrubbed with it.
        for sql in [
            "SELECT * FROM f INNER JOIN d ON f.date_key = d.date_key WHERE customer_key = customer_id",
            "SELECT token FROM t WHERE token = 'abcdef' AND password_hash = 'xyz123'",
            "UPDATE t SET api_token = 'abcdef' WHERE sort_key = 1",
            "INSERT INTO t VALUES ('https://u:p@x.test/f?sig=1')",
            "SET max_threads = 8",
        ] {
            let tokens = tokenize_sql(sql).unwrap();
            let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
            let (masked, hidden) = stmt.to_mask_sql_with_secrets();
            assert_eq!(masked, stmt.to_string(), "{sql}");
            assert_eq!(hidden.scrub(sql), sql, "{sql}");
        }
    }

    #[test]
    fn test_to_mask_sql_secrets() {
        for (sql, secrets) in [
            (
                r#"CREATE CATALOG ctl TYPE=ICEBERG CONNECTION=(TYPE='rest' ADDRESS='http://x' WAREHOUSE='w' "s3.secret-access-key"='leak_ice' "s3.session-token"='leak_token')"#,
                &["leak_ice", "leak_token"][..],
            ),
            (
                "CREATE NOTIFICATION INTEGRATION n TYPE=WEBHOOK ENABLED=TRUE WEBHOOK=(url='http://x' method='POST' authorization_header='Bearer leak_wh')",
                &["leak_wh"],
            ),
            (
                "CREATE FUNCTION f(INT) RETURNS INT LANGUAGE python HANDLER='h' HEADERS=('Authorization'='Bearer leak_udf') ADDRESS='http://x'",
                &["leak_udf"],
            ),
            (
                "CREATE DICTIONARY d (a INT) PRIMARY KEY a SOURCE(MYSQL(host='h' port='3306' username='u' password='leak_dict' db='d' table='t'))",
                &["leak_dict"],
            ),
            (
                "EXPLAIN CREATE CONNECTION c STORAGE_TYPE='s3' SECRET_ACCESS_KEY='leak_key'",
                &["leak_key"],
            ),
            ("CALL PROCEDURE p('leak_arg', 42)", &["leak_arg"]),
            (
                "CREATE TASK t enterprise_license='leak_lic' AS BEGIN SET VARIABLE p = 'leak_var'; EXECUTE IMMEDIATE concat('CREATE USER u IDENTIFIED BY ', $p); END",
                &["leak_lic", "leak_var"],
            ),
            ("SET GLOBAL enterprise_license='leak_lic'", &["leak_lic"]),
            (
                "SELECT /*+ SET_VAR(enterprise_license='leak_lic') */ * FROM 's3://b/x.csv' (CONNECTION => (ACCESS_KEY_ID = 'leak_akid', SECRET_ACCESS_KEY = 'leak_sk'))",
                &["leak_lic", "leak_akid", "leak_sk"],
            ),
            ("ALTER TASK t SET enterprise_license='leak_lic'", &[
                "leak_lic",
            ]),
            (
                "CREATE TABLE t (a INT) 's3://b/' CONNECTION = (secret_access_key = 'leak_sk') session_token = 'leak_tok'",
                &["leak_sk", "leak_tok"],
            ),
        ] {
            let tokens = tokenize_sql(sql).unwrap();
            let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
            let (masked, hidden) = stmt.to_mask_sql_with_secrets();
            for secret in secrets {
                assert!(!masked.contains(secret), "{masked}");
                assert!(!hidden.scrub(sql).contains(secret), "{sql}");
            }
        }
    }
}
