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

use std::sync::Arc;
use std::time::Instant;

use common_ast::parser::token::TokenKind;
use common_ast::parser::token::Tokenizer;
use common_config::DATABEND_COMMIT_VERSION;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::StringType;
use common_expression::types::ValueType;
use common_expression::SendableDataBlockStream;
use common_meta_app::principal::GrantObject;
use common_meta_app::principal::UserInfo;
use common_meta_app::principal::UserPrivilegeSet;
use common_sql::Planner;
use common_storages_fuse::TableContext;
use futures_util::StreamExt;
use rustyline::config::Builder;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::CompletionType;
use rustyline::Editor;

use super::helper::CliHelper;
use crate::interpreters::InterpreterFactory;
use crate::sessions::Session;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;

/// Session to execute local commands.
pub(crate) struct SessionExecutor {
    session: Arc<Session>,
    is_repl: bool,

    query: String,
    in_comment_block: bool,

    keywords: Arc<Vec<String>>,
}

static PROMPT_SQL: &str = "select name from system.tables union all select name from system.columns union all select name from system.databases union all select name from system.functions";

impl SessionExecutor {
    pub async fn try_new(is_repl: bool) -> Result<Self> {
        let mut keywords = Vec::with_capacity(1024);

        let session = SessionManager::instance()
            .create_session(SessionType::Local)
            .await
            .unwrap();

        let mut user = UserInfo::new_no_auth("root", "%");
        user.grants.grant_privileges(
            &GrantObject::Global,
            UserPrivilegeSet::available_privileges_on_global(),
        );

        session.set_authed_user(user, None).await.unwrap();

        if is_repl {
            println!("Welcome to Databend, version {}.", *DATABEND_COMMIT_VERSION);
            println!();

            let rows = Self::query(&session, PROMPT_SQL).await;
            if let Ok(mut rows) = rows {
                while let Some(row) = rows.next().await {
                    if let Ok(row) = row {
                        let num_rows = row.num_rows();
                        let col = row.columns()[0]
                            .value
                            .convert_to_full_column(&DataType::String, num_rows);
                        let value = StringType::try_downcast_column(&col).unwrap();
                        for r in value.iter() {
                            keywords.push(unsafe { String::from_utf8_unchecked(r.to_vec()) });
                        }
                    }
                }
            }
        }

        Ok(Self {
            session,
            is_repl,
            query: String::new(),
            in_comment_block: false,
            keywords: Arc::new(keywords),
        })
    }

    async fn query(session: &Arc<Session>, sql: &str) -> Result<SendableDataBlockStream> {
        let context = session.create_query_context().await?;
        let mut planner = Planner::new(context.clone());
        let (plan, extras) = planner.plan_sql(sql).await?;

        let interpreter = InterpreterFactory::get(context.clone(), &plan).await?;
        let has_result_set = plan.has_result_set();
        interpreter.execute(context).await
    }

    async fn update(session: &Arc<Session>, sql: &str) -> Result<usize> {
        let context = session.create_query_context().await?;
        let mut planner = Planner::new(context.clone());
        let (plan, extras) = planner.plan_sql(sql).await?;

        let interpreter = InterpreterFactory::get(context.clone(), &plan).await?;
        let mut stream = interpreter.execute(context.clone()).await?;
        while let Some(_) = stream.next().await {}

        Ok(context.get_write_progress_value().rows)
    }

    pub async fn handle_repl(&mut self) {
        let config = Builder::new()
            .completion_prompt_limit(5)
            .completion_type(CompletionType::Circular)
            .build();
        let mut rl = Editor::<CliHelper, DefaultHistory>::with_config(config).unwrap();

        rl.set_helper(Some(CliHelper::with_keywords(self.keywords.clone())));
        rl.load_history(&get_history_path()).ok();

        'F: loop {
            match rl.readline(&self.prompt().await) {
                Ok(line) => {
                    let queries = self.append_query(&line);
                    for query in queries {
                        let _ = rl.add_history_entry(&query);
                        match self.handle_query(true, &query).await {
                            Ok(true) => {
                                break 'F;
                            }
                            Ok(false) => {}
                            Err(e) => {
                                eprintln!("{}", e);
                                self.query.clear();
                                break;
                            }
                        }
                    }
                }
                Err(e) => match e {
                    ReadlineError::Io(err) => {
                        eprintln!("io err: {err}");
                    }
                    ReadlineError::Interrupted => {
                        self.query.clear();
                        println!("^C");
                    }
                    ReadlineError::Eof => {
                        break;
                    }
                    _ => {}
                },
            }
        }
        println!("Bye~");
        let _ = rl.save_history(&get_history_path());
    }

    pub fn append_query(&mut self, line: &str) -> Vec<String> {
        let line = line.trim();
        if line.is_empty() {
            return vec![];
        }

        if self.query.is_empty() && (line.starts_with('.') || line == "exit" || line == "quit") {
            return vec![line.to_owned()];
        }

        self.query.push(' ');

        let mut queries = Vec::new();
        let mut tokenizer = Tokenizer::new(line);
        let mut in_comment = false;
        let mut start = 0;
        let mut comment_block_start = 0;

        while let Some(Ok(token)) = tokenizer.next() {
            match token.kind {
                TokenKind::SemiColon => {
                    if in_comment || self.in_comment_block {
                        continue;
                    } else {
                        let mut sql = self.query.trim().to_owned();
                        if sql.is_empty() {
                            continue;
                        }
                        sql.push(';');

                        queries.push(sql);
                        self.query.clear();
                    }
                }
                TokenKind::Comment => {
                    in_comment = true;
                }
                TokenKind::EOI => {
                    in_comment = false;
                }
                _ => {
                    if !in_comment && !self.in_comment_block {
                        self.query.push_str(&line[start..token.span.end]);
                    }
                }
            }
            start = token.span.end;
        }

        if self.in_comment_block {
            self.query.push_str(&line[comment_block_start..]);
        }
        queries
    }

    pub async fn handle_query(&mut self, is_repl: bool, query: &str) -> Result<bool> {
        let query = query.trim_end_matches(';').trim();
        if is_repl && (query == "exit" || query == "quit") {
            return Ok(true);
        }

        let start = Instant::now();
        let kind = QueryKind::from(query);
        match (kind, is_repl) {
            (QueryKind::Update, false) => {
                let affected = Self::update(&self.session, query).await?;
                if is_repl {
                    if affected > 0 {
                        eprintln!(
                            "{} rows affected in ({:.3} sec)",
                            affected,
                            start.elapsed().as_secs_f64()
                        );
                    } else {
                        eprintln!("Processed in ({:.3} sec)", start.elapsed().as_secs_f64());
                    }
                    eprintln!();
                }
                Ok(false)
            }
            other => {
                let replace_newline = replace_newline_in_box_display(query);
                let mut stream = Self::query(&self.session, query).await?;

                println!("execute {:?}", query);
                Ok(false)
            }
        }
    }

    async fn prompt(&self) -> String {
        "databend-local:) ".to_owned()
    }
}

fn get_history_path() -> String {
    format!(
        "{}/.databend_history",
        std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
    )
}

#[derive(PartialEq, Eq, Debug)]
pub enum QueryKind {
    Query,
    Update,
    Explain,
}

impl From<&str> for QueryKind {
    fn from(query: &str) -> Self {
        let mut tz = Tokenizer::new(query);
        match tz.next() {
            Some(Ok(t)) => match t.kind {
                TokenKind::EXPLAIN => QueryKind::Explain,
                TokenKind::ALTER
                | TokenKind::DELETE
                | TokenKind::UPDATE
                | TokenKind::INSERT
                | TokenKind::CREATE
                | TokenKind::DROP
                | TokenKind::OPTIMIZE
                | TokenKind::COPY => QueryKind::Update,
                _ => QueryKind::Query,
            },
            _ => QueryKind::Query,
        }
    }
}

fn replace_newline_in_box_display(query: &str) -> bool {
    let mut tz = Tokenizer::new(query);
    match tz.next() {
        Some(Ok(t)) => match t.kind {
            TokenKind::EXPLAIN => false,
            TokenKind::SHOW => !matches!(tz.next(), Some(Ok(t)) if t.kind == TokenKind::CREATE),
            _ => true,
        },
        _ => true,
    }
}
