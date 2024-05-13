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

use databend_common_ast::ast::Statement;
use databend_common_ast::parser::token::TokenKind;
use databend_common_ast::parser::token::Tokenizer;
use databend_common_base::base::tokio;
use databend_common_base::base::tokio::io::AsyncRead;
use databend_common_base::base::tokio::time::Instant;
use databend_common_config::DATABEND_COMMIT_VERSION;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::ValueType;
use databend_common_expression::SendableDataBlockStream;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_sql::plans::Plan;
use databend_common_sql::Planner;
use futures_util::StreamExt;
use rustyline::config::Builder;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::CompletionType;
use rustyline::Editor;

use super::config::Config;
use super::config::OutputFormat;
use super::config::Settings;
use super::display::ChunkDisplay;
use super::display::FormatDisplay;
use super::helper::CliHelper;
use crate::interpreters::InterpreterFactory;
use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;

/// Session to execute local commands.
pub(crate) struct SessionExecutor {
    session: Arc<Session>,
    is_repl: bool,

    settings: Settings,
    query: String,
    keywords: Arc<Vec<String>>,
}

static PROMPT_SQL: &str = "select name from system.tables union all select name from system.columns union all select name from system.databases union all select name from system.functions";

impl SessionExecutor {
    pub async fn try_new(is_repl: bool, output_format: &str) -> Result<Self> {
        let mut keywords = Vec::with_capacity(1024);
        let session_manager = SessionManager::instance();

        let session = session_manager
            .create_session(SessionType::Local)
            .await
            .unwrap();

        session_manager.register_session(session.clone())?;

        let mut user = UserInfo::new_no_auth("root", "%");
        user.grants.grant_privileges(
            &GrantObject::Global,
            UserPrivilegeSet::available_privileges_on_global(),
        );
        session.set_authed_user(user, None).await.unwrap();

        let config = Config::load();
        let mut settings = Settings::default();
        if is_repl {
            settings.display_pretty_sql = true;
            settings.show_progress = true;
            settings.show_stats = true;
            settings.output_format = OutputFormat::Table;
        } else {
            settings.output_format = OutputFormat::Tsv;
        }

        if !output_format.is_empty() {
            settings.inject_ctrl_cmd("output_format", output_format)?;
        }

        settings.merge_config(config.settings);

        if is_repl {
            println!("Welcome to Databend, version {}.", *DATABEND_COMMIT_VERSION);
            println!();

            let rows = Self::query(&session, PROMPT_SQL).await;
            if let Ok((mut rows, _, _, _)) = rows {
                while let Some(row) = rows.next().await {
                    if let Ok(row) = row {
                        let num_rows = row.num_rows();
                        let col = row.columns()[0]
                            .value
                            .convert_to_full_column(&DataType::String, num_rows);
                        let value = StringType::try_downcast_column(&col).unwrap();
                        for r in value.iter() {
                            keywords.push(r.to_string());
                        }
                    }
                }
            }
        }

        Ok(Self {
            session,
            is_repl,
            settings,
            query: String::new(),
            keywords: Arc::new(keywords),
        })
    }

    async fn query(
        session: &Arc<Session>,
        sql: &str,
    ) -> Result<(SendableDataBlockStream, Arc<QueryContext>, Plan, Statement)> {
        let context = session.create_query_context().await?;
        let mut planner = Planner::new(context.clone());
        let (plan, extras) = planner.plan_sql(sql).await?;

        let interpreter = InterpreterFactory::get(context.clone(), &plan).await?;
        let ctx = context.clone();
        Ok((
            interpreter.execute(context).await?,
            ctx,
            plan,
            extras.statement,
        ))
    }

    pub async fn handle(&mut self, query_sql: &str) {
        if self.is_repl {
            self.handle_repl().await;
        } else if query_sql.is_empty() {
            self.handle_reader(tokio::io::stdin()).await;
        } else {
            self.handle_reader(query_sql.as_bytes()).await;
        }
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

    pub async fn handle_reader<R: AsyncRead + Unpin>(&mut self, r: R) {
        let start = Instant::now();

        use tokio::io::AsyncBufReadExt;
        let mut lines = tokio::io::BufReader::new(r).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let queries = self.append_query(&line);
            for query in queries {
                if let Err(e) = self.handle_query(false, &query).await {
                    eprintln!("{}", e);
                    return;
                }
            }
        }

        // if the last query is not finished with `;`, we need to execute it.
        let query = self.query.trim().to_owned();
        if !query.is_empty() {
            self.query.clear();
            if let Err(e) = self.handle_query(false, &query).await {
                eprintln!("{}", e);
            }
        }
        if self.settings.time {
            println!("{:.3}", start.elapsed().as_secs_f64());
        }
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

        while let Some(Ok(token)) = tokenizer.next() {
            match token.kind {
                TokenKind::SemiColon => {
                    if in_comment {
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
                    if !in_comment {
                        self.query.push_str(&line[start..token.span.end()]);
                    }
                }
            }
            start = token.span.end();
        }

        queries
    }

    pub async fn handle_query(&mut self, is_repl: bool, query: &str) -> Result<bool> {
        let query = query.trim_end_matches(';').trim();
        if is_repl && (query == "exit" || query == "quit") {
            return Ok(true);
        }

        if is_repl && query.starts_with('.') {
            let query = query
                .trim_start_matches('.')
                .split_whitespace()
                .collect::<Vec<_>>();
            if query.len() != 2 {
                return Err(ErrorCode::BadArguments(
                    "Control command error, must be syntax of `.cmd_name cmd_value`.".to_string(),
                ));
            }
            self.settings.inject_ctrl_cmd(query[0], query[1])?;
            return Ok(false);
        }

        let start = Instant::now();

        let (stream, ctx, plan, stmt) = Self::query(&self.session, query).await?;

        let mut displayer =
            FormatDisplay::new(ctx, self.is_repl, &self.settings, stmt, start, plan, stream);
        displayer.display().await?;

        Ok(false)
    }

    async fn prompt(&self) -> String {
        if !self.query.is_empty() {
            "> ".to_owned()
        } else {
            let mut prompt = self.settings.prompt.clone();
            prompt = prompt.replace("{database}", &self.session.get_current_database());
            format!("{} ", prompt.trim_end())
        }
    }
}

fn get_history_path() -> String {
    format!(
        "{}/.databend_history",
        std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
    )
}
