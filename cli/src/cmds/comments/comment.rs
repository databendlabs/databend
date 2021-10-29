// Copyright 2020 Datafuse Labs.
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

use async_trait::async_trait;
use colored::Colorize;
use std::sync::Arc;

use clap::App;
use clap::ArgMatches;
use crate::cmds::command::Command;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct CommentCommand {}

impl CommentCommand {
    pub fn create() -> Self {
        CommentCommand {}
    }
}

#[async_trait]
impl Command for CommentCommand {
    fn name(&self) -> &str {
        "comment"
    }

    fn about(&self) -> &str {
        "# your comments"
    }

    fn clap(&self) -> App<'static> {
        App::new("#").about("your comments")
    }

    fn subcommands(&self) -> Vec<Arc<dyn Command>> {
        vec![]
    }

    fn is(&self, s: &str) -> bool {
        s.starts_with('#')
    }

    async fn exec_matches(&self, writer: &mut Writer, _matches: Option<&ArgMatches>) -> Result<()> {
        // TODO writer.writeln(format!("{}", args.green()).as_str());
        Ok(())
    }

}
