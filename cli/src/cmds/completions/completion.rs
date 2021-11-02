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

use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use clap_generate::generate;
use clap_generate::generators::Bash;
use clap_generate::generators::Zsh;
use clap_generate::Generator;

use crate::cmds::command::Command;
use crate::cmds::Writer;
use crate::error::Result;

fn print_completions<G: Generator>(gen: G, app: &mut App) {
    generate::<G, _>(gen, app, app.get_name().to_string(), &mut io::stdout());
}

#[derive(Clone)]
pub struct CompletionCommand {}

impl CompletionCommand {
    pub fn create() -> Self {
        Self {}
    }

    pub fn default() -> Self {
        Self::create()
    }
}

#[async_trait]
impl Command for CompletionCommand {
    fn name(&self) -> &str {
        "completion"
    }

    fn clap(&self) -> App<'static> {
        App::new("completion")
            .setting(AppSettings::DisableVersionFlag)
            .about("Generate auto completion scripts for bash or zsh terminal")
            .arg(
                Arg::new("completion")
                    .takes_value(true)
                    .possible_values(&["bash", "zsh"]),
            )
    }

    fn about(&self) -> &'static str {
        "Generate auto completion scripts for bash or zsh terminal"
    }

    fn subcommands(&self) -> Vec<Arc<dyn Command>> {
        vec![]
    }

    fn is(&self, s: &str) -> bool {
        self.name() == s
    }

    async fn exec_matches(&self, _writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        let mut clap = self.clap();
        match args.unwrap().value_of("completion") {
            Some("bash") => print_completions::<Bash>(Bash, &mut clap),
            Some("zsh") => print_completions::<Zsh>(Zsh, &mut clap),
            _ => panic!("Unknown generator"),
        }
        Ok(())
    }
}
