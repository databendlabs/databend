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

use clap::Parser;

// Add options when run sqllogictest, such as specific dir or file
#[derive(Parser, Debug, Clone)]
pub struct SqlLogicTestArgs {
    // Set specific dir to run
    #[arg(
        short = 'd',
        long = "run_dir",
        help = "Run sqllogictests in specific directory, the arg is optional"
    )]
    pub dir: Option<String>,

    // Set specific test file to run
    #[arg(
        short = 'f',
        long = "run_file",
        help = "Run sqllogictests in specific test file, the arg is optional"
    )]
    pub file: Option<String>,

    // Set specific dir to skip
    #[arg(
        short = 's',
        long = "skip_dir",
        help = "Skip sqllogictests in specific directory, the arg is optional"
    )]
    pub skipped_dir: Option<String>,

    // Set specific file to skip
    #[arg(
        short = 'x',
        long = "skip_file",
        help = "Skip sqllogictests in specific test file, the arg is optional"
    )]
    pub skipped_file: Option<String>,

    // Set handler to run tests
    #[arg(
        short = 'l',
        long = "handlers",
        use_value_delimiter = true,
        value_delimiter = ',',
        help = "Choose handlers to run tests, support mysql, http handler, the arg is optional. If use multiple handlers, please use \',\' to split them"
    )]
    pub handlers: Option<Vec<String>>,

    // Choose suits to run
    #[arg(
        short = 'u',
        long = "suites",
        help = "The tests to be run will come from under suits",
        default_value = "tests/sqllogictests/suites"
    )]
    pub suites: String,

    // If enable complete mode
    #[arg(
        short = 'c',
        long = "complete",
        default_missing_value = "true",
        help = "The arg is used to enable auto complete mode"
    )]
    pub complete: bool,

    // If close fast fail.
    #[arg(
        long = "no-fail-fast",
        default_missing_value = "true",
        help = "The arg is used to cancel fast fail"
    )]
    pub no_fail_fast: bool,

    #[arg(
        short = 'p',
        long = "parallel",
        default_value_t = 1,
        help = "The arg is used to set parallel number"
    )]
    pub parallel: usize,

    #[arg(
        long = "enable_sandbox",
        default_missing_value = "true",
        help = "The arg is used to enable sandbox_tenant"
    )]
    pub enable_sandbox: bool,

    #[arg(
        long = "debug",
        default_missing_value = "true",
        help = "The arg is used to enable debug mode which would print some debug messages"
    )]
    pub debug: bool,

    #[arg(
        long = "bench",
        default_missing_value = "true",
        help = "The arg is used to run benchmark instead of test"
    )]
    pub bench: bool,

    // Set specific the database to connect
    #[arg(
        long = "database",
        default_value = "default",
        help = "Specify the database to connnect, the default database is 'default'"
    )]
    pub database: String,
}
