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

use colored::Colorize;

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

impl Command for CommentCommand {
    fn name(&self) -> &str {
        "comment"
    }

    fn about(&self) -> &str {
        "# your comments"
    }

    fn is(&self, s: &str) -> bool {
        s.starts_with('#')
    }

    fn exec(&self, writer: &mut Writer, args: String) -> Result<()> {
        writer.writeln(format!("{}", args.green()).as_str());
        Ok(())
    }
}
