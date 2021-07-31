// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use ansi_term::Colour::Blue;

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
        writer.writeln(format!("{}", Blue.paint(args.as_str())).as_str(), "");
        Ok(())
    }
}
