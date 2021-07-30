// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::cmds::command::Command;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct UpdateCommand {}

impl UpdateCommand {
    pub fn create() -> Self {
        UpdateCommand {}
    }
}

impl Command for UpdateCommand {
    fn name(&self) -> &str {
        "update"
    }

    fn about(&self) -> &str {
        "Check and download the package to local path"
    }

    fn is(&self, s: &str) -> bool {
        self.name() == s
    }

    fn exec(&self, writer: &mut Writer) -> Result<()> {
        let (_, os_type, _) = run_script::run_script!(r#"uname -s"#)?;
        writer.writeln("os", os_type.as_str());

        Ok(())
    }
}
