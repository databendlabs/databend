// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::cmds::command::Command;
use crate::cmds::Config;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct ClusterCommand {
    conf: Config,
}

impl ClusterCommand {
    pub fn create(conf: Config) -> Self {
        ClusterCommand { conf }
    }
}

impl Command for ClusterCommand {
    fn name(&self) -> &str {
        "cluster"
    }

    fn about(&self) -> &str {
        "Start the fuse-query and fuse-store service"
    }

    fn is(&self, s: &str) -> bool {
        s.starts_with(self.name())
    }

    fn exec(&self, _writer: &mut Writer, _args: String) -> Result<()> {
        Ok(())
    }
}
