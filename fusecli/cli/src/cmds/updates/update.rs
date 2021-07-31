// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fs::File;
use std::io;

use crate::cmds::command::Command;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct UpdateCommand {}

impl UpdateCommand {
    pub fn create() -> Self {
        UpdateCommand {}
    }

    pub fn get_architecture(&self) -> Result<String> {
        let os = std::env::consts::OS;
        let arch = std::env::consts::ARCH;
        let mut clib = "gnu";

        // Check musl
        let (_, musl, _) = run_script::run_script!(r#"ldd --version 2>&1 | grep -q 'musl'"#)?;
        if !musl.is_empty() {
            clib = "musl";
        }

        let os = match os {
            "darwin" => "apple-darwin".to_string(),
            "linux" => format!("unknown-linux-{}", clib),
            _ => os.to_string(),
        };

        Ok(format!("{}-{}", arch, os))
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
        let arch = self.get_architecture()?;
        writer.writeln("os", arch.as_str());

        let res = ureq::get("https://sh.rustup.rs").call()?;
        println!("--{:?}", res.header("content-length"));
        let mut out = File::create("/tmp/foo").expect("Unable to create file");
        io::copy(&mut res.into_reader(), &mut out).expect("failed to copy content");

        Ok(())
    }
}
