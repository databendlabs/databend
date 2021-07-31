// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fs::File;
use std::io;

use indicatif::ProgressBar;
use indicatif::ProgressStyle;

use crate::cmds::command::Command;
use crate::cmds::Config;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct UpdateCommand {
    conf: Config,
}

impl UpdateCommand {
    pub fn create(conf: Config) -> Self {
        UpdateCommand { conf }
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

    pub fn get_latest_tag(&self) -> Result<String> {
        let tag_url = self.conf.tag_url.clone();
        let resp = ureq::get(tag_url.as_str()).call()?;
        let json: serde_json::Value = resp.into_json().unwrap();

        Ok(format!("{}", json[0]["name"]).replace("\"", ""))
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
        writer.writeln("Arch", arch.as_str());

        let latest_tag = self.get_latest_tag()?;
        writer.writeln("Latest Tag", latest_tag.as_str());

        let binary_url = format!(
            "{}/{}/{}--{}.tar.gz",
            self.conf.download_url.clone(),
            latest_tag,
            "datafuse",
            arch
        );
        writer.writeln("Download", binary_url.as_str());

        let res = ureq::get(binary_url.as_str()).call()?;
        let total_size: u64 = res.header("content-length").unwrap().parse().unwrap();
        let pb = ProgressBar::new(total_size);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .progress_chars("#>-"));

        let mut out = File::create("/tmp/foo").unwrap();
        io::copy(&mut pb.wrap_read(res.into_reader()), &mut out).unwrap();

        Ok(())
    }
}
