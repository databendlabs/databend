// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::cell::RefCell;
use std::fs;
use std::fs::File;
use std::io;
use std::path::Path;

use clap::App;
use clap::AppSettings;
use flate2::read::GzDecoder;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use tar::Archive;

use crate::cmds::command::Command;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct GetCommand {
    conf: Config,
    clap: RefCell<App<'static, 'static>>,
}

impl GetCommand {
    pub fn create(conf: Config) -> Self {
        let clap = RefCell::new(
            App::new("get")
                .setting(AppSettings::DisableVersion)
                .setting(AppSettings::DisableHelpSubcommand)
                .setting(AppSettings::ColoredHelp)
                .subcommand(App::new("update").about(
                    "Check already-downloaded packages for available updates and install the newest versions available",
                )),
        );
        GetCommand { conf, clap }
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

impl Command for GetCommand {
    fn name(&self) -> &str {
        "get"
    }

    fn about(&self) -> &str {
        "Download the latest version packages from remote to subdirectory of the current namespace"
    }

    fn is(&self, s: &str) -> bool {
        s.contains(self.name())
    }

    fn exec(&self, writer: &mut Writer, args: String) -> Result<()> {
        match self
            .clap
            .borrow_mut()
            .clone()
            .get_matches_from_safe(args.split(' '))
        {
            Ok(_matches) => {
                let arch = self.get_architecture()?;
                writer.writeln("Arch", arch.as_str());

                let latest_tag = self.get_latest_tag()?;
                writer.writeln("Tag", latest_tag.as_str());

                // Create download dir.
                let bin_download_dir = format!(
                    "{}/downloads/{}",
                    self.conf.datafuse_dir.clone(),
                    latest_tag
                );
                fs::create_dir_all(bin_download_dir.clone()).unwrap();

                // Create bin dir.
                let bin_unpack_dir =
                    format!("{}/bin/{}", self.conf.datafuse_dir.clone(), latest_tag);
                fs::create_dir_all(bin_unpack_dir.clone()).unwrap();

                let bin_name = format!("datafuse--{}.tar.gz", arch);
                let bin_file = format!("{}/{}", bin_download_dir, bin_name);
                writer.writeln("Binary", bin_file.as_str());
                let exists = Path::new(bin_file.as_str()).exists();

                // Download.
                if !exists {
                    let binary_url = format!(
                        "{}/{}/{}",
                        self.conf.download_url.clone(),
                        latest_tag,
                        bin_name,
                    );
                    writer.writeln("Download", binary_url.as_str());
                    let res = ureq::get(binary_url.as_str()).call()?;
                    let total_size: u64 = res.header("content-length").unwrap().parse().unwrap();
                    let pb = ProgressBar::new(total_size);
                    pb.set_style(ProgressStyle::default_bar()
                        .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                        .progress_chars("#>-"));

                    let mut out = File::create(bin_file.clone()).unwrap();
                    io::copy(&mut pb.wrap_read(res.into_reader()), &mut out).unwrap();
                }

                // Unpack.
                let tar_gz = File::open(bin_file)?;
                let tar = GzDecoder::new(tar_gz);
                let mut archive = Archive::new(tar);
                writer.writeln("Unpack to", bin_unpack_dir.as_str());
                archive.unpack(bin_unpack_dir).unwrap();

                // Write status.
                let mut status = Status::read(self.conf.clone())?;
                status.latest = latest_tag;
                status.write()?;
            }
            Err(err) => {
                writer.writeln_width("", format!("{}", err).as_str(), 0);
            }
        }

        Ok(())
    }
}
