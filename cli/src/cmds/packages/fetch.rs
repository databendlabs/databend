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

use std::fs;
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use clap::App;
use clap::Arg;
use clap::ArgMatches;
use flate2::read::GzDecoder;
use fs_extra::dir;
use fs_extra::move_items;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use tar::Archive;

use crate::cmds::command::Command;
use crate::cmds::Config;
use crate::cmds::SwitchCommand;
use crate::cmds::Writer;
use crate::error::CliError;
use crate::error::Result;

#[derive(Clone)]
pub struct FetchCommand {
    conf: Config,
}

pub fn unpack(tar_file: &str, target_dir: &str) -> Result<()> {
    let tar_gz = File::open(tar_file)?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);
    let res = archive.unpack(target_dir);
    return match res {
        Ok(_) => {
            if Path::new(format!("{}/GNUSparseFile.0", target_dir).as_str()).exists()
                && Path::new(format!("{}/GNUSparseFile.0", target_dir).as_str()).is_dir()
            {
                let options = dir::CopyOptions::new(); //Initialize default values for CopyOptions

                let mut from_paths = Vec::new();
                from_paths.push(format!("{}/GNUSparseFile.0/databend-query", target_dir));
                from_paths.push(format!("{}/GNUSparseFile.0/databend-meta", target_dir));
                move_items(&from_paths, target_dir, &options)
                    .expect("cannot move executable files");
                if let Ok(()) = std::fs::remove_dir_all(format!("{}/GNUSparseFile.0", target_dir)) {
                }
            }
            Ok(())
        }
        Err(e) => Err(CliError::Unknown(format!(
            "cannot unpack file {} to {}, error: {}",
            tar_file, target_dir, e
        ))),
    };
}

pub fn download_and_unpack(
    url: &str,
    download_file_name: &str,
    target_dir: &str,
    exist: Option<String>,
) -> Result<()> {
    if exist.is_some() && Path::new(exist.unwrap().as_str()).exists() {
        return Ok(());
    }
    if let Err(e) = download(url, download_file_name) {
        return Err(e);
    }
    if let Err(e) = unpack(download_file_name, target_dir) {
        return Err(e);
    }
    Ok(())
}

pub fn download(url: &str, target_file: &str) -> Result<()> {
    let res = ureq::get(url).call()?;
    let total_size: u64 = res
        .header("content-length")
        .expect("cannot fetch content length from header")
        .parse()
        .expect("cannot parse content header");
    let pb = ProgressBar::new(total_size);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .progress_chars("#>-"));

    let mut out = File::create(target_file).unwrap_or_else(|_| {
        panic!(
            "{}",
            format!("cannot create target file {}", target_file).as_str()
        )
    });
    io::copy(&mut pb.wrap_read(res.into_reader()), &mut out)
        .expect("cannot download to target file");
    Ok(())
}

//(TODO(zhihanz)) general get_architecture similar to install-bendctl.sh
pub fn get_rust_architecture() -> Result<String> {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let mut clib = "gnu";

    // Check musl
    let (_, musl, _) = run_script::run_script!(r#"ldd --version 2>&1 | grep -q 'musl'"#)?;
    if !musl.is_empty() {
        clib = "musl";
    }

    // Check rosetta
    let (_, rosetta, _) = run_script::run_script!(r#"uname -a"#)?;
    if rosetta.contains("Darwin") && rosetta.contains("arm64") {
        return Ok("x86_64-apple-darwin".to_string());
    }
    let os = match os {
        "darwin" => "apple-darwin".to_string(),
        "macos" => "apple-darwin".to_string(),
        "linux" => format!("unknown-linux-{}", clib),
        _ => {
            return Err(CliError::Unknown(format!(
                "Unsupported architecture os: {}, arch {}",
                std::env::consts::OS,
                std::env::consts::ARCH
            )))
        }
    };

    Ok(format!("{}-{}", arch, os))
}

fn get_latest_tag(conf: &Config) -> Result<String> {
    let tag_url = conf.mirror.databend_tag_url.clone();
    let resp = ureq::get(tag_url.as_str()).call()?;
    let json: serde_json::Value = resp.into_json().unwrap();
    Ok(format!("{}", json[0]["name"]).replace("\"", ""))
}

pub fn get_version(conf: &Config, version: Option<String>) -> Result<String> {
    if version.is_none() || version.as_ref().unwrap().eq("latest") {
        return get_latest_tag(conf);
    }
    Ok(version.unwrap())
}

impl FetchCommand {
    pub fn create(conf: Config) -> Self {
        FetchCommand { conf }
    }

    async fn download_databend(
        &self,
        arch: &str,
        tag: &str,
        writer: &mut Writer,
        args: Option<&ArgMatches>,
    ) -> Result<()> {
        // Create download dir.
        let bin_download_dir = format!("{}/downloads/{}", self.conf.databend_dir.clone(), tag);
        fs::create_dir_all(bin_download_dir.clone()).unwrap();

        // Create bin dir.
        let bin_unpack_dir = format!("{}/bin/{}", self.conf.databend_dir.clone(), tag);
        fs::create_dir_all(bin_unpack_dir.clone()).unwrap();

        let bin_name = format!("databend-{}-{}.tar.gz", tag, arch);
        let bin_file = format!("{}/{}", bin_download_dir, bin_name);
        let binary_url = format!(
            "{}/{}/{}",
            self.conf.mirror.databend_url.clone(),
            tag,
            bin_name,
        );
        if let Err(e) = download_and_unpack(
            &*binary_url,
            &*bin_file.clone(),
            &*bin_unpack_dir,
            Some(bin_file),
        ) {
            writer.write_err(format!("Cannot download or unpack error: {:?}", e))
        }
        let switch = SwitchCommand::create(self.conf.clone());
        switch.exec_matches(writer, args).await
    }
}

#[async_trait]
impl Command for FetchCommand {
    fn name(&self) -> &str {
        "fetch"
    }

    fn clap(&self) -> App<'static> {
        App::new("fetch").about(self.about()).arg(
            Arg::new("version")
                .about("Version of databend package to fetch")
                .default_value("latest"),
        )
    }

    fn subcommands(&self) -> Vec<Arc<dyn Command>> {
        vec![]
    }

    fn about(&self) -> &'static str {
        "Fetch the given version binary package"
    }

    fn is(&self, s: &str) -> bool {
        s.contains(self.name())
    }

    async fn exec_matches(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => {
                let arch = get_rust_architecture();
                if let Ok(arch) = arch {
                    writer.write_ok(format!("Arch {}", arch));
                    let current_tag = get_version(
                        &self.conf,
                        matches.value_of("version").map(|e| e.to_string()),
                    )?;
                    writer.write_ok(format!("Tag {}", current_tag));
                    if let Err(e) = self
                        .download_databend(&arch, current_tag.as_str(), writer, args)
                        .await
                    {
                        writer.write_err(format!("{:?}", e));
                    }
                } else {
                    writer.write_err(format!("{:?}", arch.unwrap_err()));
                }
            }
            None => {
                println!("none ");
            }
        }

        Ok(())
    }
}
