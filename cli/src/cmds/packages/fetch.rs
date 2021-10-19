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

use clap::ArgMatches;
use flate2::read::GzDecoder;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use tar::Archive;

use crate::cmds::Config;
use crate::cmds::Status;
use crate::cmds::SwitchCommand;
use crate::cmds::Writer;
use crate::error::CliError;
use crate::error::Result;

const QUERY_CLIENT_VERSION: &str = "v0.9.4-databend-rc5";

#[derive(Clone)]
pub struct FetchCommand {
    conf: Config,
}

pub fn get_go_architecture() -> Result<(String, String)> {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    // Check rosetta
    let (_, rosetta, _) = run_script::run_script!(r#"uname -a"#)?;
    if rosetta.contains("Darwin") && rosetta.contains("arm64") {
        return Ok(("darwin".to_string(), "arm64".to_string()));
    }
    let goos = match os {
        "darwin" => "darwin".to_string(),
        "macos" => "darwin".to_string(),
        "linux" => "linux".to_string(),
        _ => {
            return Err(CliError::Unknown(format!(
                "Unsupported go os {}",
                std::env::consts::OS
            )));
        }
    };
    let goarch = match arch {
        "x86_64" => "amd64".to_string(),
        "aarch_64" => "arm64".to_string(),
        _ => {
            return Err(CliError::Unknown(format!(
                "Unsupported go architecture {}",
                std::env::consts::ARCH
            )));
        }
    };
    Ok((goos, goarch))
}

pub fn unpack(tar_file: &str, target_dir: &str) -> Result<()> {
    let tar_gz = File::open(tar_file)?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);
    let res = archive.unpack(target_dir);
    return match res {
        Ok(_) => Ok(()),
        Err(e) => Err(CliError::Unknown(format!(
            "cannot unpack file {} to {}, error: {}",
            tar_file, target_dir, e
        ))),
    };
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

//(TODO(zhihanz)) general get_architecture similar to install-databend.sh
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
        return Err(CliError::Unknown(
            "Unsupported architecture aarch64-apple-darwin".to_string(),
        ));
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
impl FetchCommand {
    pub fn create(conf: Config) -> Self {
        FetchCommand { conf }
    }

    fn get_latest_tag(&self) -> Result<String> {
        let tag_url = self.conf.mirror.databend_tag_url.clone();
        let resp = ureq::get(tag_url.as_str()).call()?;
        let json: serde_json::Value = resp.into_json().unwrap();

        Ok(format!("{}", json[0]["name"]).replace("\"", ""))
    }

    fn download_databend(
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
        let exists = Path::new(bin_file.as_str()).exists();
        // Download.
        if !exists {
            let binary_url = format!(
                "{}/{}/{}",
                self.conf.mirror.databend_url.clone(),
                tag,
                bin_name,
            );
            if let Err(e) = download(&binary_url, &bin_file) {
                writer.write_err(
                    format!("Cannot download from {}, error: {:?}", binary_url, e).as_str(),
                )
            }
        }

        // Unpack.
        match unpack(&bin_file, &bin_unpack_dir) {
            Ok(_) => {
                writer.write_ok(format!("Unpacked {} to {}", bin_file, bin_unpack_dir).as_str());

                // switch to fetched version
                let switch = SwitchCommand::create(self.conf.clone());
                return switch.exec_match(writer, args);
            }
            Err(e) => {
                writer.write_err(format!("{:?}", e).as_str());
            }
        };
        Ok(())
    }

    fn download_query(&self, writer: &mut Writer, _args: Option<&ArgMatches>) -> Result<()> {
        let (goos, goarch) =
            get_go_architecture().expect("cannot get architecture info for query client download");
        // Create download dir.
        let bin_download_dir = format!(
            "{}/downloads/usql/{}",
            self.conf.databend_dir.clone(),
            QUERY_CLIENT_VERSION
        );
        fs::create_dir_all(bin_download_dir.clone()).unwrap();

        // Create bin dir.
        let bin_unpack_dir = format!(
            "{}/bin/usql/{}",
            self.conf.databend_dir.clone(),
            QUERY_CLIENT_VERSION
        );
        fs::create_dir_all(bin_unpack_dir.clone()).unwrap();

        let bin_name = format!(
            "usql-databend-{}-{}-{}.tar.gz",
            QUERY_CLIENT_VERSION, goos, goarch
        );
        let bin_file = format!("{}/{}", bin_download_dir, bin_name);
        let exists = Path::new(bin_file.as_str()).exists();

        // Download.
        if !exists {
            let binary_url = format!(
                "{}/{}/{}",
                self.conf.mirror.client_url.clone(),
                QUERY_CLIENT_VERSION,
                bin_name,
            );
            if let Err(e) = download(&binary_url, &bin_file) {
                writer.write_err(
                    format!("Cannot download from {}, error: {:?}", binary_url, e).as_str(),
                )
            }
        }

        // Unpack.
        match unpack(&bin_file, &bin_unpack_dir) {
            Ok(_) => {
                // update local usql path
                let mut status = Status::read(self.conf.clone())?;
                status.query_path = Some(
                    std::fs::canonicalize(format!("{}/usql", bin_unpack_dir))
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string(),
                );
                status.write()?;
                writer.write_ok(format!("Unpacked {} to {}", bin_file, bin_unpack_dir).as_str());
            }
            Err(e) => {
                writer.write_err(format!("{:?}", e).as_str());
            }
        };
        Ok(())
    }

    pub fn exec_match(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => {
                let arch = get_rust_architecture();
                if let Ok(arch) = arch {
                    writer.write_ok(format!("Arch {}", arch).as_str());
                    let current_tag = if matches.value_of("version").unwrap() == "latest" {
                        self.get_latest_tag()?
                    } else {
                        matches.value_of("version").unwrap().to_string()
                    };
                    writer.write_ok(format!("Tag {}", current_tag).as_str());
                    if let Err(e) = self.download_databend(&arch, &current_tag, writer, args) {
                        writer.write_err(format!("{:?}", e).as_str());
                    }
                    if let Err(e) = self.download_query(writer, args) {
                        writer.write_err(format!("{:?}", e).as_str());
                    }
                } else {
                    writer.write_err(format!("{:?}", arch.unwrap_err()).as_str());
                }
            }
            None => {
                println!("none ");
            }
        }

        Ok(())
    }
}
