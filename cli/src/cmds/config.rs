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

use std::cell::RefCell;

use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;

use crate::cmds::ClusterCommand;
use crate::cmds::PackageCommand;
use crate::cmds::VersionCommand;
use crate::cmds::queries::query::QueryCommand;
use std::str::FromStr;
use std::thread::sleep;
use std::time;
use common_base::tokio::runtime;
use crate::error::CliError;

#[derive(Clone, Debug)]
pub struct Config {
    //(TODO(zhihanz) remove those field as they already mentioned in Clap global flag)
    pub group: String,

    pub databend_dir: String,

    pub download_url: String,

    pub tag_url: String,
    pub clap: RefCell<ArgMatches>,
}

pub enum MirrorType {
    GITHUB,
    LOCAL(String),
}

pub trait MirrorAsset {
    const BASE_URL: String;
    const DATABEND_URL: String;
    const DATABEND_TAG_URL: String;
    const CLIENT_URL: String;
    fn is_ok(&self) -> bool {
        for i in 0..5 {
            if let Ok(res) = ureq::get(self.get_base_url().as_str()).call() {
                return res.status()%100 != 4 && res.status()%100 != 5
            } else {
                sleep(time::Duration::from_secs(1));
            }
        }
        return false
    }
    fn get_base_url(&self) -> String;
    fn get_databend_url(&self) -> String;
    fn get_databend_tag_url(&self) -> String;
    fn get_client_url(&self) -> String;
}

pub struct GithubMirror {}

impl MirrorAsset for GithubMirror {
    const BASE_URL: String = "https://github.com".to_string();
    const DATABEND_URL: String = "https://github.com/datafuselabs/databend/releases/download".to_string();
    const DATABEND_TAG_URL: String = "https://api.github.com/repos/datafuselabs/databend/tags".to_string();
    const CLIENT_URL: String = "https://github.com/ZhiHanZ/usql/releases/download".to_string();
    fn get_base_url(&self) -> String {
        return GithubMirror::BASE_URL
    }
    fn get_databend_url(&self) -> String {
        return GithubMirror::DATABEND_URL
    }
    fn get_databend_tag_url(&self) -> String {
        return GithubMirror::DATABEND_TAG_URL
    }
    fn get_client_url(&self) -> String {
        return GithubMirror::CLIENT_URL
    }
}

struct MirrorFactory;
impl MirrorFactory {
    fn new_mirror(s: &MirrorType) -> Box<dyn MirrorAsset> {
        match s {
            MirrorType::GITHUB => Box::new(GithubMirror {}),
            MirrorType::LOCAL(_) => todo!(),
        }
    }
}

// Implement the trait
impl FromStr for MirrorType {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<MirrorType, &'static str> {
        match s {
            "local" => Ok(MirrorType::LOCAL("".to_string())),
            "github" => Ok(MirrorType::GITHUB),
            _ => Err("no match for mirror"),
        }
    }
}

// Select a mirror that could be used for asset downloading
pub fn choose_mirror(mirrors: Vec<Box<dyn MirrorAsset>>) -> Result<Box<dyn MirrorAsset>, E>{

    for mirror in mirrors {
        if mirror.is_ok() {
            return Ok(mirror)
        }
    }
    Err(CliError::Unknown("no mirror available".to_string()))
}


impl Config {
    pub(crate) fn build_cli() -> App<'static> {
        App::new("bendctl")
            .setting(AppSettings::ColoredHelp)
            .arg(
                Arg::new("group")
                    .long("group")
                    .about("Sets the group name for configuration")
                    .default_value("test")
                    .env("DATABEND_GROUP")
                    .global(true)
                    .takes_value(true),
            )
            .arg(
                Arg::new("databend_dir")
                    .long("databend_dir")
                    .about("Sets the directory to store databend binaries(query and store)")
                    .default_value("~/.databend")
                    .env("databend_dir")
                    .global(true)
                    .takes_value(true)
                    .value_hint(clap::ValueHint::DirPath),
            )
            .arg(
                Arg::new("databend_mirror")
                    .long("donwload_mirror")
                    .about("Sets the mirror type to download databend and relevant binaries")
                    .env("DOWNLOAD_MIRROR")
                    .possible_values(&["github"])
                    .takes_value(true),
            )
            .subcommand(
                App::new("completion")
                    .setting(AppSettings::ColoredHelp)
                    .setting(AppSettings::DisableVersionFlag)
                    .about("Generate auto completion scripts for bash or zsh terminal")
                    .arg(
                        Arg::new("completion")
                            .takes_value(true)
                            .possible_values(&["bash", "zsh"]),
                    ),
            )
            .subcommand(PackageCommand::generate())
            .subcommand(VersionCommand::generate())
            .subcommand(ClusterCommand::generate())
            .subcommand( QueryCommand::generate())
    }
    pub fn create() -> Self {
        let clap = RefCell::new(Config::build_cli().get_matches());
        let config = Config {
            group: clap
                .clone()
                .into_inner()
                .value_of("group")
                .unwrap()
                .parse()
                .unwrap(),
            databend_dir: clap
                .clone()
                .into_inner()
                .value_of("databend_dir")
                .unwrap()
                .parse()
                .unwrap(),
            download_url: clap
                .clone()
                .into_inner()
                .value_of("download_url")
                .unwrap()
                .parse()
                .unwrap(),
            tag_url: clap
                .clone()
                .into_inner()
                .value_of("tag_url")
                .unwrap()
                .parse()
                .unwrap(),
            clap,
        };
        Config::build(config)
    }
    fn build(mut conf: Config) -> Self {
        let home_dir = dirs::home_dir().unwrap();
        let databend_dir = home_dir.join(".databend");
        if conf.databend_dir == "~/.databend" {
            conf.databend_dir = format!("{}/{}", databend_dir.to_str().unwrap(), conf.group);
        } else {
            conf.databend_dir = format!("{}/{}", conf.databend_dir, conf.group);
        }
        conf
    }
}
