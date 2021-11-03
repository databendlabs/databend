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

use std::fmt::Display;
use std::fmt::Formatter;
use std::thread::sleep;
use std::time::Duration;

use clap::App;
use clap::ArgMatches;
use colored::Colorize;
use serde::Deserialize;
use serde::Serialize;

use crate::cmds::Status;
use crate::error::CliError;

pub(crate) const GITHUB_BASE_URL: &str = "https://api.github.com/repos/datafuselabs/databend/tags";
pub(crate) const GITHUB_DATABEND_URL: &str =
    "https://github.com/datafuselabs/databend/releases/download";
pub(crate) const GITHUB_DATABEND_TAG_URL: &str =
    "https://api.github.com/repos/datafuselabs/databend/tags";
pub(crate) const GITHUB_PLAYGROUND_URL: &str =
    "https://github.com/datafuselabs/databend-playground/releases/download";

pub(crate) const REPO_BASE_URL: &str = "https://repo.databend.rs/databend/tags.json";
pub(crate) const REPO_DATABEND_URL: &str = "https://repo.databend.rs/databend";
pub(crate) const REPO_DATABEND_TAG_URL: &str = "https://repo.databend.rs/databend/tags.json";
pub(crate) const REPO_PLAYGROUND_URL: &str = "https://repo.databend.rs/databend";

#[derive(Clone, Debug)]
pub struct Config {
    //(TODO(zhihanz) remove those field as they already mentioned in Clap global flag)
    pub group: String,
    pub mode: Mode,
    pub databend_dir: String,
    pub mirror: CustomMirror,
    pub clap: ArgMatches,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Mode {
    Sql,
    Admin,
}

impl Display for Mode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            Mode::Sql => write!(f, "{}", String::from("sql").purple()),
            Mode::Admin => write!(f, "{}", String::from("admin").red()),
        }
    }
}

pub trait MirrorAsset {
    fn is_ok(&self) -> bool {
        if let Ok(res) = ureq::get(self.get_base_url().as_str()).call() {
            return res.status() % 100 != 4 && res.status() % 100 != 5;
        }
        false
    }
    fn get_base_url(&self) -> String;
    fn get_databend_url(&self) -> String;
    fn get_databend_tag_url(&self) -> String;
    fn get_playground_url(&self) -> String;
    fn to_mirror(&self) -> CustomMirror {
        CustomMirror {
            base_url: self.get_base_url(),
            databend_url: self.get_databend_url(),
            databend_tag_url: self.get_databend_tag_url(),
            playground_url: self.get_playground_url(),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct GithubMirror {}

impl MirrorAsset for GithubMirror {
    fn get_base_url(&self) -> String {
        GITHUB_BASE_URL.to_string()
    }
    fn get_databend_url(&self) -> String {
        GITHUB_DATABEND_URL.to_string()
    }
    fn get_databend_tag_url(&self) -> String {
        GITHUB_DATABEND_TAG_URL.to_string()
    }

    fn get_playground_url(&self) -> String {
        GITHUB_PLAYGROUND_URL.to_string()
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct RepoMirror {}

impl MirrorAsset for RepoMirror {
    fn get_base_url(&self) -> String {
        REPO_BASE_URL.to_string()
    }
    fn get_databend_url(&self) -> String {
        REPO_DATABEND_URL.to_string()
    }
    fn get_databend_tag_url(&self) -> String {
        REPO_DATABEND_TAG_URL.to_string()
    }

    fn get_playground_url(&self) -> String {
        REPO_PLAYGROUND_URL.to_string()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct CustomMirror {
    pub base_url: String,
    pub databend_url: String,
    pub databend_tag_url: String,
    pub playground_url: String,
}

impl CustomMirror {
    fn new(
        base_url: String,
        databend_url: String,
        databend_tag_url: String,
        playground_url: String,
    ) -> Self {
        CustomMirror {
            base_url,
            databend_url,
            databend_tag_url,
            playground_url,
        }
    }
}

impl MirrorAsset for CustomMirror {
    fn get_base_url(&self) -> String {
        self.base_url.clone()
    }

    fn get_databend_url(&self) -> String {
        self.databend_url.clone()
    }

    fn get_databend_tag_url(&self) -> String {
        self.databend_tag_url.clone()
    }

    fn get_playground_url(&self) -> String {
        self.playground_url.clone()
    }
}

// choose one mirror which could be connected
// if the mirror user provided works, it would choose it as default mirror, otherwise it would panic
// if user have not provided a mirror, it would validate on mirror stored in status and warn
// user if it could not be connected.
// in default situation(no mirror stored in status), we provided a pool of possible mirrors to use.
// it would select one working mirror as default mirror
pub fn choose_mirror(conf: &Config) -> Result<CustomMirror, CliError> {
    // try user defined mirror source at first
    let conf = conf.clone();
    let default = RepoMirror {};
    if default.to_mirror() != conf.mirror {
        let custom: CustomMirror = conf.mirror.clone();
        for _ in 0..2 {
            let custom: CustomMirror = conf.mirror.clone();
            if custom.is_ok() {
                let mut status = Status::read(conf).expect("cannot configure status");
                status.mirrors = Some(custom.to_mirror());
                status.write()?;
                return Ok(custom);
            } else {
                sleep(Duration::from_secs(1));
            }
        }
        return Err(CliError::Unknown(format!(
            "cannot connect to the provided mirror {:?}",
            custom
        )));
    }

    let status = Status::read(conf).expect("cannot configure status");
    if let Some(mirror) = status.mirrors {
        return Ok(mirror);
    }

    let default_mirrors: Vec<Box<dyn MirrorAsset>> =
        vec![Box::new(RepoMirror {}), Box::new(GithubMirror {})];
    for _ in 0..2 {
        for i in &default_mirrors {
            if i.is_ok() {
                return Ok(i.to_mirror());
            } else {
                sleep(Duration::from_secs(1));
            }
        }
    }

    Err(CliError::Unknown(
        "cannot find possible mirror to connect".to_string(),
    ))
}

impl Config {
    pub fn create(clap: App<'static>) -> Self {
        let clap = clap.get_matches();
        let config = Config {
            group: clap.clone().value_of("group").unwrap().parse().unwrap(),
            mode: Mode::Sql,
            databend_dir: clap
                .clone()
                .value_of("databend_dir")
                .unwrap()
                .parse()
                .unwrap(),

            mirror: CustomMirror::new(
                clap.clone()
                    .value_of("validation_url")
                    .unwrap()
                    .parse()
                    .unwrap(),
                clap.clone()
                    .value_of("download_url")
                    .unwrap()
                    .parse()
                    .unwrap(),
                clap.clone().value_of("tag_url").unwrap().parse().unwrap(),
                clap.clone()
                    .value_of("playground_url")
                    .unwrap()
                    .parse()
                    .unwrap(),
            ),
            clap,
        };
        Config::build(config)
    }

    pub fn default() -> Self {
        Config {
            group: "".into(),
            mode: Mode::Sql,
            databend_dir: "~/.databend".into(),
            clap: Default::default(),
            mirror: CustomMirror {
                base_url: "".into(),
                databend_url: "".into(),
                databend_tag_url: "".into(),
                playground_url: "".into(),
            },
        }
    }

    fn build(mut conf: Config) -> Self {
        let home_dir = dirs::home_dir().unwrap();
        let databend_dir = home_dir.join(".databend");

        if conf.databend_dir == "~/.databend" {
            conf.databend_dir = format!("{}/{}", databend_dir.to_str().unwrap(), conf.group);
        } else {
            conf.databend_dir = format!("{}/{}", conf.databend_dir, conf.group);
        }
        let res = choose_mirror(&conf);
        if let Ok(mirror) = res {
            conf.mirror = mirror.clone();
            let mut status = Status::read(conf.clone()).expect("cannot read status");
            status.mirrors = Some(mirror);
            status.write().expect("cannot write status");
        } else {
            panic!("{}", format!("{:?}", res.unwrap_err()))
        }
        conf
    }
}
