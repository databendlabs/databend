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

use crate::cmds::PackageCommand;
use crate::cmds::VersionCommand;

#[derive(Clone, Debug)]
pub struct Config {
    //(TODO(zhihanz) remove those field as they already mentioned in Clap global flag)
    pub group: String,

    pub datafuse_dir: String,

    pub download_url: String,

    pub tag_url: String,
    pub clap: RefCell<ArgMatches<'static>>,
}

impl Config {
    pub fn create() -> Self {
        let clap = RefCell::new(
            App::new("config")
                .setting(AppSettings::ColoredHelp)
                .setting(AppSettings::DisableVersion)
                .arg(
                    Arg::with_name("group")
                        .long("group")
                        .help("Sets the group name for configuration")
                        .default_value("test")
                        .env("DATAFUSE_GROUP")
                        .global(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("datafuse_dir")
                        .long("datafuse_dir")
                        .help("Sets the directory to store datafuse binaries(query and store)")
                        .default_value("~/.datafuse")
                        .env("DATAFUSE_DIR")
                        .global(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("download_url")
                        .long("download_url")
                        .help("Sets the url to download datafuse binaries")
                        .default_value("https://github.com/datafuselabs/datafuse/releases/download")
                        .env("DOWNLOAD_URL")
                        .global(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("tag_url")
                        .long("tag_url")
                        .help("Sets the url to for datafuse tags")
                        .default_value("https://api.github.com/repos/datafuselabs/datafuse/tags")
                        .env("DOWNLOAD_URL")
                        .global(true)
                        .takes_value(true),
                )
                .subcommand(PackageCommand::generate())
                .subcommand(VersionCommand::generate())
                .get_matches(),
        );
        let config = Config {
            group: clap
                .clone()
                .into_inner()
                .value_of("group")
                .unwrap()
                .parse()
                .unwrap(),
            datafuse_dir: clap
                .clone()
                .into_inner()
                .value_of("datafuse_dir")
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
        let datafuse_dir = home_dir.join(".datafuse");
        if conf.datafuse_dir == "~/.datafuse" {
            conf.datafuse_dir = format!("{}/{}", datafuse_dir.to_str().unwrap(), conf.group);
        } else {
            conf.datafuse_dir = format!("{}/{}", conf.datafuse_dir, conf.group);
        }
        conf
    }
}
