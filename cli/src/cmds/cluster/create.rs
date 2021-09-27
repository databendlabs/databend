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
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use tar::Archive;

use crate::cmds::Config;
use crate::cmds::SwitchCommand;
use crate::cmds::Writer;
use crate::error::Result;
use crate::cmds::cluster::cluster::ClusterProfile;

#[derive(Clone)]
pub struct CreateCommand {
    conf: Config,
}

impl CreateCommand {
    pub fn create(conf: Config) -> Self {
        CreateCommand { conf }
    }

    pub fn exec_match(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => {
                match matches.value_of_t("profile") {
                    Ok(val) => {
                        match val {
                            ClusterProfile::Local => {
                                writer.write_ok(format!("local").as_str());
                                // precheck

                                // installer
                            }
                            ClusterProfile::Cluster => {
                                todo!()
                            }
                        }
                    }
                    Err(E) => {
                        log::error!("{}", E)
                    }
                }
            }
            None => {
                println!("none ");
            }
        }

        Ok(())
    }
}
