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

use clap::ArgMatches;

use crate::cmds::Config;
use crate::cmds::ListCommand;
use crate::cmds::Status;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct SwitchCommand {
    conf: Config,
}

impl SwitchCommand {
    pub fn create(conf: Config) -> Self {
        SwitchCommand { conf }
    }

    fn get_latest_tag(&self) -> Result<String> {
        let tag_url = self.conf.tag_url.clone();
        let resp = ureq::get(tag_url.as_str()).call()?;
        let json: serde_json::Value = resp.into_json().unwrap();

        Ok(format!("{}", json[0]["name"]).replace("\"", ""))
    }

    pub fn exec_match(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => {
                let bin_dir = format!("{}/bin", self.conf.datafuse_dir.clone());
                let paths = fs::read_dir(bin_dir)?;
                let current_tag = if matches.value_of("version").unwrap() == "latest" {
                    self.get_latest_tag()?
                } else {
                    matches.value_of("version").unwrap().to_string()
                };
                let mut exists = false;
                for path in paths {
                    let path = path.unwrap().path();
                    let version = path.file_name().unwrap().to_string_lossy().into_owned();
                    if version == current_tag {
                        exists = true;
                        break;
                    }
                }

                if !exists {
                    writer.write_err(
                        format!("Can't found version: {}, package list:", current_tag).as_str(),
                    );
                    let list = ListCommand::create(self.conf.clone());
                    list.exec_match(writer, args)?;
                    writer.write_err(
                        format!(
                            "Use command datafuse-cli package fetch {} to retrieve this version",
                            current_tag
                        )
                        .as_str(),
                    );
                    return Ok(());
                }

                // Write to status.
                let mut status = Status::read(self.conf.clone())?;
                status.version = current_tag;
                status.write()?;

                writer.write_ok(format!("Package switch to {}", status.version).as_str());

                Ok(())
            }
            None => {
                println!("no command in switch");
                Ok(())
            }
        }
    }
}
