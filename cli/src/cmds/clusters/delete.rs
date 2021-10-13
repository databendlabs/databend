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

use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use serde_json;

use crate::cmds::clusters::cluster::ClusterProfile;
use crate::cmds::status::LocalRuntime;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::cmds::Writer;
use crate::error::CliError;
use crate::error::Result;

#[derive(Clone)]
pub struct DeleteCommand {
    conf: Config,
}

impl DeleteCommand {
    pub fn create(conf: Config) -> Self {
        DeleteCommand { conf }
    }
    pub fn generate() -> App<'static> {
        App::new("delete")
            .setting(AppSettings::ColoredHelp)
            .setting(AppSettings::DisableVersionFlag)
            .about("Create a databend clusters based on profile")
            .arg(
                Arg::new("profile")
                    .long("profile")
                    .about("Profile to delete, support local and clusters")
                    .required(false)
                    .takes_value(true),
            )
            .arg(
                Arg::new("purge")
                    .long("purge")
                    .takes_value(false)
                    .about("Purge would delete both persist data and deploy instances"),
            )
    }

    fn local_exec_match(&self, writer: &mut Writer, _args: &ArgMatches) -> Result<()> {
        let mut status = Status::read(self.conf.clone())?;
        for (fs, query) in status.get_local_query_configs() {
            if query.kill().is_err() {
                writer.write_err(&*format!(
                    "cannot kill query service with config in {}",
                    fs.clone()
                ))
            }

            //(TODO) check port freed
            if Status::delete_local_config(&mut status, "query".to_string(), fs.clone()).is_err() {
                writer.write_err(&*format!("cannot clean query config in {}", fs.clone()))
            }
        }
        if status.get_local_meta_config().is_some() {
            let (fs, meta) = status.get_local_meta_config().unwrap();
            meta.kill()
                .expect(&*format!("cannot kill meta service with config in {}", fs));
            Status::delete_local_config(&mut status, "meta".to_string(), fs)
                .expect("cannot clean meta config");
        }
        status.current_profile = None;
        status.write()?;
        writer.write_ok("🚀 stopped services");

        //(TODO) purge semantics
        Ok(())
    }

    fn get_profile(&self, profile: Option<&str>) -> Result<ClusterProfile> {
        match profile {
            Some("local") => Ok(ClusterProfile::Local),
            Some("cluster") => Ok(ClusterProfile::Cluster),
            None => {
                let status = Status::read(self.conf.clone())?;
                if status.current_profile.is_none() {
                    return Err(CliError::Unknown(
                        "Currently there is no profile in use, please create or use a profile"
                            .parse()
                            .unwrap(),
                    ));
                }
                Ok(serde_json::from_str::<ClusterProfile>(
                    &*status.current_profile.unwrap(),
                )?)
            }
            _ => Err(CliError::Unknown(
                "Currently there is no profile in use, please create or use a profile"
                    .parse()
                    .unwrap(),
            )),
        }
    }
    pub fn exec_match(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => {
                let p = self.get_profile(matches.value_of("profile"));
                match p {
                    Ok(ClusterProfile::Local) => return self.local_exec_match(writer, matches),
                    Ok(ClusterProfile::Cluster) => {
                        todo!()
                    }
                    Err(e) => writer.write_err(format!("cannot parse profile, {:?}", e).as_str()),
                }
            }
            None => {
                writer.write_err(&*"cannot find matches for cluster delete");
            }
        }

        Ok(())
    }
}
