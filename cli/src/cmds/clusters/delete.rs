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

use crate::cmds::clusters::cluster::ClusterProfile;
use crate::cmds::clusters::utils;
use crate::cmds::status::LocalRuntime;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::cmds::Writer;
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
            .setting(AppSettings::DisableVersionFlag)
            .about("Delete a databend cluster (delete current cluster by default) ")
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

    pub fn stop_current_local_services(status: &mut Status, writer: &mut Writer) -> Result<()> {
        writer.write_ok("⚠ start to clean up local services");
        for (fs, query) in status.get_local_query_configs() {
            if query.kill().is_err() {
                if Status::delete_local_config(status, "query".to_string(), fs.clone()).is_err() {
                    writer.write_err(&*format!("cannot clean query config in {}", fs.clone()))
                }
                writer.write_err(&*format!(
                    "cannot kill query service with config in {}",
                    fs.clone()
                ))
            }

            if Status::delete_local_config(status, "query".to_string(), fs.clone()).is_err() {
                writer.write_err(&*format!("cannot clean query config in {}", fs.clone()))
            }
            writer.write_ok(format!("⚠️ stopped query service with config in {}", fs).as_str());
        }
        if status.get_local_meta_config().is_some() {
            let (fs, meta) = status.get_local_meta_config().unwrap();
            if meta.kill().is_err() {
                writer.write_err(&*format!("cannot kill meta service with config in {}", fs));
                if Status::delete_local_config(status, "meta".to_string(), fs.clone()).is_err() {
                    writer.write_err(&*format!("cannot clean meta config in {}", fs))
                }
            }
            Status::delete_local_config(status, "meta".to_string(), fs.clone())
                .expect("cannot clean meta config");
            writer.write_ok(format!("⚠️ stopped meta service with config in {}", fs).as_str());
        }
        Ok(())
    }

    fn local_exec_match(&self, writer: &mut Writer, _args: &ArgMatches) -> Result<()> {
        let mut status = Status::read(self.conf.clone())?;
        DeleteCommand::stop_current_local_services(&mut status, writer)?;
        status.current_profile = None;
        status.write()?;
        writer.write_ok("🚀 stopped services");

        //(TODO) purge semantics
        Ok(())
    }

    pub fn exec_match(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => {
                let status = Status::read(self.conf.clone())?;
                let p = utils::get_profile(status, matches.value_of("profile"));
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
