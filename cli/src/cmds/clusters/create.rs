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
use std::path::Path;

use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use metasrv::configs::Config as MetaConfig;

use crate::cmds::clusters::cluster::ClusterProfile;
use crate::cmds::status::LocalConfig;
use crate::cmds::status::LocalMetaConfig;
use crate::cmds::status::LocalRuntime;
use crate::cmds::Config;
use crate::cmds::FetchCommand;
use crate::cmds::Status;
use crate::cmds::SwitchCommand;
use crate::cmds::Writer;
use crate::error::CliError;
use crate::error::Result;

#[derive(Clone)]
pub struct CreateCommand {
    conf: Config,
}
struct LocalBinaryPaths {
    query: String,
    meta: String,
}
impl CreateCommand {
    pub fn create(conf: Config) -> Self {
        CreateCommand { conf }
    }
    pub fn generate() -> App<'static> {
        App::new("create")
            .setting(AppSettings::ColoredHelp)
            .setting(AppSettings::DisableVersionFlag)
            .about("Create a databend clusters based on profile")
            .arg(
                Arg::new("profile")
                    .long("profile")
                    .about("Profile for deployment, support local and clusters")
                    .required(true)
                    .possible_values(&["local"]),
            )
            .arg(
                Arg::new("meta_address")
                    .long("meta-address")
                    .about("Set endpoint to provide metastore service")
                    .env(databend_query::configs::config_meta::META_ADDRESS),
            )
            .arg(
                Arg::new("meta_username")
                    .long("meta-username")
                    .about("Set user name for metastore service authentication")
                    .env(databend_query::configs::config_meta::META_USERNAME)
                    .default_value("root"),
            )
            .arg(
                Arg::new("meta_password")
                    .long("meta-password")
                    .about("Set password for metastore service authentication")
                    .env(databend_query::configs::config_meta::META_PASSWORD)
                    .default_value("root"),
            )
            .arg(
                Arg::new("log_level")
                    .long("log-level")
                    .about("Set logging level")
                    .env(databend_query::configs::config_log::LOG_LEVEL)
                    .default_value("INFO"),
            )
            .arg(
                Arg::new("version")
                    .long("version")
                    .about("Set databend version to run")
                    .default_value("latest"),
            )
    }

    fn binary_path(&self, dir: String, version: String, name: String) -> Result<String> {
        if version.is_empty() {
            return Err(CliError::Unknown(
                "cannot find binary path, current version is empty".to_string(),
            ));
        }
        let bin_path = format!("{}/{}/{}", dir, version, name);
        if !Path::new(bin_path.as_str()).exists() {
            return Err(CliError::Unknown(format!(
                "cannot find binary path in {}",
                bin_path
            )));
        }
        Ok(fs::canonicalize(&bin_path)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string())
    }

    fn ensure_bin(&self, writer: &mut Writer, args: &ArgMatches) -> Result<LocalBinaryPaths> {
        let status = Status::read(self.conf.clone())?;

        let mut paths = LocalBinaryPaths {
            query: "".to_string(),
            meta: "".to_string(),
        };
        if self
            .binary_path(
                format!("{}/bin", self.conf.databend_dir),
                status.version,
                "databend-query".to_string(),
            )
            .is_err()
        {
            // fetch latest version of databend binary if version not found
            writer.write_ok(&*format!(
                "Cannot find databend binary path in version {}, start to download",
                args.value_of("version").unwrap()
            ));
            FetchCommand::create(self.conf.clone()).exec_match(writer, Some(args))?;
        }
        SwitchCommand::create(self.conf.clone()).exec_match(writer, Some(args))?;
        let status = Status::read(self.conf.clone())?;
        paths.query = self
            .binary_path(
                format!("{}/bin", self.conf.databend_dir),
                status.version.clone(),
                "databend-query".to_string(),
            )
            .expect("cannot find query bin");
        paths.meta = self
            .binary_path(
                format!("{}/bin", self.conf.databend_dir),
                status.version,
                "databend-meta".to_string(),
            )
            .expect("cannot find store bin");
        Ok(paths)
    }

    fn generate_meta_config(&self) -> MetaConfig {
        let mut config = MetaConfig::empty();
        if !portpicker::is_free(config.metric_api_address.parse().unwrap()) {
            config.metric_api_address = Status::find_unused_local_port()
        }

        if !portpicker::is_free(config.admin_api_address.parse().unwrap()) {
            config.admin_api_address = Status::find_unused_local_port()
        }
        if !portpicker::is_free(config.flight_api_address.parse().unwrap()) {
            config.flight_api_address = Status::find_unused_local_port()
        }
        config
    }
    fn generate_local_meta_config(
        &self,
        args: &ArgMatches,
        bin_path: LocalBinaryPaths,
    ) -> Option<LocalMetaConfig> {
        let mut config = self.generate_meta_config();

        if args.value_of("meta_address").is_some()
            && args.value_of("meta_address").unwrap().is_empty()
        {
            config.flight_api_address = args.value_of("meta_address").unwrap().to_string();
        }
        config.log_level = args.value_of("log_level").unwrap().to_string();
        let log_base = format!("{}/logs", self.conf.clone().databend_dir);
        if !Path::new(log_base.as_str()).exists() {
            fs::create_dir(Path::new(log_base.as_str()))
                .unwrap_or_else(|_| panic!("cannot create directory {}", log_base));
        }
        let meta_log_dir = format!("{}/local_meta_log", log_base);
        if !Path::new(meta_log_dir.as_str()).exists() {
            fs::create_dir(Path::new(meta_log_dir.as_str()))
                .unwrap_or_else(|_| panic!("cannot create directory {}", meta_log_dir));
        }
        config.log_dir = meta_log_dir;
        config.raft_config.single = true;
        Some(LocalMetaConfig {
            config,
            pid: None,
            path: Some(bin_path.meta),
            log_dir: Some(log_base),
        })
    }

    fn provision_local_meta_service(
        &self,
        writer: &mut Writer,
        args: &ArgMatches,
        bin_path: LocalBinaryPaths,
    ) -> Result<()> {
        let mut meta_config = self
            .generate_local_meta_config(args, bin_path)
            .expect("cannot generate metaservice config");
        match meta_config.start() {
            Ok(_) => {
                assert!(meta_config.get_pid().is_some());
                let mut status = Status::read(self.conf.clone())?;
                status.local_configs.meta_configs = Some(meta_config.clone());
                status.write()?;
                writer.write_ok(
                    format!(
                        "👏 successfully started meta service with rpc endpoint {}",
                        meta_config.config.flight_api_address
                    )
                    .as_str(),
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn local_exec_match(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()> {
        match self.local_exec_precheck(args) {
            Ok(_) => {
                writer.write_ok("databend clusters precheck passed!");
                // ensuring needed dependencies
                let bin_path = self
                    .ensure_bin(writer, args)
                    .expect("cannot find binary path");

                {
                    let res = self.provision_local_meta_service(writer, args, bin_path);
                    writer.write_err(&*format!(
                        "❌ Cannot provison meta service, error: {:?}",
                        res.unwrap_err()
                    ));
                }
                Ok(())
            }
            Err(CliError::Unknown(s)) => {
                writer.write_err(s.as_str());
                Ok(())
            }
            Err(e) => {
                log::error!("{:?}", e);
                Ok(())
            }
        }
    }

    /// precheck whether current local profile applicable for local host machine
    fn local_exec_precheck(&self, args: &ArgMatches) -> Result<()> {
        let status = Status::read(self.conf.clone())?;
        if status.local_configs != LocalConfig::empty() {
            return Err(CliError::Unknown(format!(
                "❗ found previously existed clusters with config in {}",
                status.path
            )));
        }
        if args.value_of("meta_address").is_some()
            && !args.value_of("meta_address").unwrap().is_empty()
            && !portpicker::is_free(args.value_of("meta_address").unwrap().parse().unwrap())
        {
            return Err(CliError::Unknown(format!(
                "clickhouse handler port {} is occupied by other program",
                args.value_of("clickhouse_handler_port").unwrap()
            )));
        }

        Ok(())
    }

    pub fn exec_match(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => {
                let profile = matches.value_of_t("profile");
                match profile {
                    Ok(ClusterProfile::Local) => {
                        return self.local_exec_match(writer, matches);
                    }
                    Ok(ClusterProfile::Cluster) => {
                        todo!()
                    }
                    Err(_) => writer.write_err("currently profile only support clusters or local"),
                }
            }
            None => {
                println!("none ");
            }
        }

        Ok(())
    }
}
