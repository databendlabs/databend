// Copyright 2021 Datafuse Labs.
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

use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;

use crate::cmds::clusters::cluster::ClusterProfile;
use crate::cmds::clusters::stop::StopCommand;
use crate::cmds::loads::load::LoadCommand;
use crate::cmds::queries::query::QueryCommand;
use crate::cmds::ClusterCommand;
use crate::cmds::Command;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::cmds::Writer;
use crate::error::CliError;
use crate::error::Result;

#[derive(Clone)]
pub struct GenerateCommand {
    conf: Config,
}

// Support to load datasets from official resource
#[derive(Clone)]
pub enum DataSets {
    Ontime(&'static str, &'static str),
}

// Implement the trait
impl FromStr for DataSets {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<DataSets, &'static str> {
        match s {
            "ontime" => Ok(DataSets::Ontime(
                "https://repo.databend.rs/dataset/stateful/ontime.csv",
                include_str!("./ddls/ontime.sql"),
            )),
            _ => Err("no match for profile"),
        }
    }
}

impl GenerateCommand {
    pub fn create(conf: Config) -> Self {
        GenerateCommand { conf }
    }

    pub fn default() -> Self {
        GenerateCommand::create(Config::default())
    }

    async fn local_up(&self, dataset: DataSets, writer: &mut Writer) -> Result<()> {
        // bootstrap cluster
        writer.write_ok("Welcome to use our databend product 🎉🎉🎉".to_string());
        let cluster = ClusterCommand::create(self.conf.clone());
        if let Err(e) = cluster
            .exec(writer, ["cluster", "create", "--force"].join(" "))
            .await
        {
            return Err(CliError::Unknown(format!(
                "Cannot bootstrap local cluster, error {:?}",
                e
            )));
        }

        match dataset {
            DataSets::Ontime(data, ddl) => {
                let table = "ontime".to_string();

                writer.write_ok(format!("Creating table `{}`", table));
                if let Err(e) = self.create_ddl(writer, table.as_str(), ddl).await {
                    return Err(e);
                }

                writer.write_ok(format!("Loading data into table `{}`", table));
                self.load_data(writer, table.as_str(), data).await?;
            }
        }

        Ok(())
    }

    async fn create_ddl(&self, writer: &mut Writer, table_name: &str, ddl: &str) -> Result<()> {
        let query = QueryCommand::create(self.conf.clone());
        if let Err(e) = query
            .exec(writer, format!(r#"DROP TABLE IF EXISTS {}"#, table_name))
            .await
        {
            return Err(e);
        }
        if let Err(e) = query.exec(writer, ddl.to_string()).await {
            return Err(e);
        }

        Ok(())
    }

    async fn load_data(&self, writer: &mut Writer, table_name: &str, data: &str) -> Result<()> {
        let loader = LoadCommand::create(self.conf.clone());
        loader.load(Some(data), table_name, "1", writer).await?;

        Ok(())
    }

    async fn local_exec_match(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()> {
        match self.local_exec_precheck(args).await {
            Ok(_) => {
                let dataset = args.value_of_t("dataset");
                match dataset {
                    Ok(d) => {
                        if let Err(e) = self.local_up(d, writer).await {
                            writer.write_err(format!("{:?}", e));
                            let mut status = Status::read(self.conf.clone())?;
                            if let Err(e) =
                                StopCommand::stop_current_local_services(&mut status, writer).await
                            {
                                writer.write_err(format!("{:?}", e));
                            }
                        }
                    }
                    Err(e) => {
                        writer.write_err(format!("Cannot find public dataset, error {:?}", e));
                    }
                }
                Ok(())
            }
            Err(e) => {
                writer.write_err(format!("Generate command precheck failed, error {:?}", e));
                Ok(())
            }
        }
    }

    /// precheck whether current local profile applicable for local host machine
    async fn local_exec_precheck(&self, _args: &ArgMatches) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Command for GenerateCommand {
    fn name(&self) -> &str {
        "generate"
    }

    fn clap(&self) -> App<'static> {
        App::new(self.name())
            .setting(AppSettings::DisableVersionFlag)
            .about(self.about())
            .arg(
                Arg::new("profile")
                    .long("profile")
                    .help("Profile to run queries")
                    .required(false)
                    .possible_values(&["local"])
                    .default_value("local"),
            )
            .arg(
                Arg::new("dataset")
                    .long("dataset")
                    .help("Dataset for generate")
                    .takes_value(true)
                    .possible_values(&["ontime"])
                    .default_value("ontime")
                    .required(false),
            )
    }

    fn about(&self) -> &'static str {
        "Generate example datasets"
    }

    fn is(&self, s: &str) -> bool {
        s.contains(self.name())
    }

    fn subcommands(&self) -> Vec<Arc<dyn Command>> {
        vec![]
    }

    async fn exec_matches(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => {
                let profile = matches.value_of_t("profile");
                match profile {
                    Ok(ClusterProfile::Local) => {
                        return self.local_exec_match(writer, matches).await;
                    }
                    Ok(ClusterProfile::Cluster) => {
                        todo!()
                    }
                    Err(_) => writer
                        .write_err("Currently profile only support cluster or local".to_string()),
                }
            }
            None => {
                println!("none");
            }
        }

        Ok(())
    }
}
