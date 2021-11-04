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

use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use comfy_table::Cell;
use comfy_table::Color;
use comfy_table::Table;
use sysinfo::System;
use sysinfo::SystemExt;

use crate::cmds::clusters::cluster::ClusterProfile;
use crate::cmds::clusters::utils;
use crate::cmds::command::Command;
use crate::cmds::status::LocalRuntime;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::cmds::Writer;
use crate::error::CliError;
use crate::error::Result;

#[derive(Clone)]
pub struct ViewCommand {
    conf: Config,
}

pub enum HealthStatus {
    Ready,
    UnReady,
}

impl fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            HealthStatus::Ready => write!(f, "✅ ready"),
            HealthStatus::UnReady => write!(f, "❌ unReady"),
        }
    }
}

// poll_health would check the health of active meta service and query service, and return their status quo
// first element is the health status in dashboard service and the second element is the health status of meta services
// the third element is the health status in query services
pub async fn poll_health(
    profile: &ClusterProfile,
    status: &Status,
    retry: Option<u32>,
    duration: Option<Duration>,
) -> (
    Option<(String, Result<()>)>,
    Option<(String, Result<()>)>,
    (Vec<String>, Vec<Result<()>>),
) {
    match profile {
        ClusterProfile::Local => {
            let meta_config = status.get_local_meta_config();
            let query_config = status.get_local_query_configs();
            let dashboard_config = status.get_local_dashboard_config();
            let mut handles = Vec::with_capacity(query_config.len() + 2);
            let mut fs_vec = Vec::with_capacity(query_config.len() + 2);
            for (fs, dashboard_config) in dashboard_config.iter() {
                fs_vec.push(fs.clone());
                handles.push(dashboard_config.verify(retry, duration));
            }
            for (fs, meta_config) in meta_config.iter() {
                fs_vec.push(fs.clone());
                handles.push(meta_config.verify(retry, duration));
            }
            for (fs, query_config) in query_config.iter() {
                fs_vec.push(fs.clone());
                handles.push(query_config.verify(retry, duration));
            }
            let mut res = futures::future::join_all(handles).await;
            let dash_res = dashboard_config.map(|_| (fs_vec.remove(0), res.remove(0)));
            let meta_res = meta_config.map(|_| (fs_vec.remove(0), res.remove(0)));
            (dash_res, meta_res, (fs_vec, res))
        }
        ClusterProfile::Cluster => {
            todo!()
        }
    }
}

impl ViewCommand {
    pub fn create(conf: Config) -> Self {
        ViewCommand { conf }
    }

    async fn local_exec_match(&self, writer: &mut Writer, _args: &ArgMatches) -> Result<()> {
        match self.local_exec_precheck().await {
            Ok(_) => {
                let status = Status::read(self.conf.clone())?;
                let table = ViewCommand::build_local_table(&status, None, None).await;
                if let Ok(t) = table {
                    writer.writeln(&t.trim_fmt());
                } else {
                    writer.write_err(format!(
                        "Cannot retrieve view table, error: {:?}",
                        table.unwrap_err()
                    ));
                }
            }
            Err(e) => {
                writer.write_err(format!("View precheck failed: {:?}", e));
            }
        }
        Ok(())
    }

    /// precheck whether on view configs
    async fn local_exec_precheck(&self) -> Result<()> {
        let status = Status::read(self.conf.clone())?;

        if !status.has_local_configs() {
            return Err(CliError::Unknown(format!(
                "❗ Does not have local config in {}",
                status.local_config_dir
            )));
        }
        let s = System::new_all();

        if s.process_by_name("databend-meta").is_empty() {
            return Err(CliError::Unknown(
                "❗ cannot find existing meta service on local machine"
                    .parse()
                    .unwrap(),
            ));
        }
        if s.process_by_name("databend-query").is_empty() {
            return Err(CliError::Unknown(
                "❗ cannot find existing query service on local machine"
                    .parse()
                    .unwrap(),
            ));
        }

        Ok(())
    }

    fn build_row(fs: &str, verify_result: &Result<()>) -> Vec<Cell> {
        let file = Path::new(fs);
        let mut row = vec![];
        row.push(Cell::new(
            file.file_stem()
                .unwrap_or_else(|| panic!("cannot stem file {:?}", file))
                .to_string_lossy(),
        ));
        row.push(Cell::new("local"));
        row.push(verify_result.as_ref().map_or(
            Cell::new(format!("{}", HealthStatus::UnReady).as_str()).fg(Color::Red),
            |_| Cell::new(format!("{}", HealthStatus::Ready).as_str()).fg(Color::Green),
        ));
        row.push(Cell::new("disabled"));
        row.push(Cell::new(fs.to_string()));
        row
    }

    pub async fn build_local_table(
        status: &Status,
        retry: Option<u32>,
        duration: Option<Duration>,
    ) -> Result<Table> {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");
        // Title.
        table.set_header(vec![
            Cell::new("Name"),
            Cell::new("Profile"),
            Cell::new("Health"),
            Cell::new("Tls"),
            Cell::new("Config"),
        ]);
        let (dashboard, meta, query) =
            poll_health(&ClusterProfile::Local, status, retry, duration).await;
        if let Some(meta) = meta {
            table.add_row(ViewCommand::build_row(&*meta.0, &meta.1));
        }
        for (i, fs) in query.0.iter().enumerate() {
            table.add_row(ViewCommand::build_row(fs, query.1.get(i).unwrap()));
        }
        if let Some(dashboard) = dashboard {
            table.add_row(ViewCommand::build_row(&*dashboard.0, &dashboard.1));
        }
        Ok(table)
    }
}

#[async_trait]
impl Command for ViewCommand {
    fn name(&self) -> &str {
        "view"
    }

    fn clap(&self) -> App<'static> {
        App::new("view")
            .setting(AppSettings::DisableVersionFlag)
            .about(self.about())
            .arg(
                Arg::new("profile")
                    .long("profile")
                    .about("Profile to view, support local and clusters")
                    .required(false)
                    .takes_value(true),
            )
    }

    fn subcommands(&self) -> Vec<Arc<dyn Command>> {
        vec![]
    }

    fn about(&self) -> &'static str {
        "View health status of current profile"
    }

    fn is(&self, s: &str) -> bool {
        s.contains(self.name())
    }

    async fn exec_matches(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => {
                let status = Status::read(self.conf.clone())?;
                let p = utils::get_profile(status, matches.value_of("profile"));
                match p {
                    Ok(ClusterProfile::Local) => {
                        return self.local_exec_match(writer, matches).await
                    }
                    Ok(ClusterProfile::Cluster) => {
                        todo!()
                    }
                    Err(e) => writer.write_err(format!("cannot parse profile, {:?}", e)),
                }
            }
            None => {
                writer.write_err("cannot find available profile to view".to_string());
            }
        }
        Ok(())
    }
}
