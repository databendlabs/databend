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

use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use comfy_table::Cell;
use comfy_table::Color;
use comfy_table::Table;

use crate::cmds::clusters::cluster::ClusterProfile;
use crate::cmds::clusters::utils;
use crate::cmds::status::LocalRuntime;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::cmds::Writer;
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

impl ViewCommand {
    pub fn create(conf: Config) -> Self {
        ViewCommand { conf }
    }
    pub fn generate() -> App<'static> {
        App::new("view")
            .setting(AppSettings::DisableVersionFlag)
            .about("View health status of current profile")
            .arg(
                Arg::new("profile")
                    .long("profile")
                    .about("Profile to view, support local and clusters")
                    .required(false)
                    .takes_value(true),
            )
    }

    fn local_exec_match(&self, writer: &mut Writer, _args: &ArgMatches) -> Result<()> {
        let status = Status::read(self.conf.clone())?;
        let table = ViewCommand::build_local_table(&status);
        if let Ok(t) = table {
            writer.writeln(&t.trim_fmt());
        } else {
            writer.write_err(
                format!(
                    "cannot retrieve view table, error: {:?}",
                    table.unwrap_err()
                )
                .as_str(),
            );
        }
        Ok(())
    }

    fn build_row<T>(fs: String, config: T) -> Vec<Cell>
    where T: LocalRuntime {
        let file = Path::new(fs.as_str());
        let mut row = vec![];
        row.push(Cell::new(
            file.file_stem()
                .unwrap_or_else(|| panic!("cannot stem file {:?}", file))
                .to_string_lossy(),
        ));
        row.push(Cell::new("local"));
        row.push(config.verify().map_or(
            Cell::new(format!("{}", HealthStatus::UnReady).as_str()).fg(Color::Red),
            |_| Cell::new(format!("{}", HealthStatus::Ready).as_str()).fg(Color::Green),
        ));
        row.push(Cell::new("disabled"));
        row.push(Cell::new(fs.to_string()));
        row
    }

    pub fn build_local_table(status: &Status) -> Result<Table> {
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
        let meta_config = status.get_local_meta_config();
        if let Some((fs, meta_config)) = meta_config {
            let row = ViewCommand::build_row(fs, meta_config);
            table.add_row(row);
        }
        for (fs, query_config) in status.get_local_query_configs() {
            let row = ViewCommand::build_row(fs, query_config);
            table.add_row(row);
        }
        Ok(table)
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
                writer.write_err(&*"cannot find available profile to view");
            }
        }
        Ok(())
    }
}
