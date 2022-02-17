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

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use common_base::tokio::time;
use http::HeaderMap;
// Lets us call into_async_read() to convert a futures::stream::Stream into a
// futures::io::AsyncRead.
use itertools::Itertools;
use lexical_util::num::AsPrimitive;
use num_format::Locale;
use num_format::ToFormattedString;

use crate::cmds::clusters::cluster::ClusterProfile;
use crate::cmds::command::Command;
use crate::cmds::queries::query::build_load_endpoint;
use crate::cmds::queries::query::build_query_endpoint;
use crate::cmds::queries::query::execute_load;
use crate::cmds::queries::query::execute_query_json;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::cmds::Writer;
use crate::error::CliError;
use crate::error::Result;
// Support different file format to be loaded
pub enum FileFormat {
    Csv,
}

impl FromStr for FileFormat {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<FileFormat, &'static str> {
        match s {
            "csv" => Ok(FileFormat::Csv),
            _ => Err("no match for profile"),
        }
    }
}

pub struct Schema {
    schema: BTreeMap<String, String>,
}

impl FromStr for Schema {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Schema, &'static str> {
        let mut str = String::from(s);
        let mut schema = Schema {
            schema: BTreeMap::new(),
        };
        str.retain(|e| e != ' ');
        for field in str.split(',') {
            let elems: Vec<&str> = field.split(':').filter(|e| !e.is_empty()).collect();
            if elems.len() != 2 {
                return Err(
                    "not a valid schema, please input schema in format like a:uint8,b:uint64",
                );
            }
            schema
                .schema
                .insert(elems[0].to_string(), elems[1].to_string());
        }
        Ok(schema)
    }
}

impl ToString for Schema {
    fn to_string(&self) -> String {
        return self
            .schema
            .iter()
            .map(|(a, b)| a.to_owned() + " " + &*b.to_owned())
            .join(",");
    }
}

#[derive(Clone)]
pub struct LoadCommand {
    conf: Config,
    clap: App<'static>,
}

impl LoadCommand {
    pub fn create(conf: Config) -> Self {
        let clap = LoadCommand::generate();
        LoadCommand { conf, clap }
    }
    pub fn default() -> Self {
        LoadCommand::create(Config::default())
    }

    pub fn generate() -> App<'static> {
        let app = App::new("load")
            .setting(AppSettings::DisableVersionFlag)
            .about("Query on databend cluster")
            .arg(
                Arg::new("profile")
                    .long("profile")
                    .help("Profile to run queries")
                    .required(false)
                    .possible_values(&["local"])
                    .default_value("local"),
            )
            .arg(
                Arg::new("format").long("format")
                    .help("the format of file, support csv")
                    .takes_value(true)
                    .required(false)
                    .default_value("csv"),
            )
            .arg(
                Arg::new("schema").long("schema")
                    .help("defined schema for table load, for example:\
                    bendctl load --schema a:uint8, b:uint64, c:String")
                    .takes_value(true)
                    .required(false),
            )
            .arg(
                Arg::new("load")
                    .help("file to get loaded for example foo.csv")
                    .takes_value(true)
                    .required(false),
            )
            .arg(
                Arg::new("skip_header_lines").long("skip-header-lines")
                    .help("state on whether CSV has dataset header for example: \
                    bendctl load test.csv --with_header true would ignore the first ten lines in csv file")
                    .default_value("1")
                    .required(false)
                    .takes_value(true),
            )
            .arg(
                Arg::new("table").long("table")
                .help("database table")
                .takes_value(true)
                .required(true),
            );

        app
    }
    async fn local_exec_match(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()> {
        match self.local_exec_precheck(args).await {
            Ok(_) => {
                let location = args.value_of("load");
                let table = args.value_of("table").unwrap();
                let skip_header = args.value_of("skip_header_lines").unwrap_or("1");
                let schema = args.value_of("schema");

                let table_with_schema = match schema {
                    Some(_) => {
                        let schema: Schema =
                            args.value_of_t("schema").expect("cannot build schema");
                        format!(
                            "{} ({})",
                            table,
                            schema.schema.keys().into_iter().join(", ")
                        )
                    }
                    None => table.to_string(),
                };
                self.load(location, table_with_schema.as_str(), skip_header, writer)
                    .await
            }
            Err(e) => {
                writer.write_err(format!("Query command precheck failed, error {e:?}"));
                Ok(())
            }
        }
    }

    pub async fn load(
        &self,
        location: Option<&str>,
        table_with_schema: &str,
        skip_header: &str,
        writer: &mut Writer,
    ) -> Result<()> {
        let mut reader = build_reader(location).await;
        let mut data = vec![];
        reader.read_to_end(&mut data)?;

        let start = time::Instant::now();
        let status = Status::read(self.conf.clone())?;
        let (cli, url) = build_load_endpoint(&status)?;
        let mut headers = HeaderMap::new();
        headers.insert(
            "insert_sql",
            format!("INSERT INTO {table_with_schema} format CSV")
                .parse()
                .unwrap(),
        );
        headers.insert("csv_header", skip_header.to_string().parse().unwrap());
        let progress = execute_load(&cli, &url, headers, data).await?;

        let elapsed = start.elapsed();
        let time = elapsed.as_millis() as f64 / 1000f64;
        writer.write_ok(format!(
            "successfully loaded {} rows, rows/src: {} (rows/sec). time: {} sec",
            progress.read_rows.to_formatted_string(&Locale::en),
            (progress.read_rows as f64 / time)
                .as_u128()
                .to_formatted_string(&Locale::en),
            time
        ));

        Ok(())
    }

    /// precheck would at build up and validate schema for incoming INSERT operations
    async fn local_exec_precheck(&self, args: &ArgMatches) -> Result<()> {
        let status = Status::read(self.conf.clone())?;
        if status.current_profile.is_none() {
            return Err(CliError::Unknown(format!(
                "Query command error: cannot find local configs in {}, please run `bendctl cluster create` to create a new local cluster or '\\admin' switch to the admin mode",
                status.local_config_dir
            )));
        }
        let status = Status::read(self.conf.clone())?;
        // TODO typecheck
        if args.value_of("schema").is_none() {
            if let Err(e) = table_exists(&status, args.value_of("table")).await {
                return Err(e);
            }
            Ok(())
        } else {
            match args.value_of_t::<Schema>("schema") {
                Ok(schema) => {
                    return create_table_if_not_exists(&status, args.value_of("table"), schema)
                        .await
                }
                Err(e) => {
                    return Err(CliError::Unknown(format!(
                        "{} schema is not in valid format, error: {:?}",
                        args.value_of("table").unwrap(),
                        e
                    )))
                }
            }
        }
    }
}

pub async fn build_reader(load: Option<&str>) -> Box<dyn std::io::Read + Send + Sync> {
    match load {
        Some(val) => {
            if Path::new(val).exists() {
                let f = std::fs::File::open(val).expect("cannot open file: permission denied");
                Box::new(f)
            } else if val.contains("://") {
                let target = reqwest::get(val)
                    .await
                    .expect("cannot connect to target url")
                    .error_for_status()
                    .expect("return code is not OK")
                    .text()
                    .await
                    .expect("cannot fetch for target"); // generate an error if server didn't respond
                Box::new(Box::new(Cursor::new(target)))
            } else {
                Box::new(Cursor::new(val.to_string()))
            }
        }
        None => {
            let io = std::io::stdin();
            Box::new(io)
        }
    }
}

async fn table_exists(status: &Status, table: Option<&str>) -> Result<()> {
    match table {
        Some(t) => {
            let (cli, url) = build_query_endpoint(status)?;
            let query = format!("SHOW TABLES LIKE '{t}';");
            let (col, data, _) = execute_query_json(&cli, &url, query).await?;
            if col.is_none() || data.is_empty() {
                return Err(CliError::Unknown(format!("table {t} not found")));
            }
        }
        None => return Err(CliError::Unknown("no table found in argument".to_string())),
    }
    Ok(())
}

async fn create_table_if_not_exists(
    status: &Status,
    table: Option<&str>,
    schema: Schema,
) -> Result<()> {
    return match table_exists(status, table).await {
        Ok(_) => Ok(()),
        Err(_) => {
            let (cli, url) = build_query_endpoint(status)?;
            let query = format!(
                "CREATE TABLE {}({}) Engine = Fuse;",
                table.unwrap(),
                schema.to_string()
            );
            execute_query_json(&cli, &url, query).await?;
            Ok(())
        }
    };
}

#[async_trait]
impl Command for LoadCommand {
    fn name(&self) -> &str {
        "load"
    }

    fn clap(&self) -> App<'static> {
        self.clap.clone()
    }

    fn about(&self) -> &'static str {
        "Query on databend cluster"
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
                println!("none ");
            }
        }
        Ok(())
    }
}
