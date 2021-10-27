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

use std::borrow::Borrow;
use std::io::Read;
use std::net::SocketAddr;
use std::path::Path;

use async_trait::async_trait;
use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use comfy_table::Cell;
use comfy_table::Color;
use comfy_table::Table;
use common_base::ProgressValues;
use lexical_util::num::AsPrimitive;
use num_format::Locale;
use num_format::ToFormattedString;
use serde_json::json;
use crate::cmds::clusters::cluster::ClusterProfile;
use crate::cmds::command::Command;
use crate::cmds::{Config, ClusterCommand};
use crate::cmds::Status;
use crate::cmds::Writer;
use crate::error::CliError;
use crate::error::Result;
use std::str::FromStr;
use crate::cmds::packages::fetch::{download, unpack};
use crate::cmds::clusters::delete::DeleteCommand;
use crate::cmds::queries::query::QueryCommand;
use databend_query::common::HashMap;

const ONTIME_DOWNLOAD_URL : &str ="https://repo.databend.rs/dataset/ontime_mini.tar.gz";
const ONTIME_DDL_TEMPLATE : &str = r#"
CREATE TABLE ontime
(
    Year                            UInt16,
    Quarter                         UInt8,
    Month                           UInt8,
    DayofMonth                      UInt8,
    DayOfWeek                       UInt8,
    FlightDate                      Date,
    Reporting_Airline               String,
    DOT_ID_Reporting_Airline        Int32,
    IATA_CODE_Reporting_Airline     String,
    Tail_Number                     String,
    Flight_Number_Reporting_Airline String,
    OriginAirportID                 Int32,
    OriginAirportSeqID              Int32,
    OriginCityMarketID              Int32,
    Origin                          String,
    OriginCityName                  String,
    OriginState                     String,
    OriginStateFips                 String,
    OriginStateName                 String,
    OriginWac                       Int32,
    DestAirportID                   Int32,
    DestAirportSeqID                Int32,
    DestCityMarketID                Int32,
    Dest                            String,
    DestCityName                    String,
    DestState                       String,
    DestStateFips                   String,
    DestStateName                   String,
    DestWac                         Int32,
    CRSDepTime                      Int32,
    DepTime                         Int32,
    DepDelay                        Int32,
    DepDelayMinutes                 Int32,
    DepDel15                        Int32,
    DepartureDelayGroups            String,
    DepTimeBlk                      String,
    TaxiOut                         Int32,
    WheelsOff                       Int32,
    WheelsOn                        Int32,
    TaxiIn                          Int32,
    CRSArrTime                      Int32,
    ArrTime                         Int32,
    ArrDelay                        Int32,
    ArrDelayMinutes                 Int32,
    ArrDel15                        Int32,
    ArrivalDelayGroups              Int32,
    ArrTimeBlk                      String,
    Cancelled                       UInt8,
    CancellationCode                String,
    Diverted                        UInt8,
    CRSElapsedTime                  Int32,
    ActualElapsedTime               Int32,
    AirTime                         Int32,
    Flights                         Int32,
    Distance                        Int32,
    DistanceGroup                   UInt8,
    CarrierDelay                    Int32,
    WeatherDelay                    Int32,
    NASDelay                        Int32,
    SecurityDelay                   Int32,
    LateAircraftDelay               Int32,
    FirstDepTime                    String,
    TotalAddGTime                   String,
    LongestAddGTime                 String,
    DivAirportLandings              String,
    DivReachedDest                  String,
    DivActualElapsedTime            String,
    DivArrDelay                     String,
    DivDistance                     String,
    Div1Airport                     String,
    Div1AirportID                   Int32,
    Div1AirportSeqID                Int32,
    Div1WheelsOn                    String,
    Div1TotalGTime                  String,
    Div1LongestGTime                String,
    Div1WheelsOff                   String,
    Div1TailNum                     String,
    Div2Airport                     String,
    Div2AirportID                   Int32,
    Div2AirportSeqID                Int32,
    Div2WheelsOn                    String,
    Div2TotalGTime                  String,
    Div2LongestGTime                String,
    Div2WheelsOff                   String,
    Div2TailNum                     String,
    Div3Airport                     String,
    Div3AirportID                   Int32,
    Div3AirportSeqID                Int32,
    Div3WheelsOn                    String,
    Div3TotalGTime                  String,
    Div3LongestGTime                String,
    Div3WheelsOff                   String,
    Div3TailNum                     String,
    Div4Airport                     String,
    Div4AirportID                   Int32,
    Div4AirportSeqID                Int32,
    Div4WheelsOn                    String,
    Div4TotalGTime                  String,
    Div4LongestGTime                String,
    Div4WheelsOff                   String,
    Div4TailNum                     String,
    Div5Airport                     String,
    Div5AirportID                   Int32,
    Div5AirportSeqID                Int32,
    Div5WheelsOn                    String,
    Div5TotalGTime                  String,
    Div5LongestGTime                String,
    Div5WheelsOff                   String,
    Div5TailNum                     String
) ENGINE = CSV location = "{{ csv_location }}";
"#;

#[derive(Clone)]
pub struct UpCommand {
    #[allow(dead_code)]
    conf: Config,
    clap: App<'static>,
}

// Support to load datasets from official resource
pub enum DataSets {
    OntimeMini(&'static str, &'static str),
}

// Implement the trait
impl FromStr for DataSets {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<DataSets, &'static str> {
        match s {
            "ontime_mini" => Ok(DataSets::OntimeMini(ONTIME_DOWNLOAD_URL, ONTIME_DDL_TEMPLATE)),
            _ => Err("no match for profile"),
        }
    }
}

fn download_and_unpack(url: String, download_file_name: String, target_file: String) -> Result<()> {

}

fn render(ddl: &str, template: serde_json::Value)  -> Result<String> {
    let mut reg = handlebars::Handlebars::new();
    // render without register
    match reg.render_template(ddl, &template) {
        Ok(str) => {
            return Ok(str)
        }
        Err(e) => {
            return Err(CliError::Unknown(format!("cannot render DDL, error: {}", e)))
        }
    }

}


impl UpCommand {
    pub fn create(conf: Config) -> Self {
        let clap = UpCommand::generate();
        UpCommand { conf, clap }
    }
    pub fn generate() -> App<'static> {
        let app = App::new("up")
            .setting(AppSettings::DisableVersionFlag)
            .about("Set up a cluster and load prepared dataset for demo")
            .arg(
                Arg::new("profile")
                    .long("profile")
                    .about("Profile to run queries")
                    .required(false)
                    .possible_values(&["local"])
                    .default_value("local"),
            )
            .arg(
                Arg::new("dataset")
                    .about("Prepared datasets")
                    .takes_value(true)
                    .possible_values(&["ontime_mini"])
                    .default_value("ontime_mini")
                    .required(false),
            );
        app
    }

    pub(crate) async fn exec_match(
        &self,
        writer: &mut Writer,
        args: Option<&ArgMatches>,
    ) -> Result<()> {
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
                    Err(_) => writer.write_err("currently profile only support cluster or local"),
                }
            }
            None => {
                println!("none ");
            }
        }
        Ok(())
    }

    async fn local_exec_match(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()> {
        match self.local_exec_precheck(args) {
            Ok(_) => {
                let dataset = args.value_of_t("dataset");
                match dataset {
                    Ok(DataSets::OntimeMini(url, ddl)) => {
                        writer.write_ok("Welcome to use our databend product 🎉🎉🎉");
                        let cluster = ClusterCommand::create(self.conf.clone());
                        if let Err(e) = cluster.exec(writer, ["cluster", "create", "--force"].join(" ")).await {
                            writer.write_err(&*format!("Cannot bootstrap local cluster, error {:?}", e));
                            let mut status = Status::read(self.conf.clone())?;
                            DeleteCommand::stop_current_local_services(&mut status, writer).await?;
                            return Ok(())
                        }
                        let status = Status::read(self.conf.clone())?;
                        let cfgs = status.get_local_query_configs();
                        let (_, query_config) = cfgs.get(0).expect("cannot get local query config");
                        let dataset_dir = query_config.config.storage.disk.data_path.as_str();
                        let dataset_location = format!("{}/ontime_2019_2021.csv", dataset_dir);
                        if Path::new(dataset_location.as_str()).exists() {
                            writer.write_ok("Dataset ontime mini already exists")
                        } else {
                            let download_location = format!("{}/downloads/datasets/ontime_mini.tar.gz", self.conf.databend_dir);
                            std::fs::create_dir_all(Path::new(format!("{}/downloads/datasets/", self.conf.databend_dir).as_str()))?;
                            writer.write_ok(format!("start to download dataset on {}", download_location).as_str());
                            if let Err(e) = download(url, &*download_location) {
                                writer.write_err(&*format!("Cannot download target dataset, error {:?}", e));
                                let mut status = Status::read(self.conf.clone())?;
                                DeleteCommand::stop_current_local_services(&mut status, writer).await?;
                            }
                            writer.write_ok("start to unpack dataset");
                            if let Err(e) = unpack(download_location.as_str(), dataset_dir) {
                                writer.write_err(&*format!("Cannot unpack target dataset, error {:?}", e));
                                let mut status = Status::read(self.conf.clone())?;
                                DeleteCommand::stop_current_local_services(&mut status, writer).await?;
                            }
                            if !Path::new(dataset_location.as_str()).exists() {
                                writer.write_err(&*format!("Cannot find dataset on {}",Path::new(dataset_location.as_str()).canonicalize().unwrap().to_str().unwrap()));
                            }
                        }
                        let query = QueryCommand::create(self.conf.clone());
                        let ddl = render(ddl, json!({"csv_location": Path::new(dataset_location.as_str()).canonicalize().unwrap().to_str().unwrap()}));
                        match ddl {
                            Ok(ddl) => {
                                if let Err(_) = query.exec(writer, "DROP DATABASE IF EXISTS ontime;".to_string()).await {}
                                if let Err(e) = query.exec(writer, ddl).await {
                                    writer.write_err(&*format!("Cannot create table, error {:?}", e));
                                }
                            }
                            Err(e) => {
                                writer.write_err(&*format!("{:?}", e));
                                let mut status = Status::read(self.conf.clone())?;
                                DeleteCommand::stop_current_local_services(&mut status, writer).await?;
                            }
                        }

                    }
                    Err(e) => {
                        writer.write_err(&*format!("Cannot find public dataset, error {:?}", e));
                    }
                }
               Ok(())
            }
            Err(e) => {
                writer.write_err(&*format!("Query command precheck failed, error {:?}", e));
                Ok(())
            }
        }
    }

    /// precheck whether current local profile applicable for local host machine
    fn local_exec_precheck(&self, _args: &ArgMatches) -> Result<()> {

        Ok(())
    }
}
#[async_trait]
impl Command for UpCommand {
    fn name(&self) -> &str {
        "query"
    }

    fn about(&self) -> &str {
        "Query on databend cluster"
    }

    fn is(&self, s: &str) -> bool {
        s.contains(self.name())
    }

    async fn exec(&self, writer: &mut Writer, args: String) -> Result<()> {
        match self
            .clap
            .clone()
            .try_get_matches_from(vec!["query", args.as_str()])
        {
            Ok(matches) => {
                return self.exec_match(writer, Some(matches.borrow())).await;
            }
            Err(err) => {
                println!("Cannot get subcommand matches: {}", err);
            }
        }

        Ok(())
    }
}