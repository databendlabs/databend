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

use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use serde_json::json;

use crate::cmds::clusters::cluster::ClusterProfile;
use crate::cmds::clusters::stop::StopCommand;
use crate::cmds::command::Command;
use crate::cmds::packages::fetch::download_and_unpack;
use crate::cmds::packages::fetch::get_rust_architecture;
use crate::cmds::queries::query::QueryCommand;
use crate::cmds::status::LocalDashboardConfig;
use crate::cmds::status::LocalRuntime;
use crate::cmds::ClusterCommand;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::cmds::Writer;
use crate::error::CliError;
use crate::error::Result;

const ONTIME_DOWNLOAD_URL: &str = "https://repo.databend.rs/dataset/ontime_mini.tar.gz";
const ONTIME_DDL_TEMPLATE: &str = r#"
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

const PLAYGROUND_VERSION: &str = "v0.4.1-nightly";

#[derive(Clone)]
pub struct UpCommand {
    #[allow(dead_code)]
    conf: Config,
}

// Support to load datasets from official resource
#[derive(Clone)]
pub enum DataSets {
    OntimeMini(&'static str, &'static str),
}

// Implement the trait
impl FromStr for DataSets {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<DataSets, &'static str> {
        match s {
            "ontime_mini" => Ok(DataSets::OntimeMini(
                ONTIME_DOWNLOAD_URL,
                ONTIME_DDL_TEMPLATE,
            )),
            _ => Err("no match for profile"),
        }
    }
}

pub async fn generate_dashboard(status: &Status) -> Result<LocalDashboardConfig> {
    let query_configs = status.get_local_query_configs();
    if query_configs.is_empty() {
        return Err(CliError::Unknown(
            "No active query config exists".to_string(),
        ));
    }
    let (_, query) = query_configs.get(0).unwrap();
    let mut dashboard = LocalDashboardConfig {
        listen_addr: None,
        http_api: None,
        pid: None,
        path: None,
        log_dir: None,
    };
    dashboard.http_api = Some(format!(
        "http://{}:{}",
        query.config.query.http_handler_host, query.config.query.http_handler_port
    ));
    dashboard.listen_addr = Some(Status::find_unused_local_port());
    Ok(dashboard)
}

fn render(ddl: &str, template: serde_json::Value) -> Result<String> {
    let reg = handlebars::Handlebars::new();
    // render without register
    match reg.render_template(ddl, &template) {
        Ok(str) => Ok(str),
        Err(e) => {
            return Err(CliError::Unknown(format!(
                "cannot render DDL, error: {}",
                e
            )))
        }
    }
}

impl UpCommand {
    pub fn create(conf: Config) -> Self {
        UpCommand { conf }
    }

    pub fn default() -> Self {
        UpCommand::create(Config::default())
    }

    async fn download_dataset(&self, dataset: DataSets) -> Result<String> {
        return match dataset {
            DataSets::OntimeMini(url, _) => {
                let status = Status::read(self.conf.clone())?;
                let cfgs = status.get_local_query_configs();
                let (_, query_config) = cfgs.get(0).expect("cannot get local query config");
                let dataset_dir = query_config.config.storage.disk.data_path.as_str();
                let dataset_location = format!("{}/ontime_2019_2021.csv", dataset_dir);
                let download_location = format!(
                    "{}/downloads/datasets/ontime_mini.tar.gz",
                    self.conf.databend_dir
                );
                std::fs::create_dir_all(Path::new(
                    format!("{}/downloads/datasets/", self.conf.databend_dir).as_str(),
                ))?;
                if let Err(e) = download_and_unpack(
                    url,
                    &*download_location,
                    dataset_dir,
                    Some(dataset_location.clone()),
                ) {
                    return Err(CliError::Unknown(format!(
                        "Cannot download/unpack dataset {:?}",
                        e
                    )));
                }
                Ok::<String, CliError>(dataset_location)
            }
        };
    }

    async fn download_playground(&self) -> Result<String> {
        let arch = get_rust_architecture()?;
        let bin_name = format!("databend-playground-{}-{}.tar.gz", PLAYGROUND_VERSION, arch);
        let bin_file = format!("{}/downloads/{}", self.conf.databend_dir, bin_name);
        let url = format!(
            "{}/{}/{}",
            self.conf.mirror.playground_url.clone(),
            PLAYGROUND_VERSION,
            bin_name,
        );
        let target_dir = format!(
            "{}/bin/playground/{}",
            self.conf.databend_dir, PLAYGROUND_VERSION
        );
        std::fs::create_dir_all(Path::new(target_dir.as_str()))?;
        if let Err(e) = download_and_unpack(
            &*url,
            &*bin_file,
            &*target_dir,
            Some(format!("{}/databend-playground", target_dir)),
        ) {
            return Err(CliError::Unknown(format!(
                "Cannot download/unpack dataset {:?}",
                e
            )));
        }
        Ok(format!("{}/databend-playground", target_dir))
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
        writer.write_ok("Start to download dataset".to_string());
        match self.download_dataset(dataset.clone()).await {
            Ok(dataset_location) => {
                writer.write_ok(format!("Download dataset to {}", dataset_location));
                match dataset {
                    DataSets::OntimeMini(_, ddl) => {
                        if let Err(e) = self
                            .create_ddl(writer, dataset_location, "ontime".to_string(), ddl)
                            .await
                        {
                            return Err(e);
                        }
                    }
                }
                writer.write_ok("Start to download playground".to_string());

                match self.download_playground().await {
                    Ok(path) => {
                        let status = Status::read(self.conf.clone())?;
                        let mut config = generate_dashboard(&status).await?;
                        config.path = Some(path);
                        std::fs::create_dir_all(Path::new(
                            format!("{}/logs/dashboard/", self.conf.databend_dir).as_str(),
                        ))?;
                        config.log_dir =
                            Some(format!("{}/logs/dashboard/", self.conf.databend_dir));
                        if let Err(e) = self.provision_local_dashboard_service(writer, config).await
                        {
                            return Err(e);
                        }
                        let status = Status::read(self.conf.clone())?;
                        let (_, dash) = status
                            .get_local_dashboard_config()
                            .expect("no dashboard config exists");
                        if let Err(e) =
                            webbrowser::open(&*format!("http://{}", dash.listen_addr.unwrap()))
                        {
                            return Err(CliError::Unknown(format!(
                                "Cannot open web browser for dashboard error: {}",
                                e
                            )));
                        }
                    }
                    Err(e) => return Err(e),
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn provision_local_dashboard_service(
        &self,
        writer: &mut Writer,
        mut dash_config: LocalDashboardConfig,
    ) -> Result<()> {
        match dash_config.start().await {
            Ok(_) => {
                assert!(dash_config.get_pid().is_some());
                let mut status = Status::read(self.conf.clone())?;
                Status::save_local_config::<LocalDashboardConfig>(
                    &mut status,
                    "dashboard".to_string(),
                    "dashboard_config_0.yaml".to_string(),
                    &dash_config.clone(),
                )?;
                writer.write_ok(format!(
                    "👏 Successfully started meta service listen on {}",
                    dash_config
                        .listen_addr
                        .expect("dashboard config has no listen address")
                ));
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn create_ddl(
        &self,
        writer: &mut Writer,
        dataset_location: String,
        table_name: String,
        ddl: &str,
    ) -> Result<()> {
        if !Path::new(dataset_location.as_str()).exists() {
            return Err(CliError::Unknown(format!(
                "Cannot find dataset on {}",
                Path::new(dataset_location.as_str())
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
            )));
        }
        let query = QueryCommand::create(self.conf.clone());
        let ddl = render(
            ddl,
            json!({"csv_location": Path::new(dataset_location.as_str()).canonicalize().unwrap().to_str().unwrap()}),
        );
        match ddl {
            Ok(ddl) => {
                if let Err(e) = query
                    .exec(writer, format!(r#"DROP TABLE IF EXISTS {}"#, table_name))
                    .await
                {
                    return Err(e);
                }
                if let Err(e) = query.exec(writer, ddl).await {
                    return Err(e);
                }
            }
            Err(e) => return Err(e),
        }
        Ok(())
    }

    async fn local_exec_match(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()> {
        match self.local_exec_precheck(args) {
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
                writer.write_err(format!("Query command precheck failed, error {:?}", e));
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
        "up"
    }

    fn clap(&self) -> App<'static> {
        App::new("up")
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
            )
    }

    fn about(&self) -> &'static str {
        "Bootstrap a single cluster with dashboard"
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
