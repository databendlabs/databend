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
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use clap::ValueHint;
use databend_meta::configs::Config as MetaConfig;
use databend_query::configs::Config as QueryConfig;
use lexical_util::num::AsPrimitive;
use sysinfo::ProcessExt;
use sysinfo::Signal;
use sysinfo::System;
use sysinfo::SystemExt;

use crate::cmds::clusters::cluster::ClusterProfile;
use crate::cmds::clusters::stop::StopCommand;
use crate::cmds::command::Command;
use crate::cmds::packages::fetch::get_version;
use crate::cmds::status::LocalMetaConfig;
use crate::cmds::status::LocalQueryConfig;
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

#[derive(Clone)]
pub struct LocalBinaryPaths {
    pub query: String,
    pub meta: String,
}

async fn reconcile_local_meta(status: &mut Status) -> Result<()> {
    let s = System::new_all();
    if let Some((_, meta)) = status.get_local_meta_config() {
        if meta.pid.is_none() || s.process(meta.pid.unwrap()).is_none() {
            return Err(CliError::Unknown(
                "meta service process not found".to_string(),
            ));
        }
        if meta.verify(None, None).await.is_err() {
            return Err(CliError::Unknown("cannot verify meta service".to_string()));
        }
    }
    Ok(())
}

async fn reconcile_local_query(status: &mut Status) -> Result<()> {
    let s = System::new_all();
    for (_, query) in status.get_local_query_configs() {
        if query.pid.is_none() || s.process(query.pid.unwrap()).is_none() {
            return Err(CliError::Unknown(
                "query service process not found".to_string(),
            ));
        }
        if query.verify(None, None).await.is_err() {
            return Err(CliError::Unknown("cannot verify query service".to_string()));
        }
    }
    Ok(())
}

async fn reconcile_local(status: &mut Status) -> Result<()> {
    if status.has_local_configs() {
        if let Err(e) = reconcile_local_meta(status).await {
            return Err(e);
        }
        if let Err(e) = reconcile_local_query(status).await {
            return Err(e);
        }
    }
    Ok(())
}

pub fn generate_query_config() -> QueryConfig {
    let mut config = QueryConfig::default();
    if config.query.http_api_address.parse::<SocketAddr>().is_err()
        || !portpicker::is_free(
            config
                .query
                .http_api_address
                .parse::<SocketAddr>()
                .unwrap()
                .port(),
        )
    {
        config.query.http_api_address = Status::find_unused_local_port()
    }

    if config
        .query
        .metric_api_address
        .parse::<SocketAddr>()
        .is_err()
        || !portpicker::is_free(
            config
                .query
                .metric_api_address
                .parse::<SocketAddr>()
                .unwrap()
                .port(),
        )
    {
        config.query.metric_api_address = Status::find_unused_local_port()
    }
    if config
        .query
        .flight_api_address
        .parse::<SocketAddr>()
        .is_err()
        || !portpicker::is_free(
            config
                .query
                .flight_api_address
                .parse::<SocketAddr>()
                .unwrap()
                .port(),
        )
    {
        config.query.flight_api_address = Status::find_unused_local_port()
    }
    if config.query.mysql_handler_host.is_empty() {
        config.query.mysql_handler_host = "0.0.0.0".to_string();
    }
    if config.query.clickhouse_handler_host.is_empty() {
        config.query.clickhouse_handler_host = "0.0.0.0".to_string();
    }

    if config.query.http_handler_host.is_empty() {
        config.query.http_handler_host = "0.0.0.0".to_string();
    }
    if !portpicker::is_free(config.query.mysql_handler_port) {
        config.query.mysql_handler_port = portpicker::pick_unused_port().unwrap();
    }
    if !portpicker::is_free(config.query.clickhouse_handler_port) {
        config.query.clickhouse_handler_port = portpicker::pick_unused_port().unwrap();
    }
    if !portpicker::is_free(config.query.http_handler_port) {
        config.query.http_handler_port = portpicker::pick_unused_port().unwrap();
    }
    config
}

pub async fn provision_local_query_service(
    mut status: &mut Status,
    writer: &mut Writer,
    mut query_config: LocalQueryConfig,
    file_name: String,
) -> Result<()> {
    match query_config.start().await {
        Ok(_) => {
            assert!(query_config.get_pid().is_some());
            Status::save_local_config::<LocalQueryConfig>(
                &mut status,
                "query".to_string(),
                file_name,
                &query_config.clone(),
            )?;
            status.write()?;
            writer.write_ok("Successfully started query service.".to_string());
            writer.write_ok(
                "To run queries through bendctl, run: bendctl query 'your SQL'".to_string(),
            );
            writer.write_ok(
                "For example: bendctl query 'SELECT sum(number), avg(number) FROM numbers(100)'"
                    .to_string(),
            );
            writer.write_ok(format!(
                "To process mysql queries, run: mysql -h{} -P{} -uroot",
                query_config.config.query.mysql_handler_host,
                query_config.config.query.mysql_handler_port
            ));
            writer.write_ok(
                format!(
                    "To process clickhouse queries, run: clickhouse client --host {} --port {} --user root",
                    query_config.config.query.clickhouse_handler_host,
                    query_config.config.query.clickhouse_handler_port
                )
            );
            writer.write_ok(
                format!(
                    "To process HTTP REST queries, run: curl --location --request POST '{}:{}/v1/statement/' --header 'Content-Type: text/plain' --data-raw 'your SQL'",
                    query_config.config.query.http_handler_host,
                    query_config.config.query.http_handler_port
                )
            );

            Ok(())
        }
        Err(e) => Err(e),
    }
}

pub fn generate_local_log_dir(conf: &Config, dir_name: &str) -> String {
    let log_base = format!("{}/logs", conf.clone().databend_dir);
    if !Path::new(log_base.as_str()).exists() {
        fs::create_dir(Path::new(log_base.as_str()))
            .unwrap_or_else(|_| panic!("cannot create directory {}", log_base));
    }
    let query_log_dir = format!("{}/{}", log_base, dir_name);
    if !Path::new(query_log_dir.as_str()).exists() {
        fs::create_dir(Path::new(query_log_dir.as_str())).unwrap_or_else(|_| {
            panic!("cannot create meta serivce log directory {}", query_log_dir)
        });
    }
    query_log_dir
}

pub fn generate_local_query_config(
    conf: Config,
    args: &ArgMatches,
    bin_path: LocalBinaryPaths,
    meta_config: &LocalMetaConfig,
    log_dir: String,
) -> Result<LocalQueryConfig> {
    let mut config = generate_query_config();
    // configure meta address based on provisioned meta service

    config.meta.meta_address = meta_config.clone().config.flight_api_address;

    config.meta.meta_username = "root".to_string();
    config.meta.meta_password = "root".to_string();
    // tenant
    if args.value_of("query_namespace").is_some()
        && !args.value_of("query_namespace").unwrap().is_empty()
    {
        config.query.namespace = args.value_of("query_namespace").unwrap().to_string();
    }

    if args.value_of("query_tenant").is_some() && !args.value_of("query_tenant").unwrap().is_empty()
    {
        config.query.tenant = args.value_of("query_tenant").unwrap().to_string();
    }

    if args.value_of("num_cpus").is_some() && !args.value_of("num_cpus").unwrap().is_empty() {
        config.query.num_cpus = args.value_of("num_cpus").unwrap().parse::<u64>().unwrap();
    }

    // mysql handler
    if args.value_of("mysql_handler_port").is_some()
        && !args.value_of("mysql_handler_port").unwrap().is_empty()
    {
        config.query.mysql_handler_port = args
            .value_of("mysql_handler_port")
            .unwrap()
            .parse::<u16>()
            .unwrap();
    }

    // clickhouse handler
    if args.value_of("clickhouse_handler_port").is_some()
        && !args.value_of("clickhouse_handler_port").unwrap().is_empty()
    {
        config.query.clickhouse_handler_port = args
            .value_of("clickhouse_handler_port")
            .unwrap()
            .parse::<u16>()
            .unwrap();
    }
    // storage
    let storage_type = args.value_of("storage_type");
    match storage_type {
        Some("disk") => {
            if args.value_of("disk_path").is_some()
                && !args.value_of("disk_path").unwrap().is_empty()
            {
                if !Path::new(&args.value_of("disk_path").unwrap()).exists() {
                    return Err(CliError::Unknown(format!(
                        "cannot find local disk_path in {}",
                        args.value_of("disk_path").unwrap()
                    )));
                }
                config.storage.disk.data_path =
                    fs::canonicalize(&args.value_of("disk_path").unwrap())
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string()
            } else {
                let data_dir = format!("{}/data", conf.databend_dir);
                if !Path::new(data_dir.as_str()).exists()
                    && fs::create_dir(Path::new(data_dir.as_str())).is_err()
                {
                    return Err(CliError::Unknown(format!(
                        "cannot find local disk_path in {}",
                        data_dir
                    )));
                }

                config.storage.disk.data_path = fs::canonicalize(data_dir)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string()
            }
        }
        Some("s3") => {
            todo!()
        }
        Some(_) | None => {
            return Err(CliError::Unknown(
                "storage type is not supported for now".parse().unwrap(),
            ))
        }
    }

    // log
    config.log.log_level = args.value_of("log_level").unwrap().to_string();
    let query_log_dir = log_dir;

    config.log.log_dir = query_log_dir.clone();
    Ok(LocalQueryConfig {
        config,
        pid: None,
        path: Some(bin_path.query),
        log_dir: Some(query_log_dir),
    })
}

pub fn get_bin(conf: &Config, status: &Status) -> Result<LocalBinaryPaths> {
    let mut paths = LocalBinaryPaths {
        query: "".to_string(),
        meta: "".to_string(),
    };
    paths.query = binary_path(
        format!("{}/bin", conf.databend_dir),
        status.version.clone(),
        "databend-query".to_string(),
    )
    .expect("cannot find query bin");
    paths.meta = binary_path(
        format!("{}/bin", conf.databend_dir),
        status.version.clone(),
        "databend-meta".to_string(),
    )
    .expect("cannot find meta service binary");
    Ok(paths)
}

async fn ensure_bin(
    conf: Config,
    version: String,
    writer: &mut Writer,
    args: &ArgMatches,
) -> Result<LocalBinaryPaths> {
    let mut status = Status::read(conf.clone())?;

    let current_version = version;
    status.version = current_version.clone();
    status.write()?;
    if binary_path(
        format!("{}/bin", conf.databend_dir),
        current_version,
        "databend-query".to_string(),
    )
    .is_err()
    {
        // fetch latest version of databend binary if version not found
        writer.write_ok(format!(
            "Cannot find databend binary path in version {}, start to download from {:?}",
            args.value_of("version").unwrap(),
            status.mirrors
        ));
        FetchCommand::create(conf.clone())
            .exec_matches(writer, Some(args))
            .await?;
    }
    SwitchCommand::create(conf.clone())
        .exec_matches(writer, Some(args))
        .await?;
    let status = Status::read(conf.clone())?;
    let paths = get_bin(&conf, &status)?;
    Ok(paths)
}

fn binary_path(dir: String, version: String, name: String) -> Result<String> {
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

impl CreateCommand {
    pub fn create(conf: Config) -> Self {
        CreateCommand { conf }
    }

    fn generate_meta_config(&self) -> MetaConfig {
        let mut config = MetaConfig::empty();
        if config.metric_api_address.parse::<SocketAddr>().is_err()
            || !portpicker::is_free(
                config
                    .metric_api_address
                    .parse::<SocketAddr>()
                    .unwrap()
                    .port(),
            )
        {
            config.metric_api_address = Status::find_unused_local_port()
        }

        if config.admin_api_address.parse::<SocketAddr>().is_err()
            || !portpicker::is_free(
                config
                    .admin_api_address
                    .parse::<SocketAddr>()
                    .unwrap()
                    .port(),
            )
        {
            config.admin_api_address = Status::find_unused_local_port()
        }
        if config.flight_api_address.parse::<SocketAddr>().is_err()
            || !portpicker::is_free(
                config
                    .admin_api_address
                    .parse::<SocketAddr>()
                    .unwrap()
                    .port(),
            )
        {
            config.flight_api_address = Status::find_unused_local_port()
        }
        if !portpicker::is_free(config.raft_config.raft_api_port.as_u16()) {
            config.raft_config.raft_api_port = portpicker::pick_unused_port().unwrap() as u32;
        }
        if config.raft_config.raft_api_host.is_empty() {
            config.raft_config.raft_api_host = "0.0.0.0".to_string();
        }
        config
    }
    pub fn generate_local_meta_config(
        &self,
        args: &ArgMatches,
        bin_path: LocalBinaryPaths,
    ) -> Option<LocalMetaConfig> {
        let mut config = self.generate_meta_config();

        if args.value_of("meta_address").is_some()
            && !args.value_of("meta_address").unwrap().is_empty()
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
            fs::create_dir(Path::new(meta_log_dir.as_str())).unwrap_or_else(|_| {
                panic!("cannot create meta serivce log directory {}", meta_log_dir)
            });
        }
        config.log_dir = meta_log_dir.clone();
        config.raft_config.single = true;
        let raft_dir = format!("{}/local_raft_dir", log_base);
        if !Path::new(raft_dir.as_str()).exists() {
            fs::create_dir(Path::new(raft_dir.as_str()))
                .unwrap_or_else(|_| panic!("cannot create raft data directory {}", raft_dir));
        }
        config.raft_config.raft_dir = raft_dir;
        Some(LocalMetaConfig {
            config,
            pid: None,
            path: Some(bin_path.meta),
            log_dir: Some(meta_log_dir),
        })
    }

    async fn provision_local_meta_service(
        &self,
        writer: &mut Writer,
        mut meta_config: LocalMetaConfig,
    ) -> Result<()> {
        match meta_config.start().await {
            Ok(_) => {
                assert!(meta_config.get_pid().is_some());
                let mut status = Status::read(self.conf.clone())?;
                Status::save_local_config::<LocalMetaConfig>(
                    &mut status,
                    "meta".to_string(),
                    "meta_config_0.yaml".to_string(),
                    &meta_config.clone(),
                )?;
                writer.write_ok(format!(
                    "Successfully started meta service with rpc endpoint {}",
                    meta_config.config.flight_api_address
                ));
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn local_exec_match(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()> {
        match self.local_exec_precheck(writer, args).await {
            Ok(_) => {
                writer.write_ok("Databend cluster pre-check passed!".to_string());
                // ensuring needed dependencies
                let version =
                    get_version(&self.conf, args.value_of("version").map(|s| s.to_string()))?;
                let bin_path = ensure_bin(self.conf.clone(), version, writer, args)
                    .await
                    .expect("cannot find binary path");
                let meta_config = self
                    .generate_local_meta_config(args, bin_path.clone())
                    .expect("Cannot generate meta-service config");
                {
                    let res = self
                        .provision_local_meta_service(writer, meta_config.clone())
                        .await;
                    if res.is_err() {
                        writer.write_err(format!(
                            "Cannot provision meta service, error: {:?}, please check the logs output by: tail -n 100 {}/*",
                            res.unwrap_err(),
                            meta_config.log_dir.unwrap_or_else(|| "unknown log_dir".into()),
                        ));
                        return Ok(());
                    }
                }
                let meta_status = meta_config.verify(None, None).await;
                if meta_status.is_err() {
                    let mut status = Status::read(self.conf.clone())?;
                    writer.write_err(format!(
                        "Cannot connect to meta service: {:?}",
                        meta_status.unwrap_err()
                    ));
                    StopCommand::stop_current_local_services(&mut status, writer)
                        .await
                        .unwrap();
                    return Ok(());
                }
                let local_log_dir = generate_local_log_dir(&self.conf, "local_query_0");
                let query_config = generate_local_query_config(
                    self.conf.clone(),
                    args,
                    bin_path,
                    &meta_config,
                    local_log_dir,
                );
                if query_config.is_err() {
                    let mut status = Status::read(self.conf.clone())?;
                    writer.write_err(format!(
                        "Cannot generate query configurations, error: {:?}",
                        query_config.as_ref().unwrap_err()
                    ));
                    StopCommand::stop_current_local_services(&mut status, writer)
                        .await
                        .unwrap();
                }
                writer.write_ok(format!(
                    "Local data would be stored in {}",
                    query_config
                        .as_ref()
                        .unwrap()
                        .config
                        .storage
                        .disk
                        .data_path
                        .as_str()
                ));
                {
                    let mut status = Status::read(self.conf.clone())?;
                    let res = provision_local_query_service(
                        &mut status,
                        writer,
                        query_config.as_ref().unwrap().clone(),
                        "query_config_0.yaml".to_string(),
                    )
                    .await;
                    if res.is_err() {
                        let mut status = Status::read(self.conf.clone())?;
                        writer.write_err(format!(
                            "Cannot provison query service, error: {:?}, please check the logs output by: tail -n 100 {}/*",
                            res.unwrap_err(),
                            query_config.as_ref().unwrap().config.log.log_dir,
                        ));
                        StopCommand::stop_current_local_services(&mut status, writer)
                            .await
                            .unwrap();
                    }
                }
                let mut status = Status::read(self.conf.clone())?;
                status.current_profile =
                    Some(serde_json::to_string::<ClusterProfile>(&ClusterProfile::Local).unwrap());
                status.write()?;

                Ok(())
            }
            Err(e) => {
                writer.write_err(format!("Cluster precheck failed, error {:?}", e));
                Ok(())
            }
        }
    }

    /// precheck whether current local profile applicable for local host machine
    async fn local_exec_precheck(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()> {
        let mut status = Status::read(self.conf.clone())?;
        if args.is_present("force") {
            writer.write_ok("Delete existing cluster".to_string());
            StopCommand::stop_current_local_services(&mut status, writer)
                .await
                .expect("cannot stop current services");
            let s = System::new_all();
            for elem in s.process_by_name("databend-meta") {
                elem.kill(Signal::Term);
            }
            for elem in s.process_by_name("databend-query") {
                elem.kill(Signal::Term);
            }
            for elem in s.process_by_name("databend-dashboard") {
                elem.kill(Signal::Term);
            }
        }
        if let Err(e) = reconcile_local(&mut status).await {
            writer.write_ok(format!(
                "Local environment has problem {:?}, start reconcile",
                e
            ));
            if let Err(e) = StopCommand::stop_current_local_services(&mut status, writer).await {
                writer.write_err(format!("Cannot delete existing service, error: {:?}", e));
                return Err(e);
            }
        }
        if status.has_local_configs() {
            return Err(CliError::Unknown(format!(
                "❗ found previously existed cluster with config in {}",
                status.local_config_dir
            )));
        }
        let s = System::new_all();

        if !s.process_by_name("databend-meta").is_empty() {
            return Err(CliError::Unknown(
                "❗ have installed databend-meta process before, please stop them and retry"
                    .parse()
                    .unwrap(),
            ));
        }
        if !s.process_by_name("databend-query").is_empty() {
            return Err(CliError::Unknown(
                "❗ have installed databend-query process before, please stop them and retry"
                    .parse()
                    .unwrap(),
            ));
        }
        if args.value_of("meta_address").is_some()
            && !args.value_of("meta_address").unwrap().is_empty()
        {
            let meta_address = SocketAddr::from_str(args.value_of("meta_address").unwrap());
            match meta_address {
                Ok(addr) => {
                    if !portpicker::is_free(addr.port()) {
                        return Err(CliError::Unknown(format!(
                            "Address {} has been used for local meta service",
                            addr.port()
                        )));
                    }
                }
                Err(e) => {
                    return Err(CliError::Unknown(format!(
                        "Cannot parse meta service address, error: {:?}",
                        e
                    )))
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Command for CreateCommand {
    fn name(&self) -> &str {
        "create"
    }

    fn clap(&self) -> App<'static> {
        App::new("create")
            .setting(AppSettings::DisableVersionFlag)
            .about(self.about())
            .arg(
                Arg::new("profile")
                    .long("profile")
                    .about("Profile for deployment, support local and cluster")
                    .required(false)
                    .takes_value(true)
                    .possible_values(&["local"]).default_value("local"),
            )
            .arg(
                Arg::new("meta_address")
                    .long("meta-address")
                    .about("Set endpoint to provide metastore service")
                    .takes_value(true)
                    .env(databend_query::configs::config_meta::META_ADDRESS),
            )
            .arg(
                Arg::new("log_level")
                    .long("log-level")
                    .about("Set logging level")
                    .takes_value(true)
                    .env(databend_query::configs::config_log::LOG_LEVEL)
                    .default_value("INFO"),
            )
            .arg(
                Arg::new("version")
                    .long("version")
                    .takes_value(true)
                    .about("Set databend version to run")
                    .default_value("latest"),
            )
            .arg(
                Arg::new("num_cpus")
                    .long("num-cpus")
                    .env(databend_query::configs::config_query::QUERY_NUM_CPUS)
                    .takes_value(true)
                    .about("Set number of cpus for query instance to use")
                    .default_value(""),
            )
            .arg(
                Arg::new("query_namespace")
                    .long("query-namespace")
                    .env(databend_query::configs::config_query::QUERY_NAMESPACE)
                    .takes_value(true)
                    .about("Set the namespace for query to work on")
                    .default_value("test_cluster"),
            )
            .arg(
                Arg::new("query_tenant")
                    .long("query-tenant")
                    .env(databend_query::configs::config_query::QUERY_TENANT)
                    .takes_value(true)
                    .about("Set the tenant for query to work on")
                    .default_value("test"),
            )
            .arg(
                Arg::new("mysql_handler_port")
                    .long("mysql-handler-port")
                    .takes_value(true)
                    .env(databend_query::configs::config_query::QUERY_MYSQL_HANDLER_PORT)
                    .about("Configure the port for mysql endpoint to run queries in mysql client")
                    .default_value("3307"),
            )
            .arg(
                Arg::new("clickhouse_handler_port")
                    .long("clickhouse-handler-port")
                    .env(databend_query::configs::config_query::QUERY_CLICKHOUSE_HANDLER_HOST)
                    .takes_value(true)
                    .about("Configure the port clickhouse endpoint to run queries in clickhouse client")
                    .default_value("9000"),
            )
            .arg(
                Arg::new("storage_type")
                    .long("storage-type")
                    .takes_value(true)
                    .env(databend_query::configs::config_storage::STORAGE_TYPE)
                    .about("Set the storage medium to store datasets, support disk or s3 object storage ")
                    .possible_values(&["disk", "s3"]).default_value("disk"),
            )
            .arg(
                Arg::new("disk_path")
                    .long("disk-path")
                    .takes_value(true)
                    // .env(databend_query::configs::config_storage::DISK_STORAGE_DATA_PATH)
                    .about("Set the root directory to store all datasets")
                    .value_hint(ValueHint::DirPath),
            )
            .arg(
                Arg::new("force")
                    .long("force")
                    // .env(databend_query::configs::config_storage::DISK_STORAGE_DATA_PATH)
                    .about("Delete existing cluster and install new cluster without check")
                    .takes_value(false),
            )
    }

    fn subcommands(&self) -> Vec<Arc<dyn Command>> {
        vec![]
    }

    fn about(&self) -> &'static str {
        "Create a databend cluster based on profile"
    }

    fn is(&self, s: &str) -> bool {
        s.contains(self.name())
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
