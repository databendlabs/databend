// Copyright 2021 Datafuse Labs
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
use std::env;
use std::fs::File;
use std::io;
use std::io::stdout;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;

use clap::Parser;
use databend_common_config::Config;
use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_storage::init_operator;
use databend_common_storage::StorageConfig;
use databend_common_tracing::init_logging;
use databend_common_tracing::Config as LogConfig;
use databend_common_version::DATABEND_COMMIT_VERSION;
use databend_query::GlobalServices;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::info;
use opendal::services::Fs;
use opendal::Operator;
use serde::Deserialize;
use serde::Serialize;
use serfig::collectors::from_file;
use serfig::parsers::Toml;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Parser)]
#[clap(about, version = &**DATABEND_COMMIT_VERSION, author)]
pub struct InspectorConfig {
    #[clap(long, short = 'i')]
    pub input: Option<String>,

    #[clap(long, short = 'o')]
    pub output: Option<String>,

    #[clap(long, short = 'c')]
    pub config: Option<String>,

    #[clap(long = "type", short = 't')]
    pub meta_type: String,
}

fn convert_input_data(data: Vec<u8>, config: &InspectorConfig) -> Result<Vec<u8>> {
    match config.meta_type.as_str() {
        "sg" | "segment" => Ok(serde_json::to_vec(&SegmentInfo::from_slice(&data)?)?),
        "ss" | "snapshot" => Ok(serde_json::to_vec(&TableSnapshot::from_slice(&data)?)?),
        _ => Err(format!(
            "Unsupported type: {}, only support ss/snapshot or sg/segment",
            config.meta_type
        )
        .into()),
    }
}

fn parse_output(config: &InspectorConfig) -> Result<Box<dyn Write>> {
    let writer = if let Some(output) = &config.output {
        let file = File::create(output)?;
        Box::new(BufWriter::new(file)) as Box<dyn Write>
    } else {
        Box::new(BufWriter::new(stdout())) as Box<dyn Write>
    };
    Ok(writer)
}

async fn parse_input_data(config: &InspectorConfig) -> Result<Vec<u8>> {
    match &config.input {
        Some(input) => {
            let op = match &config.config {
                Some(config_file) => {
                    let mut builder: serfig::Builder<Config> = serfig::Builder::default();
                    builder = builder.collect(from_file(Toml, config_file));
                    let read_config = builder.build()?;
                    let inner_config: InnerConfig = read_config.clone().try_into()?;
                    GlobalServices::init(&inner_config, false).await?;
                    let storage_config: StorageConfig = read_config.storage.try_into()?;
                    init_operator(&storage_config.params)?
                }
                None => {
                    let current_dir = env::current_dir()?;
                    let builder = Fs::default().root(current_dir.to_str().ok_or("Invalid path")?);
                    Operator::new(builder)?.finish()
                }
            };
            let buf = op.read(input).await?.to_vec();
            Ok(buf)
        }
        None => {
            let mut buffer: Vec<u8> = vec![];
            let stdin = io::stdin();
            let handle = stdin.lock();
            io::BufReader::new(handle).read_to_end(&mut buffer)?;
            Ok(buffer)
        }
    }
}

async fn run(config: &InspectorConfig) -> Result<()> {
    let input = parse_input_data(config).await?;

    let out = convert_input_data(input, config)?;

    let mut writer = parse_output(config)?;

    writer.write_all(&out)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut log_config = LogConfig::default();
    log_config.stderr.on = true;
    let _guards = init_logging("table_meta_inspector", &log_config, BTreeMap::new());

    let config = InspectorConfig::parse();

    if let Err(err) = run(&config).await {
        info!("Error: {}", err);
    }
    Ok(())
}
