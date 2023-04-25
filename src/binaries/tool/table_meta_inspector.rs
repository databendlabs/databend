// Copyright 2022 Datafuse Labs.
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

use std::env;
use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::io::stdout;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use clap::Parser;
use common_config::Config;
use common_config::DATABEND_COMMIT_VERSION;
use common_exception::Result;
use common_storage::init_operator;
use common_storage::StorageConfig;
use common_storages_fuse::io::read::meta::snapshot_reader::load_snapshot_v3;
use common_storages_fuse::io::read::meta::SegmentInfoReader::segment_reader::load_segment_v3;
use common_storages_fuse::io::SegmentInfoReader;
use opendal::services::Fs;
use opendal::Operator;
use opendal::Reader;
use serde::Deserialize;
use serde::Serialize;
use serde_json;
use serfig::collectors::from_file;
use serfig::parsers::Toml;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::TableSnapshot;
use tokio::io::AsyncReadExt;

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

async fn read_meta_data(config: &InspectorConfig) -> Result<Vec<u8>> {
    let mut buffer: Vec<u8> = vec![];
    let mut reader = parse_reader(config).await?;
    reader.read_to_end(&mut buffer).await?;
    match config.meta_type.as_str() {
        "sg" | "segment" => Ok(serde_json::to_vec(&SegmentInfo::from_bytes(buffer)?)?),
        "ss" | "snapshot" => Ok(serde_json::to_vec(&TableSnapshot::from_bytes(buffer)?)?),
        _ => Err(format!(
            "Unsupported type: {}, only support ss/snapshot or sg/segment",
            config.meta_type
        )
        .into()),
    }
}

fn parse_output(config: &InspectorConfig) -> Result<BufWriter<dyn Write>> {
    if let Some(output) = &config.output {
        let file = File::create(&output)?;
        let writer = BufWriter::new(file);
        Ok(writer)
    } else {
        let writer = BufWriter::new(stdout());
        Ok(writer)
    }
}

async fn parse_reader<R: Read>(config: &InspectorConfig) -> Result<R> {
    match &config.input {
        Some(input) => {
            let op = match &config.config {
                Some(config_file) => {
                    let mut builder: serfig::Builder<Config> = serfig::Builder::default();
                    builder = builder.collect(from_file(Toml, &config_file));
                    let conf: StorageConfig = builder.build()?.storage.try_into()?;
                    init_operator(&conf.params)?
                }
                None => {
                    let current_dir = env::current_dir()?;
                    let mut builder = Fs::default();
                    builder.root(current_dir.to_str().ok_or("Invalid path")?);
                    Operator::new(builder)?.finish();
                }
            };
            Ok(op.reader(input).await?)
        }
        None => {
            let stdin = io::stdin();
            let handle = stdin.lock();
            let reader = io::BufReader::new(handle);
            Ok(reader)
        }
    }
}

async fn run(config: &InspectorConfig) -> Result<()> {
    let out = read_meta_data(config).await?;

    let mut writer = parse_output(config)?;

    writer.write(&out)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = InspectorConfig::parse();

    if let Err(err) = run(&config).await {
        println!("Error: {:?}", err);
    }
    Ok(())
}
