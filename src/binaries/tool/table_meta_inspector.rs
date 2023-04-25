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
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use clap::Parser;
use common_config::Config;
use common_config::DATABEND_COMMIT_VERSION;
use common_exception::Result;
use common_storage::init_operator;
use common_storage::StorageConfig;
use common_storages_fuse::io::read::meta::segment_reader::load_segment_v3;
use common_storages_fuse::io::read::meta::snapshot_reader::load_snapshot_v3;
use opendal::services::Fs;
use opendal::Operator;
use opendal::Reader;
use serde::Deserialize;
use serde::Serialize;
use serde_json;
use serfig::collectors::from_file;
use serfig::parsers::Toml;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Parser)]
#[clap(about, version = &**DATABEND_COMMIT_VERSION, author)]
pub struct InspectorConfig {
    #[clap(long, short = 'i')]
    pub input: String,

    #[clap(long, short = 'o')]
    pub output: Option<String>,

    #[clap(long, short = 'c')]
    pub config: Option<String>,

    #[clap(long = "type", short = 't')]
    pub meta_type: String,
}

async fn read_meta_data(reader: &mut Reader, config: &InspectorConfig) -> Result<Vec<u8>> {
    let out = match config.meta_type.as_str() {
        "sg" | "segment" => serde_json::to_vec(&load_segment_v3(reader).await?)?,
        "ss" | "snapshot" => serde_json::to_vec(&load_snapshot_v3(reader).await?)?,
        _ => {
            return Err(format!(
                "Unsupported type: {}, only support ss/snapshot or sg/segment",
                config.meta_type
            )
            .into());
        }
    };
    Ok(out)
}

fn parse_output_file(config: &InspectorConfig) -> Result<PathBuf> {
    match &config.output {
        Some(output) => Ok(Path::new(&output).to_path_buf()),
        None => {
            let path = Path::new(&config.input);
            let mut new_name = path.file_name().ok_or("Invalid file path").to_owned();
            new_name.push("-decode");
            match &config.config {
                Some(_) => {
                    let current_dir = env::current_dir()?;
                    Ok(current_dir.join(new_name))
                }
                None => Ok(path.with_file_name(new_name)),
            }
        }
    }
}

async fn parse_reader(config: &InspectorConfig) -> Result<Reader> {
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
            Operator::new(builder)?.finish()
        }
    };
    Ok(op.reader(&config.input).await?)
}

async fn run(config: &InspectorConfig) -> Result<()> {
    let mut reader = parse_reader(config).await?;

    let out = read_meta_data(&mut reader, config).await?;

    let output_path = parse_output_file(config)?;

    let mut file = File::create(&output_path)?;
    file.write_all(&out)?;

    println!("Output file: {:?}", output_path);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = InspectorConfig::parse();
    // let args = App::new("table-meta-inspector")
    //     .arg(
    //         Arg::with_name("input")
    //             .short('i')
    //             .long("input")
    //             .value_name("FILE")
    //             .help("Sets the input file to use")
    //             .required(false),
    //     )
    //     .arg(
    //         Arg::with_name("config")
    //             .short('c')
    //             .long("config")
    //             .value_name("CONFIG")
    //             .help("Sets the config file to use")
    //             .required(false),
    //     )
    //     .arg(
    //         Arg::with_name("output")
    //             .short('o')
    //             .long("output")
    //             .value_name("FILE")
    //             .help("Sets the output file to use"),
    //     )
    //     .arg(
    //         Arg::with_name("type")
    //             .short('t')
    //             .long("type")
    //             .help("Sets the type of meta data - ss/snapshot or sg/segment")
    //             .value_name("TYPE")
    //             .required(true),
    //     )
    //     .about("Decode v3 meta data tool")
    //     .version(&**DATABEND_COMMIT_VERSION)
    //     .author(env!("CARGO_PKG_AUTHORS"))
    //     .get_matches();

    if let Err(err) = run(&config).await {
        println!("Error: {:?}", err);
    }
    Ok(())
}
