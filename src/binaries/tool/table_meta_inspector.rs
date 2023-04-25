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

use clap::App;
use clap::Arg;
use clap::ArgMatches;
use common_config::Config;
use common_config::StorageConfig as InnerStorageConfig;
use common_config::DATABEND_COMMIT_VERSION;
use common_exception::Result;
use common_storage::init_operator;
use common_storages_fuse::io::read::meta::segment_reader::load_segment_v3;
use common_storages_fuse::io::read::meta::snapshot_reader::load_snapshot_v3;
use opendal::services::Fs;
use opendal::Operator;
use opendal::Reader;
use serde_json;
use serfig::collectors::from_file;
use serfig::parsers::Toml;

async fn read_meta_data(reader: &mut Reader, meta_type: &str) -> Result<Vec<u8>> {
    let out = match meta_type {
        "sg" | "segment" => serde_json::to_vec(&load_segment_v3(reader).await?)?,
        "ss" | "snapshot" => serde_json::to_vec(&load_snapshot_v3(reader).await?)?,
        _ => {
            return Err(format!(
                "Unsupported type: {}, only support ss/snapshot or sg/segment",
                meta_type
            )
            .into());
        }
    };
    Ok(out)
}

fn parse_output_file(args: &ArgMatches, input_val: &String) -> PathBuf {
    args.get_one::<String>("output")
        .map(|output_val| Path::new(output_val).to_path_buf())
        .unwrap_or_else(|| {
            let path = Path::new(input_val);
            let mut new_name = path.file_name().unwrap().to_owned();
            new_name.push("-decode");
            path.with_file_name(new_name).to_path_buf()
        })
}

async fn parse_reader(args: &ArgMatches) -> Result<Reader> {
    let input_path = args
        .get_one::<String>("input")
        .ok_or("Need input file path")?;
    println!("Input file: {:?}", input_path);
    let config_file = args.get_one::<String>("config");
    let op = match config_file {
        Some(config_file) => {
            let mut builder: serfig::Builder<Config> = serfig::Builder::default();
            builder = builder.collect(from_file(Toml, &config_file));
            let conf = InnerStorageConfig::try_into(builder.build()?.storage)?;
            init_operator(&conf.params)?
        }
        None => {
            let current_dir = env::current_dir()?;
            let mut builder = Fs::default();
            builder.root(current_dir.to_str().ok_or("Invalid path")?);
            Operator::new(builder)?.finish()
        }
    };
    Ok(op.reader(input_path).await?)
}

async fn run(args: &ArgMatches) -> Result<()> {
    let mut reader = parse_reader(args).await?;

    let meta_type = args.get_one::<String>("type").ok_or("Need meta type")?;

    let out = read_meta_data(&mut reader, meta_type).await?;

    let output_path = parse_output_file(args, input_path);
    let mut file = File::create(&output_path)?;

    file.write_all(&out)?;
    println!("Output file: {:?}", output_path);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = App::new("table-meta-inspector")
        .arg(
            Arg::with_name("input")
                .short('i')
                .long("input")
                .value_name("FILE")
                .help("Sets the input file to use")
                .required(false),
        )
        .arg(
            Arg::with_name("config")
                .short('c')
                .long("config")
                .value_name("CONFIG")
                .help("Sets the config file to use")
                .required(false),
        )
        .arg(
            Arg::with_name("output")
                .short('o')
                .long("output")
                .value_name("FILE")
                .help("Sets the output file to use"),
        )
        .arg(
            Arg::with_name("type")
                .short('t')
                .long("type")
                .help("Sets the type of meta data - ss/snapshot or sg/segment")
                .value_name("TYPE")
                .required(true),
        )
        .about("Decode v3 meta data tool")
        .version(&**DATABEND_COMMIT_VERSION)
        .author(env!("CARGO_PKG_AUTHORS"))
        .get_matches();

    if let Err(err) = run(&args).await {
        println!("Error: {:?}", err);
    }
    Ok(())
}
