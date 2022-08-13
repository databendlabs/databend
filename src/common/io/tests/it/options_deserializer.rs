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

use std::collections::HashMap;

use common_io::prelude::OptionsDeserializer;
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Format {
    Csv,
    Parquet,
    Json,
}
impl Default for Format {
    fn default() -> Self {
        Format::Csv
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Compression {
    Auto,
    Gzip,
    Bz2,
    Brotli,
    Zstd,
    Deflate,
    RawDeflate,
    Lzo,
    Snappy,
    None,
}

impl Default for Compression {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct FileFormat {
    #[serde(default)]
    pub format: Format,
    #[serde(default = "default_record_delimiter")]
    pub record_delimiter: String,
    #[serde(default = "default_field_delimiter")]
    pub field_delimiter: String,
    #[serde(default = "default_skip_header")]
    pub skip_header: i32,
    #[serde(default = "default_compression")]
    pub compression: Compression,
}

fn default_record_delimiter() -> String {
    "\n".to_string()
}

fn default_field_delimiter() -> String {
    ",".to_string()
}

fn default_skip_header() -> i32 {
    0
}

fn default_compression() -> Compression {
    Compression::default()
}

#[test]
fn test_options_de() {
    let mut values = HashMap::new();
    values.insert("Format".to_string(), "Csv".to_string());
    values.insert("Field_delimiter".to_string(), "/".to_string());
    values.insert("skip_header".to_string(), "1".to_string());
    values.insert("compression".to_string(), "GZIP".to_string());

    let fmt = FileFormat::deserialize(OptionsDeserializer::new(&values)).unwrap();
    assert_eq!(fmt, FileFormat {
        format: Format::Csv,
        record_delimiter: "\n".to_string(),
        field_delimiter: "/".to_string(),
        skip_header: 1,
        compression: Compression::Gzip
    });

    let fmt = FileFormat::deserialize(OptionsDeserializer::new(&HashMap::new())).unwrap();
    assert_eq!(fmt, FileFormat {
        format: Format::Csv,
        record_delimiter: "\n".to_string(),
        field_delimiter: ",".to_string(),
        skip_header: 0,
        compression: Compression::None
    });

    values.insert("nokey".to_string(), "Parquet".to_string());

    let fmt = FileFormat::deserialize(OptionsDeserializer::new(&values));
    assert!(fmt.is_err());
}
