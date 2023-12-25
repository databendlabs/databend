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

use std::str::FromStr;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::StageFileFormatType;

const SUFFIX_WITH_NAMES_AND_TYPES: &str = "withnamesandtypes";
const SUFFIX_WITH_NAMES: &str = "withnames";
const SUFFIX_COMPACT: &str = "compact";
const SUFFIX_STRINGS: &str = "strings";
const SUFFIX_EACHROW: &str = "eachrow";

#[derive(Default, Clone)]
pub struct ClickhouseTypeSuffixJson {
    pub is_compact: bool,
    pub is_strings: bool,
    pub is_eachrow: bool,
}

#[derive(Default, Clone)]
pub struct ClickhouseSuffix {
    pub headers: usize,
    pub json: Option<ClickhouseTypeSuffixJson>,
}

#[derive(Default, Clone)]
pub struct ClickhouseFormatType {
    pub typ: StageFileFormatType,
    pub suffixes: ClickhouseSuffix,
}

fn try_remove_suffix<'a>(name: &'a str, suffix: &str) -> (&'a str, bool) {
    if name.ends_with(suffix) {
        (&name[0..(name.len() - suffix.len())], true)
    } else {
        (name, false)
    }
}

impl ClickhouseFormatType {
    pub fn parse_clickhouse_format(name: &str) -> Result<ClickhouseFormatType> {
        let lower = name.to_lowercase();

        let mut suffixes = ClickhouseSuffix::default();

        let (mut base, mut ok) = try_remove_suffix(&lower, SUFFIX_WITH_NAMES_AND_TYPES);
        if ok {
            suffixes.headers = 2;
        } else {
            (base, ok) = try_remove_suffix(base, SUFFIX_WITH_NAMES);
            if ok {
                suffixes.headers = 1;
            }
        }

        if base.starts_with("json") {
            let mut json = ClickhouseTypeSuffixJson::default();
            (base, json.is_eachrow) = try_remove_suffix(base, SUFFIX_EACHROW);
            (base, json.is_strings) = try_remove_suffix(base, SUFFIX_STRINGS);
            (base, json.is_compact) = try_remove_suffix(base, SUFFIX_COMPACT);
            if base != "json" {
                return Err(ErrorCode::UnknownFormat(name));
            } else {
                if !json.is_compact && suffixes.headers != 0 {
                    return Err(ErrorCode::UnknownFormat(name));
                }
                if json.is_eachrow {
                    base = "ndjson"
                }
                suffixes.json = Some(json);
            }
        }

        let format_type = StageFileFormatType::from_str(base).map_err(ErrorCode::UnknownFormat)?;

        Ok(ClickhouseFormatType {
            typ: format_type,
            suffixes,
        })
    }
}
