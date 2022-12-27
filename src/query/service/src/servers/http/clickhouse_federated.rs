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

use common_expression::types::DataType;
use common_expression::utils::ColumnFrom;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRef;
use common_expression::TableSchemaRefExt;
use common_expression::Value;
use once_cell::sync::Lazy;
use regex::Regex;

use crate::servers::federated_helper::FederatedHelper;

const CLICKHOUSE_VERSION: &str = "8.12.14";

pub struct ClickHouseFederated {}

static FORMAT_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r".*(?i)FORMAT\s*([[:alpha:]]*)\s*;?$").unwrap());

impl ClickHouseFederated {
    // Build block for select function.
    // Format:
    // |function_name()|
    // |value|
    fn select_function_block(name: &str, value: &str) -> Option<(TableSchemaRef, Chunk)> {
        let schema = TableSchemaRefExt::create(vec![TableField::new(name, TableDataType::String)]);
        let chunk = Chunk::new_from_sequence(
            vec![(
                Value::Column(Column::from_data(vec![value.as_bytes().to_vec()])),
                DataType::String,
            )],
            1,
        );
        Some((schema, chunk))
    }

    pub fn get_format(query: &str) -> Option<String> {
        match FORMAT_REGEX.captures(query) {
            Some(x) => x.get(1).map(|s| s.as_str().to_owned()),
            None => None,
        }
    }

    pub fn check(query: &str) -> Option<(TableSchemaRef, Chunk)> {
        let rules: Vec<(&str, Option<(TableSchemaRef, Chunk)>)> = vec![(
            "(?i)^(SELECT VERSION()(.*))",
            Self::select_function_block("version()", CLICKHOUSE_VERSION),
        )];
        FederatedHelper::block_match_rule(query, rules)
    }
}
