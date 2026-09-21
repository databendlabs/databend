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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;

use super::json;
use super::millis_to_micros;
use crate::error::map_paimon_result;

pub async fn read(table: &paimon::Table) -> Result<DataBlock> {
    let schemas = map_paimon_result(table.schema_manager().list_all().await)?;
    let fields = schemas
        .iter()
        .map(|schema| json(schema.fields(), "schema fields"))
        .collect::<Result<Vec<_>>>()?;
    let partition_keys = schemas
        .iter()
        .map(|schema| json(schema.partition_keys(), "schema partition keys"))
        .collect::<Result<Vec<_>>>()?;
    let primary_keys = schemas
        .iter()
        .map(|schema| json(schema.primary_keys(), "schema primary keys"))
        .collect::<Result<Vec<_>>>()?;
    let options = schemas
        .iter()
        .map(|schema| {
            let sorted: BTreeMap<_, _> = schema.options().iter().collect();
            json(&sorted, "schema options")
        })
        .collect::<Result<Vec<_>>>()?;
    let update_times = schemas
        .iter()
        .map(|schema| millis_to_micros(schema.time_millis(), "schema update"))
        .collect::<Result<Vec<_>>>()?;

    Ok(DataBlock::new_from_columns(vec![
        Int64Type::from_data(schemas.iter().map(|v| v.id()).collect()),
        StringType::from_data(fields),
        StringType::from_data(partition_keys),
        StringType::from_data(primary_keys),
        StringType::from_data(options),
        StringType::from_opt_data(schemas.iter().map(|v| v.comment()).collect()),
        TimestampType::from_data(update_times),
    ]))
}
