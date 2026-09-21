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
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use paimon::Catalog;
use paimon::catalog::Identifier;
use paimon::catalog::list_partitions_from_file_system;

use super::format_partition_spec;
use super::json;
use super::millis_to_micros;
use crate::error::map_paimon_result;

pub async fn read(
    catalog: Arc<dyn Catalog>,
    identifier: &Identifier,
    table: &paimon::Table,
) -> Result<DataBlock> {
    // Use the catalog's own listing so metastore-backed catalogs (e.g. REST)
    // carry through audit fields (created_by/updated_by/options); the filesystem
    // catalog falls back to deriving them from the manifest tree.
    let partitions = if table.identifier() == identifier {
        map_paimon_result(catalog.list_partitions(identifier).await)?
    } else {
        map_paimon_result(list_partitions_from_file_system(table).await)?
    };
    let partition_keys = table.schema().partition_keys().to_vec();

    let mut partition = Vec::with_capacity(partitions.len());
    let mut record_count = Vec::with_capacity(partitions.len());
    let mut file_size = Vec::with_capacity(partitions.len());
    let mut file_count = Vec::with_capacity(partitions.len());
    let mut last_update_time = Vec::with_capacity(partitions.len());
    let mut created_at = Vec::with_capacity(partitions.len());
    let mut created_by = Vec::with_capacity(partitions.len());
    let mut updated_by = Vec::with_capacity(partitions.len());
    let mut options = Vec::with_capacity(partitions.len());
    let mut total_buckets = Vec::with_capacity(partitions.len());
    let mut done = Vec::with_capacity(partitions.len());

    for part in &partitions {
        partition.push(format_partition_spec(&part.spec, &partition_keys));
        record_count.push(part.record_count);
        file_size.push(part.file_size_in_bytes);
        file_count.push(part.file_count);
        last_update_time.push(optional_millis(part.last_file_creation_time)?);
        created_at.push(
            part.created_at
                .map(|ms| millis_to_micros(ms, "partition created_at"))
                .transpose()?,
        );
        created_by.push(part.created_by.clone());
        updated_by.push(part.updated_by.clone());
        options.push(match &part.options {
            Some(map) => {
                let sorted: BTreeMap<_, _> = map.iter().collect();
                Some(json(&sorted, "partition options")?)
            }
            None => None,
        });
        total_buckets.push(part.total_buckets);
        done.push(part.done);
    }

    Ok(DataBlock::new_from_columns(vec![
        StringType::from_opt_data(partition),
        Int64Type::from_data(record_count),
        Int64Type::from_data(file_size),
        Int64Type::from_data(file_count),
        TimestampType::from_opt_data(last_update_time),
        TimestampType::from_opt_data(created_at),
        StringType::from_opt_data(created_by),
        StringType::from_opt_data(updated_by),
        StringType::from_opt_data(options),
        Int32Type::from_data(total_buckets),
        BooleanType::from_data(done),
    ]))
}

/// Treats a zero epoch (no data files yet) as null, otherwise converts millis to
/// Databend microseconds.
fn optional_millis(millis: i64) -> Result<Option<i64>> {
    if millis == 0 {
        Ok(None)
    } else {
        Ok(Some(millis_to_micros(
            millis,
            "partition last_update_time",
        )?))
    }
}
