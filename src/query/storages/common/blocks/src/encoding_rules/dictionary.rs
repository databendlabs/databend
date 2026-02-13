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

use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use parquet::file::properties::WriterPropertiesBuilder;

use crate::encoding_rules::ColumnPathsCache;

/// Disable dictionary encoding once the NDV-to-row ratio is greater than this threshold.
const HIGH_CARDINALITY_RATIO_THRESHOLD: f64 = 0.1;

pub fn apply_dictionary_high_cardinality_heuristic(
    mut builder: WriterPropertiesBuilder,
    column_stats: &StatisticsOfColumns,
    table_schema: &TableSchema,
    num_rows: usize,
    column_paths_cache: &mut ColumnPathsCache,
) -> WriterPropertiesBuilder {
    if num_rows == 0 {
        return builder;
    }
    let column_paths = column_paths_cache.get_or_build(table_schema);
    for (column_id, column_path) in column_paths.iter() {
        if let Some(stats) = column_stats.get(column_id) {
            if let Some(ndv) = stats.distinct_of_values {
                if (ndv as f64 / num_rows as f64) > HIGH_CARDINALITY_RATIO_THRESHOLD {
                    builder = builder.set_column_dictionary_enabled(column_path.clone(), false);
                }
            }
        }
    }
    builder
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_expression::ColumnId;
    use databend_common_expression::Scalar;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::converts::arrow::table_schema_arrow_leaf_paths;
    use databend_common_expression::types::number::NumberDataType;
    use databend_common_expression::types::number::NumberScalar;
    use databend_storages_common_table_meta::meta::ColumnStatistics;
    use databend_storages_common_table_meta::meta::StatisticsOfColumns;
    use databend_storages_common_table_meta::table::TableCompression;
    use parquet::schema::types::ColumnPath;

    fn make_test_stats(ndv_map: HashMap<ColumnId, u64>) -> StatisticsOfColumns {
        let mut stats = HashMap::new();
        for (column_id, ndv) in ndv_map {
            stats.insert(column_id, ColumnStatistics {
                min: Scalar::Number(NumberScalar::Int32(0)),
                max: Scalar::Number(NumberScalar::Int32(1000)),
                null_count: 0,
                in_memory_size: 0,
                distinct_of_values: Some(ndv),
            });
        }
        stats
    }

    fn sample_schema() -> TableSchema {
        TableSchema::new(vec![
            TableField::new("simple", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("nested", TableDataType::Tuple {
                fields_name: vec!["leaf".to_string(), "arr".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::Int64),
                    TableDataType::Array(Box::new(TableDataType::Number(NumberDataType::UInt64))),
                ],
            }),
            TableField::new("no_stats", TableDataType::String),
        ])
    }

    fn column_id(schema: &TableSchema, name: &str) -> ColumnId {
        schema
            .leaf_fields()
            .into_iter()
            .find(|field| field.name() == name)
            .unwrap_or_else(|| panic!("missing field {}", name))
            .column_id()
    }

    #[test]
    fn test_handles_nested_leaves() {
        let schema = sample_schema();

        let mut ndv = HashMap::new();
        ndv.insert(column_id(&schema, "simple"), 500);
        ndv.insert(column_id(&schema, "nested:leaf"), 50);
        ndv.insert(column_id(&schema, "nested:arr:0"), 400);

        let column_paths: HashMap<ColumnId, ColumnPath> = table_schema_arrow_leaf_paths(&schema)
            .into_iter()
            .map(|(id, path)| (id, ColumnPath::from(path)))
            .collect();

        let stats = make_test_stats(ndv);
        let props = crate::build_parquet_writer_properties(
            TableCompression::Zstd,
            true,
            false,
            Some(&stats),
            None,
            None,
            1000,
            &schema,
        );

        assert!(
            !props.dictionary_enabled(&column_paths[&column_id(&schema, "simple")]),
            "high cardinality top-level column should disable dictionary"
        );
        assert!(
            props.dictionary_enabled(&column_paths[&column_id(&schema, "nested:leaf")]),
            "low cardinality nested column should keep dictionary"
        );
        assert!(
            !props.dictionary_enabled(&column_paths[&column_id(&schema, "nested:arr:0")]),
            "high cardinality nested array element should disable dictionary"
        );
        assert!(
            props.dictionary_enabled(&column_paths[&column_id(&schema, "no_stats")]),
            "columns without NDV stats keep the default dictionary behavior"
        );
    }

    #[test]
    fn test_disabled_globally() {
        let schema = sample_schema();

        let column_paths: HashMap<ColumnId, ColumnPath> = table_schema_arrow_leaf_paths(&schema)
            .into_iter()
            .map(|(id, path)| (id, ColumnPath::from(path)))
            .collect();

        let props = crate::build_parquet_writer_properties(
            TableCompression::Zstd,
            false,
            false,
            None,
            None,
            None,
            1000,
            &schema,
        );

        for field in schema.leaf_fields() {
            assert!(
                !props.dictionary_enabled(&column_paths[&field.column_id()]),
                "dictionary must remain disabled when enable_dictionary is false",
            );
        }
    }
}
