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

use bytes::Bytes;
use databend_common_base::base::OrderedFloat;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::GeometryType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::number::UInt64Type;
use databend_common_io::geometry::geometry_from_str;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_storages_fuse::io::SpatialIndexBuilder;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SpatialStatistics;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;

fn build_schema() -> TableSchemaRef {
    TableSchemaRefExt::create(vec![
        TableField::new("id", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new("g", TableDataType::Geometry),
    ])
}

fn build_block() -> Result<DataBlock> {
    let points = vec![
        geometry_from_str("SRID=4326;POINT(1 1)", None)?,
        geometry_from_str("SRID=4326;POINT(2 2)", None)?,
        geometry_from_str("SRID=4326;POINT(3 3)", None)?,
    ];
    Ok(DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![1_u64, 2, 3]),
        GeometryType::from_data(points),
    ]))
}

fn build_block_mixed_srid() -> Result<DataBlock> {
    let points = vec![
        geometry_from_str("SRID=4326;POINT(1 1)", None)?,
        geometry_from_str("SRID=3857;POINT(2 2)", None)?,
        geometry_from_str("SRID=4326;POINT(3 3)", None)?,
    ];
    Ok(DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![1_u64, 2, 3]),
        GeometryType::from_data(points),
    ]))
}

fn build_spatial_index(
    schema: TableSchemaRef,
    with_index: bool,
    block: DataBlock,
) -> Result<(Option<Vec<String>>, Option<SpatialStatistics>)> {
    let geom_column_id = schema.fields()[1].column_id();
    let mut table_indexes = BTreeMap::new();
    if with_index {
        let index_name = "spatial_idx".to_string();
        table_indexes.insert(index_name.clone(), TableIndex {
            index_type: TableIndexType::Spatial,
            name: index_name,
            column_ids: vec![geom_column_id],
            sync_creation: true,
            version: "v1".to_string(),
            options: BTreeMap::new(),
        });
    }

    let mut builder =
        SpatialIndexBuilder::try_create(&table_indexes, schema.clone(), true).unwrap();
    builder.add_block(&block)?;
    let location: Location = ("memory://spatial_index".to_string(), 0);
    let result = builder.finalize(&location)?;

    let stats = result
        .spatial_stats
        .as_ref()
        .and_then(|spatial_stats| spatial_stats.get(&geom_column_id))
        .cloned();

    let field_names = result.index_state.as_ref().map(|state| {
        let reader = SerializedFileReader::new(Bytes::from(state.data.clone())).unwrap();
        let metadata = reader.metadata().file_metadata();
        let schema = metadata.schema_descr();
        schema
            .root_schema()
            .get_fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>()
    });

    Ok((field_names, stats))
}

#[test]
fn test_spatial_index_builder_with_index() -> Result<()> {
    let schema = build_schema();
    let geom_column_id = schema.fields()[1].column_id();
    let block = build_block()?;
    let (field_names, stats) = build_spatial_index(schema, true, block)?;

    let field_names = field_names.unwrap();
    assert!(field_names.contains(&format!("{geom_column_id}")));
    assert!(field_names.contains(&format!("{geom_column_id}_srid")));
    let stats = stats.unwrap();
    assert_eq!(stats.min_x, OrderedFloat(1.0));
    assert_eq!(stats.max_x, OrderedFloat(3.0));
    assert_eq!(stats.srid, 4326);
    assert!(!stats.has_null);
    Ok(())
}

#[test]
fn test_spatial_index_builder_without_index() -> Result<()> {
    let schema = build_schema();
    let block = build_block()?;
    let (field_names, stats) = build_spatial_index(schema, false, block)?;

    assert!(field_names.is_none());
    let stats = stats.unwrap();
    assert_eq!(stats.min_x, OrderedFloat(1.0));
    assert_eq!(stats.max_x, OrderedFloat(3.0));
    assert_eq!(stats.srid, 4326);
    assert!(!stats.has_null);
    Ok(())
}

#[test]
fn test_spatial_index_builder_mixed_srid() -> Result<()> {
    let schema = build_schema();
    let block = build_block_mixed_srid()?;
    let (field_names, stats) = build_spatial_index(schema, true, block)?;

    assert!(field_names.is_none());
    assert!(stats.is_none());
    Ok(())
}
