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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use databend_common_expression::ColumnId;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use parquet::arrow::ArrowSchemaConverter;
use parquet::file::FOOTER_SIZE;
use parquet::file::metadata::FileMetaData;
use parquet::file::metadata::FooterTail;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::metadata::ParquetMetaDataWriter;
use parquet::schema::types::SchemaDescPtr;
use serde::Deserialize;
use serde::Serialize;

use crate::plan::datasource::datasource_info::parquet_read_options::ParquetReadOptions;

#[derive(Clone, Debug)]
pub struct FullParquetMeta {
    pub location: String,
    pub size: u64,

    pub meta: Arc<ParquetMetaData>,
    /// Row group level statistics.
    ///
    /// We collect the statistics here to avoid multiple computations of the same parquet meta.
    ///
    /// The container is organized as:
    /// - row_group_level_stats[i][j] is the statistics of the j-th column in the i-th row group of current file.
    pub row_group_level_stats: Option<Vec<HashMap<ColumnId, ColumnStatistics>>>,
}

#[derive(Clone, Debug)]
pub struct ParquetTableInfo {
    pub read_options: ParquetReadOptions,
    pub stage_info: StageInfo,
    pub files_info: StageFilesInfo,

    pub table_info: TableInfo,
    pub arrow_schema: ArrowSchema,
    pub schema_descr: SchemaDescPtr,
    pub files_to_read: Option<Vec<StageFileInfo>>,
    pub schema_from: String,
    pub compression_ratio: f64,
    pub leaf_fields: Arc<Vec<TableField>>,

    pub need_stats_provider: bool,
    pub max_threads: usize,
    pub max_memory_usage: u64,
}

impl ParquetTableInfo {
    pub fn schema(&self) -> Arc<TableSchema> {
        self.table_info.schema()
    }

    pub fn desc(&self) -> String {
        self.stage_info.stage_name.clone()
    }
}

#[derive(Serialize, Deserialize)]
struct ParquetTableInfoSerde {
    read_options: ParquetReadOptions,
    stage_info: StageInfo,
    files_info: StageFilesInfo,
    table_info: TableInfo,
    arrow_schema: ArrowSchema,
    schema_descr_bytes: Vec<u8>,
    schema_descr_root: String,
    files_to_read: Option<Vec<StageFileInfo>>,
    schema_from: String,
    compression_ratio: f64,
    leaf_fields: Arc<Vec<TableField>>,
}

impl Serialize for ParquetTableInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let schema_descr_bytes =
            schema_to_bytes(&self.schema_descr).map_err(serde::ser::Error::custom)?;
        ParquetTableInfoSerde {
            read_options: self.read_options,
            stage_info: self.stage_info.clone(),
            files_info: self.files_info.clone(),
            table_info: self.table_info.clone(),
            arrow_schema: self.arrow_schema.clone(),
            schema_descr_bytes,
            schema_descr_root: self.schema_descr.root_schema().name().to_string(),
            files_to_read: self.files_to_read.clone(),
            schema_from: self.schema_from.clone(),
            compression_ratio: self.compression_ratio,
            leaf_fields: self.leaf_fields.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ParquetTableInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let helper = ParquetTableInfoSerde::deserialize(deserializer)?;
        let schema_descr = match schema_from_bytes(&helper.schema_descr_bytes) {
            Ok(schema_descr) => Ok(schema_descr),
            Err(error) => {
                log::warn!(
                    "Failed to decode serialized Parquet schema metadata, falling back to the Arrow schema: {error}"
                );
                ArrowSchemaConverter::new()
                    .schema_root(&helper.schema_descr_root)
                    .convert(&helper.arrow_schema)
                    .map(Arc::new)
            }
        };
        let schema_descr = schema_descr.map_err(|e| serde::de::Error::custom(e.to_string()))?;

        Ok(Self {
            read_options: helper.read_options,
            stage_info: helper.stage_info,
            files_info: helper.files_info,
            table_info: helper.table_info,
            arrow_schema: helper.arrow_schema,
            schema_descr,
            files_to_read: helper.files_to_read,
            schema_from: helper.schema_from,
            compression_ratio: helper.compression_ratio,
            leaf_fields: helper.leaf_fields,
            need_stats_provider: false,
            max_threads: 0,
            max_memory_usage: 0,
        })
    }
}

fn schema_from_bytes(bytes: &[u8]) -> parquet::errors::Result<SchemaDescPtr> {
    ParquetMetaDataReader::decode_schema(bytes)
}

fn schema_to_bytes(schema: &SchemaDescPtr) -> parquet::errors::Result<Vec<u8>> {
    let file_metadata = FileMetaData::new(1, 0, None, None, schema.clone(), None);
    let metadata = ParquetMetaData::new(file_metadata, vec![]);
    let mut out = Vec::new();
    ParquetMetaDataWriter::new(&mut out, &metadata).finish()?;
    let footer_offset = out.len().checked_sub(FOOTER_SIZE).ok_or_else(|| {
        parquet::errors::ParquetError::General("Parquet metadata footer is missing".to_string())
    })?;
    let metadata_len = FooterTail::try_from(&out[footer_offset..])?.metadata_length();
    if metadata_len != footer_offset {
        return Err(parquet::errors::ParquetError::General(format!(
            "Invalid Parquet metadata length: expected {footer_offset}, got {metadata_len}"
        )));
    }
    out.truncate(footer_offset);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Schema as ArrowSchema;
    use databend_common_storage::StageFilesInfo;
    use parquet::arrow::parquet_to_arrow_schema;
    use parquet::basic::ConvertedType;
    use parquet::basic::Repetition;
    use parquet::basic::Type as PhysicalType;
    use parquet::errors::ParquetError;
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescPtr;
    use parquet::schema::types::SchemaDescriptor;
    use parquet::schema::types::Type;

    use super::ParquetTableInfo;
    use super::schema_to_bytes;

    fn make_desc() -> Result<SchemaDescPtr, ParquetError> {
        let mut fields = vec![];

        let inta = Type::primitive_type_builder("a", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::INT_32)
            .build()?;
        fields.push(Arc::new(inta));
        let intb = Type::primitive_type_builder("b", PhysicalType::INT64)
            .with_converted_type(ConvertedType::INT_64)
            .build()?;
        fields.push(Arc::new(intb));
        let intc = Type::primitive_type_builder("c", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::UTF8)
            .build()?;
        fields.push(Arc::new(intc));

        // 3-level list encoding
        let item1 = Type::primitive_type_builder("item1", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::INT_64)
            .build()?;
        let item2 = Type::primitive_type_builder("item2", PhysicalType::BOOLEAN).build()?;
        let item3 = Type::primitive_type_builder("item3", PhysicalType::INT32)
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::INT_32)
            .build()?;
        let list = Type::group_type_builder("records")
            .with_repetition(Repetition::REPEATED)
            .with_converted_type(ConvertedType::LIST)
            .with_fields(vec![Arc::new(item1), Arc::new(item2), Arc::new(item3)])
            .build()?;
        let bag = Type::group_type_builder("bag")
            .with_repetition(Repetition::OPTIONAL)
            .with_fields(vec![Arc::new(list)])
            .build()?;
        fields.push(Arc::new(bag));

        let schema = Type::group_type_builder("schema")
            .with_fields(fields)
            .build()?;
        Ok(Arc::new(SchemaDescriptor::new(Arc::new(schema))))
    }

    fn make_arrow_compatible_desc() -> Result<SchemaDescPtr, ParquetError> {
        let schema = parse_message_type(
            "
            message stage_file {
              OPTIONAL INT64 number (UINT_64);
            }
            ",
        )?;
        Ok(Arc::new(SchemaDescriptor::new(Arc::new(schema))))
    }

    #[test]
    fn test_serde() {
        let schema_descr = make_desc().unwrap();
        let info = ParquetTableInfo {
            schema_descr: schema_descr.clone(),
            read_options: Default::default(),
            stage_info: Default::default(),
            files_info: StageFilesInfo {
                path: "".to_string(),
                files: None,
                pattern: None,
            },
            table_info: Default::default(),
            leaf_fields: Arc::new(vec![]),
            arrow_schema: ArrowSchema {
                fields: Default::default(),
                metadata: Default::default(),
            },
            files_to_read: None,
            schema_from: "".to_string(),
            compression_ratio: 0.0,
            need_stats_provider: false,
            max_threads: 1,
            max_memory_usage: 10000,
        };
        let s = serde_json::to_string(&info).unwrap();
        let info = serde_json::from_str::<ParquetTableInfo>(&s).unwrap();

        assert_eq!(schema_descr.root_schema(), info.schema_descr.root_schema());
    }

    #[test]
    fn test_serde_preserves_legacy_decimal_annotation() {
        let deal = Type::primitive_type_builder("deal", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_repetition(Repetition::OPTIONAL)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_length(9)
            .with_precision(20)
            .with_scale(0)
            .build()
            .unwrap();
        let nested = Type::group_type_builder("nested")
            .with_repetition(Repetition::OPTIONAL)
            .with_fields(vec![Arc::new(deal)])
            .build()
            .unwrap();
        let schema = Type::group_type_builder("spark_schema")
            .with_fields(vec![Arc::new(nested)])
            .build()
            .unwrap();
        let schema_descr = Arc::new(SchemaDescriptor::new(Arc::new(schema)));
        assert!(!schema_to_bytes(&schema_descr).unwrap().ends_with(b"PAR1"));
        assert!(
            schema_descr
                .column(0)
                .self_type()
                .get_basic_info()
                .logical_type_ref()
                .is_none()
        );

        let arrow_schema = parquet_to_arrow_schema(&schema_descr, None).unwrap();
        let info = ParquetTableInfo {
            schema_descr: schema_descr.clone(),
            read_options: Default::default(),
            stage_info: Default::default(),
            files_info: StageFilesInfo {
                path: "".to_string(),
                files: None,
                pattern: None,
            },
            table_info: Default::default(),
            leaf_fields: Arc::new(vec![]),
            arrow_schema,
            files_to_read: None,
            schema_from: "".to_string(),
            compression_ratio: 0.0,
            need_stats_provider: false,
            max_threads: 1,
            max_memory_usage: 10000,
        };

        let serialized = serde_json::to_string(&info).unwrap();
        let deserialized = serde_json::from_str::<ParquetTableInfo>(&serialized).unwrap();

        assert_eq!(
            schema_descr.root_schema(),
            deserialized.schema_descr.root_schema()
        );
        assert!(
            deserialized
                .schema_descr
                .column(0)
                .self_type()
                .get_basic_info()
                .logical_type_ref()
                .is_none()
        );
    }

    #[test]
    fn test_serde_falls_back_to_arrow_schema() {
        let schema_descr = make_arrow_compatible_desc().unwrap();
        let arrow_schema = parquet_to_arrow_schema(&schema_descr, None).unwrap();
        let info = ParquetTableInfo {
            schema_descr: schema_descr.clone(),
            read_options: Default::default(),
            stage_info: Default::default(),
            files_info: StageFilesInfo {
                path: "".to_string(),
                files: None,
                pattern: None,
            },
            table_info: Default::default(),
            leaf_fields: Arc::new(vec![]),
            arrow_schema,
            files_to_read: None,
            schema_from: "".to_string(),
            compression_ratio: 0.0,
            need_stats_provider: false,
            max_threads: 1,
            max_memory_usage: 10000,
        };

        let mut json = serde_json::to_value(&info).unwrap();
        json["schema_descr_bytes"] = serde_json::json!(Vec::<u8>::from("invalid schema"));

        let info = serde_json::from_value::<ParquetTableInfo>(json).unwrap();

        assert_eq!(
            schema_descr.root_schema().name(),
            info.schema_descr.root_schema().name()
        );
        assert_eq!(schema_descr.num_columns(), info.schema_descr.num_columns());
        assert_eq!(
            schema_descr.column(0).name(),
            info.schema_descr.column(0).name()
        );
        assert_eq!(
            schema_descr.column(0).physical_type(),
            info.schema_descr.column(0).physical_type()
        );
    }
}
