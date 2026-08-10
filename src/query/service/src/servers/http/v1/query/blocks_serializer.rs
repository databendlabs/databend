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

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use arrow_array::Array;
use arrow_array::LargeListArray;
use arrow_array::LargeStringArray;
use arrow_array::MapArray;
use arrow_array::RecordBatch;
use arrow_array::StructArray;
use arrow_array::cast::AsArray;
use arrow_array::make_array;
use arrow_buffer::OffsetBuffer;
use arrow_buffer::ScalarBuffer;
use arrow_cast::cast;
use arrow_ipc::CompressionType;
use arrow_ipc::MetadataVersion;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Fields as ArrowFields;
use arrow_schema::Schema as ArrowSchema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::converts::arrow::ARROW_EXT_TYPE_VARIANT;
use databend_common_expression::converts::arrow::EXTENSION_KEY;
use databend_common_expression::types::binary::BinaryColumn;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::geography::GeographyColumn;
use databend_common_expression::types::geometry::extract_geometry_geo_and_srid;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_formats::field_encoder::FieldEncoderToString;
use databend_common_io::GEOGRAPHY_SRID;
use databend_common_io::GeometryDataType;
use databend_common_io::geo_to_ewkb;
use databend_common_io::geo_to_ewkt;
use databend_common_io::geo_to_json;
use databend_common_io::geo_to_wkb;
use databend_common_io::geo_to_wkt;
use databend_common_io::prelude::OutputFormatSettings;
use geo::Geometry;
use jsonb::RawJsonb;
use log::info;
use serde::ser::SerializeSeq;

fn data_is_null(column: &Column, row_index: usize) -> bool {
    match column {
        Column::Null { .. } => true,
        Column::Nullable(box inner) => !inner.validity.get_bit(row_index),
        _ => false,
    }
}

#[derive(Debug, Clone, Default)]
pub struct BlocksCollector {
    // Vec<Column> for a Block
    columns: Vec<(Vec<Column>, usize)>,
}

impl BlocksCollector {
    pub fn new() -> Self {
        Self { columns: vec![] }
    }

    pub fn append_columns(&mut self, columns: Vec<Column>, num_rows: usize) {
        self.columns.push((columns, num_rows));
    }

    pub fn append_block(&mut self, block: DataBlock) {
        if block.is_empty() {
            return;
        }
        let columns = block.columns().iter().map(BlockEntry::to_column).collect();
        let num_rows = block.num_rows();
        self.append_columns(columns, num_rows);
    }

    pub fn num_rows(&self) -> usize {
        self.columns.iter().map(|(_, num_rows)| *num_rows).sum()
    }

    pub fn into_serializer(self, format: OutputFormatSettings) -> BlocksSerializer {
        BlocksSerializer {
            columns: self.columns,
            format: Some(format),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BlocksSerializer {
    // Vec<Column> for a Block
    columns: Vec<(Vec<Column>, usize)>,
    format: Option<OutputFormatSettings>,
}

impl BlocksSerializer {
    pub fn empty() -> Self {
        Self {
            columns: vec![],
            format: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn num_rows(&self) -> usize {
        self.columns.iter().map(|(_, num_rows)| *num_rows).sum()
    }

    pub fn to_arrow_ipc(
        &self,
        data_schema: &DataSchema,
        ext_meta: Vec<(String, String)>,
    ) -> Result<Vec<u8>> {
        let mut schema = build_arrow_ipc_schema(data_schema, self.format.as_ref());
        schema.metadata.extend(ext_meta);
        let schema = Arc::new(schema);

        let mut buf = Vec::new();
        let opts = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?
            .try_with_compression(Some(CompressionType::LZ4_FRAME))?;
        let mut writer = StreamWriter::try_new_with_options(&mut buf, schema.as_ref(), opts)?;

        for (columns, num_rows) in &self.columns {
            let batch =
                format_arrow_ipc_batch(columns, *num_rows, schema.clone(), self.format.as_ref())?;
            writer.write(&batch)?;
        }
        writer.finish()?;
        Ok(buf)
    }
}

fn build_arrow_ipc_schema(
    data_schema: &DataSchema,
    format: Option<&OutputFormatSettings>,
) -> arrow_schema::Schema {
    let schema = arrow_schema::Schema::from(data_schema);
    rewrite_arrow_ipc_schema(&schema, format)
}

fn rewrite_arrow_ipc_schema(
    schema: &arrow_schema::Schema,
    format: Option<&OutputFormatSettings>,
) -> arrow_schema::Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| Arc::new(rewrite_arrow_ipc_field(field.as_ref(), format)))
        .collect::<Vec<_>>();
    arrow_schema::Schema::new_with_metadata(ArrowFields::from(fields), schema.metadata.clone())
}

fn rewrite_arrow_ipc_field(
    field: &ArrowField,
    format: Option<&OutputFormatSettings>,
) -> ArrowField {
    let is_variant_json_string = field
        .metadata()
        .get(EXTENSION_KEY)
        .is_some_and(|ext| ext == ARROW_EXT_TYPE_VARIANT)
        && format.is_some_and(|format| !format.http_arrow_use_jsonb);

    let data_type = if is_variant_json_string {
        ArrowDataType::LargeUtf8
    } else {
        rewrite_arrow_ipc_data_type(field.data_type(), format)
    };

    ArrowField::new(field.name(), data_type, field.is_nullable())
        .with_metadata(field.metadata().clone())
}

fn rewrite_arrow_ipc_data_type(
    data_type: &ArrowDataType,
    format: Option<&OutputFormatSettings>,
) -> ArrowDataType {
    match data_type {
        ArrowDataType::Decimal64(precision, scale)
            if format.is_some_and(|format| !format.http_arrow_use_decimal64) =>
        {
            ArrowDataType::Decimal128(*precision, *scale)
        }
        ArrowDataType::Struct(fields) => ArrowDataType::Struct(ArrowFields::from(
            fields
                .iter()
                .map(|field| Arc::new(rewrite_arrow_ipc_field(field.as_ref(), format)))
                .collect::<Vec<_>>(),
        )),
        ArrowDataType::List(field) => {
            ArrowDataType::List(Arc::new(rewrite_arrow_ipc_field(field.as_ref(), format)))
        }
        ArrowDataType::LargeList(field) => {
            ArrowDataType::LargeList(Arc::new(rewrite_arrow_ipc_field(field.as_ref(), format)))
        }
        ArrowDataType::FixedSizeList(field, size) => ArrowDataType::FixedSizeList(
            Arc::new(rewrite_arrow_ipc_field(field.as_ref(), format)),
            *size,
        ),
        ArrowDataType::Map(field, ordered) => ArrowDataType::Map(
            Arc::new(rewrite_arrow_ipc_field(field.as_ref(), format)),
            *ordered,
        ),
        other => other.clone(),
    }
}

fn format_arrow_ipc_batch(
    columns: &[Column],
    num_rows: usize,
    target_schema: Arc<ArrowSchema>,
    format: Option<&OutputFormatSettings>,
) -> Result<RecordBatch> {
    if columns.len() != target_schema.fields.len() {
        return Err(ErrorCode::Internal(format!(
            "The number of columns in the data block does not match the number of fields in the table schema, block_columns: {}, table_schema_fields: {}",
            columns.len(),
            target_schema.fields.len(),
        )));
    }

    if target_schema.fields.is_empty() {
        return Ok(RecordBatch::try_new_with_options(
            Arc::new(ArrowSchema::empty()),
            vec![],
            &arrow_array::RecordBatchOptions::default().with_row_count(Some(num_rows)),
        )?);
    }

    let arrays = columns
        .iter()
        .zip(target_schema.fields())
        .map(|(column, field)| format_arrow_ipc_array(column, field.as_ref(), format))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(target_schema, arrays)?)
}

fn format_arrow_ipc_array(
    column: &Column,
    target_field: &ArrowField,
    format: Option<&OutputFormatSettings>,
) -> Result<Arc<dyn Array>> {
    match (column, target_field.data_type()) {
        (Column::Tuple(fields), ArrowDataType::Struct(target_fields)) => {
            let inner_arrays = fields
                .iter()
                .zip(target_fields.iter())
                .map(|(field, target_field)| {
                    format_arrow_ipc_array(field, target_field.as_ref(), format)
                })
                .collect::<Result<Vec<_>>>()?;
            let array = StructArray::new(target_fields.clone(), inner_arrays, None);
            Ok(Arc::new(array) as _)
        }
        (Column::Array(column), ArrowDataType::LargeList(target_field)) => {
            let values =
                format_arrow_ipc_array(&column.underlying_column(), target_field.as_ref(), format)?;
            let offsets = OffsetBuffer::new(ScalarBuffer::from(
                column
                    .underlying_offsets()
                    .into_iter()
                    .map(|v| v as i64)
                    .collect::<Vec<_>>(),
            ));
            let array = LargeListArray::new(target_field.clone(), offsets, values, None);
            Ok(Arc::new(array) as _)
        }
        (Column::Map(column), ArrowDataType::Map(target_field, ordered)) => {
            let entry =
                format_arrow_ipc_array(&column.underlying_column(), target_field.as_ref(), format)?;
            let offsets = OffsetBuffer::new(ScalarBuffer::from(
                column
                    .underlying_offsets()
                    .into_iter()
                    .map(|v| v as i32)
                    .collect::<Vec<_>>(),
            ));
            let array = MapArray::new(
                target_field.clone(),
                offsets,
                entry.as_struct().clone(),
                None,
                *ordered,
            );
            Ok(Arc::new(array) as _)
        }
        (Column::Nullable(column), _) => {
            format_nullable_arrow_ipc_array(column, target_field, format)
        }
        _ => format_arrow_ipc_leaf_array(column, target_field, format),
    }
}

fn format_nullable_arrow_ipc_array(
    column: &NullableColumn<databend_common_expression::types::AnyType>,
    target_field: &ArrowField,
    format: Option<&OutputFormatSettings>,
) -> Result<Arc<dyn Array>> {
    if let Column::Variant(inner) = &column.column
        && is_variant_json_string_field(target_field, format)
    {
        return Ok(Arc::new(format_nullable_variant_string_array(
            inner,
            &column.validity,
        )) as _);
    }

    let array = format_arrow_ipc_array(&column.column, target_field, format)?;
    apply_arrow_ipc_validity(array, &column.validity)
}

fn format_arrow_ipc_leaf_array(
    column: &Column,
    target_field: &ArrowField,
    format: Option<&OutputFormatSettings>,
) -> Result<Arc<dyn Array>> {
    let array = match (column, target_field.data_type(), format) {
        (Column::Variant(column), ArrowDataType::LargeUtf8, format)
            if is_variant_json_string_field(target_field, format) =>
        {
            Arc::new(format_variant_string_array(column)) as _
        }
        (Column::Geometry(column), _, Some(format)) => {
            Column::Geometry(format_geometry_column(column, format.geometry_format)?)
                .maybe_gc()
                .into_arrow_rs()
        }
        (Column::Geography(column), _, Some(format)) => {
            Column::Geography(format_geography_column(column, format.geometry_format)?)
                .maybe_gc()
                .into_arrow_rs()
        }
        _ => column.clone().maybe_gc().into_arrow_rs(),
    };

    if array.data_type() == target_field.data_type() {
        return Ok(array);
    }

    match (array.data_type(), target_field.data_type()) {
        (
            ArrowDataType::Decimal64(actual_precision, actual_scale),
            ArrowDataType::Decimal128(target_precision, target_scale),
        ) if actual_precision == target_precision && actual_scale == target_scale => {
            cast(array.as_ref(), target_field.data_type()).map_err(ErrorCode::from)
        }
        _ => Err(ErrorCode::Internal(format!(
            "unexpected HTTP Arrow IPC type rewrite mismatch: actual={:?}, target={:?}",
            array.data_type(),
            target_field.data_type()
        ))),
    }
}

fn is_variant_json_string_field(
    target_field: &ArrowField,
    format: Option<&OutputFormatSettings>,
) -> bool {
    matches!(target_field.data_type(), ArrowDataType::LargeUtf8)
        && target_field
            .metadata()
            .get(EXTENSION_KEY)
            .is_some_and(|ext| ext == ARROW_EXT_TYPE_VARIANT)
        && format.is_some_and(|format| !format.http_arrow_use_jsonb)
}

fn apply_arrow_ipc_validity(
    array: Arc<dyn Array>,
    validity: &databend_common_column::bitmap::Bitmap,
) -> Result<Arc<dyn Array>> {
    let builder = array.to_data().into_builder();
    let nulls = validity.clone().into();
    let data = unsafe { builder.nulls(Some(nulls)).build_unchecked() };
    Ok(make_array(data))
}

fn format_geometry_column(
    column: &BinaryColumn,
    geometry_format: GeometryDataType,
) -> Result<BinaryColumn> {
    format_binary_column(
        column.len(),
        column.total_bytes_len(),
        column.iter(),
        |value| match extract_geometry_geo_and_srid(value) {
            Ok((geo, srid)) => format_decoded_geo(geo, Some(srid), geometry_format),
            Err(_) => Ok(lossy_geo_value(value, geometry_format)),
        },
    )
}

fn format_geography_column(
    column: &GeographyColumn,
    geometry_format: GeometryDataType,
) -> Result<GeographyColumn> {
    Ok(GeographyColumn(format_binary_column(
        column.len(),
        column.0.total_bytes_len(),
        column.iter(),
        |value| {
            value
                .to_geo()
                .and_then(|geo| format_decoded_geo(geo, Some(GEOGRAPHY_SRID), geometry_format))
                .or_else(|_| Ok(lossy_geo_value(value.0, geometry_format)))
        },
    )?))
}

fn format_variant_string_array(column: &BinaryColumn) -> LargeStringArray {
    LargeStringArray::from_iter_values(column.iter().map(format_variant_string_value))
}

fn format_nullable_variant_string_array(
    column: &BinaryColumn,
    validity: &databend_common_column::bitmap::Bitmap,
) -> LargeStringArray {
    LargeStringArray::from_iter(
        validity
            .iter()
            .zip(column.iter())
            .map(|(is_valid, value)| is_valid.then(|| format_variant_string_value(value))),
    )
}

fn format_variant_string_value(value: &[u8]) -> String {
    RawJsonb::new(value).to_string()
}

fn format_binary_column<T>(
    len: usize,
    data_len: usize,
    values: impl IntoIterator<Item = T>,
    mut format_value: impl FnMut(T) -> Result<Vec<u8>>,
) -> Result<BinaryColumn> {
    let mut builder = BinaryColumnBuilder::with_capacity(len, data_len);
    for value in values {
        builder.put_slice(&format_value(value)?);
        builder.commit_row();
    }
    Ok(builder.build())
}

fn format_decoded_geo(
    geo: Geometry<f64>,
    srid: Option<i32>,
    geometry_format: GeometryDataType,
) -> Result<Vec<u8>> {
    match geometry_format {
        GeometryDataType::WKB => geo_to_wkb(geo),
        GeometryDataType::WKT => Ok(geo_to_wkt(geo)?.into_bytes()),
        GeometryDataType::EWKB => geo_to_ewkb(geo, srid),
        GeometryDataType::EWKT => Ok(geo_to_ewkt(geo, srid)?.into_bytes()),
        GeometryDataType::GEOJSON => Ok(geo_to_json(geo)?.into_bytes()),
    }
}

fn lossy_geo_value(value: &[u8], geometry_format: GeometryDataType) -> Vec<u8> {
    match geometry_format {
        GeometryDataType::WKB | GeometryDataType::EWKB => value.to_vec(),
        GeometryDataType::WKT | GeometryDataType::EWKT | GeometryDataType::GEOJSON => {
            String::from_utf8_lossy(value).into_owned().into_bytes()
        }
    }
}

impl serde::Serialize for BlocksSerializer {
    fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut serialize_seq = serializer.serialize_seq(Some(self.num_rows()))?;
        if let Some(format) = &self.format {
            let start = Instant::now();
            let encoder = FieldEncoderToString::create(format);
            for (columns, num_rows) in self.columns.iter() {
                for i in 0..*num_rows {
                    serialize_seq.serialize_element(&RowSerializer {
                        format,
                        data_block: columns,
                        encoder: &encoder,
                        row_index: i,
                    })?
                }
            }
            let duration = Instant::now().duration_since(start);
            if duration >= Duration::from_secs(3) {
                info!(
                    "[SLOW] http handler serialize {} rows using {} secs",
                    self.num_rows(),
                    duration.as_secs_f64()
                );
            }
        }
        serialize_seq.end()
    }
}

struct RowSerializer<'a> {
    format: &'a OutputFormatSettings,
    data_block: &'a [Column],
    encoder: &'a FieldEncoderToString,
    row_index: usize,
}

impl serde::Serialize for RowSerializer<'_> {
    fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut serialize_seq = serializer.serialize_seq(Some(self.data_block.len()))?;

        for column in self.data_block.iter() {
            if !self.format.format_null_as_str && data_is_null(column, self.row_index) {
                serialize_seq.serialize_element(&None::<String>)?;
                continue;
            }
            let string = self
                .encoder
                .encode(column, self.row_index)
                .map_err(serde::ser::Error::custom)?;
            serialize_seq.serialize_element(&string)?;
        }
        serialize_seq.end()
    }
}
