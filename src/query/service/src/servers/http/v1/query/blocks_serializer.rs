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

use std::cell::RefCell;
use std::ops::DerefMut;

use arrow_ipc::writer::StreamWriter;
use databend_common_exception::Result;
use databend_common_expression::types::date::date_to_string;
use databend_common_expression::types::interval::interval_to_string;
use databend_common_expression::types::timestamp::timestamp_to_string;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_formats::field_encoder::FieldEncoderValues;
use databend_common_io::ewkb_to_geo;
use databend_common_io::geo_to_ewkb;
use databend_common_io::geo_to_ewkt;
use databend_common_io::geo_to_json;
use databend_common_io::geo_to_wkb;
use databend_common_io::geo_to_wkt;
use databend_common_io::prelude::FormatSettings;
use databend_common_io::GeometryDataType;
use geozero::wkb::Ewkb;
use jsonb::RawJsonb;
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

    pub fn into_serializer(self, format: FormatSettings) -> BlocksSerializer {
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
    format: Option<FormatSettings>,
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

    pub fn to_arrow_ipc(&self, schema: arrow_schema::Schema) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &schema)?;

        let mut data_schema = None;
        for (block, _) in &self.columns {
            let block = DataBlock::new_from_columns(block.clone());
            let data_schema = data_schema.get_or_insert_with(|| block.infer_schema());
            let batch = block.to_record_batch_with_dataschema(data_schema)?;
            writer.write(&batch)?;
        }
        writer.finish()?;
        Ok(buf)
    }
}

impl serde::Serialize for BlocksSerializer {
    fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut serialize_seq = serializer.serialize_seq(Some(self.num_rows()))?;
        if let Some(format) = &self.format {
            let mut buf = RefCell::new(Vec::new());
            let encoder = FieldEncoderValues::create_for_http_handler(
                format.jiff_timezone.clone(),
                format.timezone,
                format.geometry_format,
            );
            for (columns, num_rows) in self.columns.iter() {
                for i in 0..*num_rows {
                    serialize_seq.serialize_element(&RowSerializer {
                        format,
                        data_block: columns,
                        encoder: &encoder,
                        buf: &mut buf,
                        row_index: i,
                    })?
                }
            }
        }
        serialize_seq.end()
    }
}

struct RowSerializer<'a> {
    format: &'a FormatSettings,
    data_block: &'a [Column],
    encoder: &'a FieldEncoderValues,
    buf: &'a RefCell<Vec<u8>>,
    row_index: usize,
}

impl RowSerializer<'_> {
    // when column is a Variant, value will have a meaningless conversion overhead:
    // `RawJsonb::to_string()` -> `as_bytes` -> `String::from_utf8_lossy`.
    // therefore, in this case where only strings are needed, Variant is specially processed to avoid the overhead of intermediate conversion.
    fn try_direct_as_string(&self, column: &Column) -> Option<String> {
        match column {
            Column::Variant(c) => {
                let v = unsafe { c.index_unchecked(self.row_index) };
                Some(RawJsonb::new(v).to_string())
            }
            Column::Decimal(c) => Some(c.index(self.row_index).unwrap().to_string()),
            Column::Binary(c) => {
                let v = unsafe { c.index_unchecked(self.row_index) };
                Some(hex::encode_upper(v))
            }
            Column::String(c) => Some(unsafe { c.index_unchecked(self.row_index).to_string() }),
            Column::Date(b) => {
                let v = unsafe { b.get_unchecked(self.row_index) };
                Some(date_to_string(*v as i64, &self.format.jiff_timezone).to_string())
            }
            Column::Timestamp(b) => {
                let v = unsafe { b.get_unchecked(self.row_index) };
                Some(timestamp_to_string(*v, &self.format.jiff_timezone).to_string())
            }
            Column::Interval(b) => {
                let v = unsafe { b.get_unchecked(self.row_index) };
                Some(interval_to_string(v).to_string())
            }
            Column::Bitmap(_) => Some("<bitmap binary>".to_string()),
            Column::Geometry(c) => {
                let v = unsafe { c.index_unchecked(self.row_index) };

                match (&self.format.geometry_format, ewkb_to_geo(&mut Ewkb(v))) {
                    (GeometryDataType::WKB, Ok((geo, _))) => {
                        geo_to_wkb(geo).map(hex::encode_upper).ok()
                    }
                    (GeometryDataType::WKT, Ok((geo, _))) => geo_to_wkt(geo).ok(),
                    (GeometryDataType::EWKB, Ok((geo, srid))) => {
                        geo_to_ewkb(geo, srid).map(hex::encode_upper).ok()
                    }
                    (GeometryDataType::EWKT, Ok((geo, srid))) => geo_to_ewkt(geo, srid).ok(),
                    (GeometryDataType::GEOJSON, Ok((geo, _))) => geo_to_json(geo).ok(),
                    (_, Err(_)) => None,
                }
            }
            Column::Geography(c) => {
                let v = unsafe { c.index_unchecked(self.row_index) };

                match (&self.format.geometry_format, ewkb_to_geo(&mut Ewkb(v.0))) {
                    (GeometryDataType::WKB, Ok((geo, _))) => {
                        geo_to_wkb(geo).map(hex::encode_upper).ok()
                    }
                    (GeometryDataType::WKT, Ok((geo, _))) => geo_to_wkt(geo).ok(),
                    (GeometryDataType::EWKB, Ok((geo, srid))) => {
                        geo_to_ewkb(geo, srid).map(hex::encode_upper).ok()
                    }
                    (GeometryDataType::EWKT, Ok((geo, srid))) => geo_to_ewkt(geo, srid).ok(),
                    (GeometryDataType::GEOJSON, Ok((geo, _))) => geo_to_json(geo).ok(),
                    (_, Err(_)) => None,
                }
            }
            Column::Nullable(c) => {
                if !c.validity.get_bit(self.row_index) {
                    return None;
                }
                self.try_direct_as_string(&c.column)
            }
            _ => None,
        }
    }
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
            let string = self.try_direct_as_string(column).unwrap_or_else(|| {
                let mut buf = self.buf.borrow_mut();
                buf.clear();
                self.encoder
                    .write_field(column, self.row_index, buf.deref_mut(), false);
                String::from_utf8_lossy(buf.deref_mut()).into_owned()
            });
            serialize_seq.serialize_element(&Some(string))?;
        }
        serialize_seq.end()
    }
}
