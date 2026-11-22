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
use std::hash::Hash;

use databend_common_catalog::plan::VirtualColumnLayout;
use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::types::DataType;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::array::ArrayColumn;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::string::StringColumnBuilder;
use jsonb::RawJsonb;
use jsonb::keypath::OwnedKeyPaths;
use log::warn;
use siphasher::sip128::Hasher128;
use siphasher::sip128::SipHasher24;

/// Extracts the values of virtual columns from Variant source columns into
/// ordinary columns of the block.
///
/// Recluster runs this right after reading, while every input stream is still
/// parallel. The extracted columns travel with the rows through sort, exchange
/// and compaction, and `VirtualColumnBuilder::add_extracted_columns` turns them
/// into the sidecar file when the block is written.
///
/// The block layout is `[source columns][extracted columns][temporary keys]`.
/// For each Variant source column, in schema order, the extracted columns are:
/// - one `Nullable(Variant)` column per layout direct path. SQL NULL means the
///   row has no value at the path; otherwise the JSONB-encoded scalar is stored,
///   JSON null included.
/// - one `Map(String, Variant)` column with every other path of the row, keyed
///   by canonical path.
#[derive(Debug)]
pub struct VirtualColumnExtractor {
    num_source_columns: usize,
    sources: Vec<VariantSource>,
    num_columns: usize,
}

/// One Variant source column and its extracted columns.
#[derive(Debug)]
pub(crate) struct VariantSource {
    /// Offset of the Variant column in the source schema and in the block.
    pub(crate) column_offset: usize,
    /// Layout direct paths of this source, in column order.
    pub(crate) direct_paths: Vec<OwnedKeyPaths>,
    /// Canonical direct path -> position in `direct_paths`.
    direct_positions: HashMap<String, usize>,
    /// Position of the first direct path column among the extracted columns.
    pub(crate) first_column: usize,
}

impl VariantSource {
    /// Position of the `Map(String, Variant)` column among the extracted columns.
    pub(crate) fn remaining_column(&self) -> usize {
        self.first_column + self.direct_paths.len()
    }
}

/// Where the values of one key path go, cached by key-path hash for one block.
#[derive(Clone, Copy)]
enum PathTarget {
    Direct(usize),
    Remaining(usize),
}

impl VirtualColumnExtractor {
    /// Returns `None` when `source_schema` has no Variant column.
    ///
    /// `source_schema` must describe exactly the columns the reader produces and
    /// the writer serializes, in the same order.
    pub fn try_create(
        source_schema: &TableSchema,
        layout: &VirtualColumnLayout,
    ) -> Option<VirtualColumnExtractor> {
        let mut sources = Vec::new();
        let mut num_columns = 0;
        // Same Variant field order as `VirtualColumnBuilder::try_create`.
        for (column_offset, field) in source_schema.fields.iter().enumerate() {
            if field.data_type().remove_nullable() != TableDataType::Variant {
                continue;
            }
            let mut direct_paths = Vec::new();
            let mut direct_positions = HashMap::new();
            for path in &layout.direct_paths {
                if path.source_column_id != field.column_id {
                    continue;
                }
                // A visited path matches a layout path by its canonical form; a
                // path that does not round-trip can never match.
                let key_paths = OwnedKeyPaths::from_canonical_path(&path.path);
                match key_paths {
                    Ok(key_paths) if key_paths.to_canonical_path() == path.path => {
                        direct_positions.insert(path.path.clone(), direct_paths.len());
                        direct_paths.push(key_paths);
                    }
                    _ => warn!(
                        "skip non-canonical virtual column layout path {:?} of column {}",
                        path.path, field.column_id
                    ),
                }
            }
            let source = VariantSource {
                column_offset,
                direct_paths,
                direct_positions,
                first_column: num_columns,
            };
            num_columns = source.remaining_column() + 1;
            sources.push(source);
        }
        if sources.is_empty() {
            return None;
        }
        Some(VirtualColumnExtractor {
            num_source_columns: source_schema.num_fields(),
            sources,
            num_columns,
        })
    }

    pub fn num_source_columns(&self) -> usize {
        self.num_source_columns
    }

    /// Number of extracted columns appended to a block.
    pub fn num_columns(&self) -> usize {
        self.num_columns
    }

    pub(crate) fn sources(&self) -> &[VariantSource] {
        &self.sources
    }

    /// Fields of the extracted columns, in block order.
    pub fn fields(&self) -> Vec<DataField> {
        let direct_type = DataType::Nullable(Box::new(DataType::Variant));
        let remaining_type = DataType::Map(Box::new(DataType::Tuple(vec![
            DataType::String,
            DataType::Variant,
        ])));
        let mut fields = Vec::with_capacity(self.num_columns);
        for source in &self.sources {
            for _ in &source.direct_paths {
                let name = format!("_virtual_column_{}", fields.len());
                fields.push(DataField::new(&name, direct_type.clone()));
            }
            let name = format!("_virtual_column_{}", fields.len());
            fields.push(DataField::new(&name, remaining_type.clone()));
        }
        fields
    }

    /// Appends the extracted columns to a block of exactly the source columns.
    pub fn extract(&self, mut block: DataBlock) -> Result<DataBlock> {
        if block.num_columns() != self.num_source_columns {
            return Err(ErrorCode::Internal(format!(
                "virtual column extraction expects {} source columns, got {}",
                self.num_source_columns,
                block.num_columns()
            )));
        }
        let mut columns = Vec::with_capacity(self.num_columns);
        for source in &self.sources {
            Self::extract_source(&block, source, &mut columns);
        }
        for column in columns {
            block.add_column(column);
        }
        Ok(block)
    }

    /// Splits `[source columns][extracted columns][temporary keys]` into the
    /// block without the extracted columns and the extracted columns.
    pub fn split(&self, mut block: DataBlock) -> Result<(DataBlock, Vec<BlockEntry>)> {
        let end = self.num_source_columns + self.num_columns;
        if block.num_columns() < end {
            return Err(ErrorCode::Internal(format!(
                "block has {} columns, expected at least {} source and extracted columns",
                block.num_columns(),
                end
            )));
        }
        let num_rows = block.num_rows();
        let meta = block.take_meta();
        let mut entries = block.take_columns();
        let extracted = entries.drain(self.num_source_columns..end).collect();
        Ok((DataBlock::new_with_meta(entries, num_rows, meta), extracted))
    }

    fn extract_source(block: &DataBlock, source: &VariantSource, columns: &mut Vec<Column>) {
        let num_rows = block.num_rows();
        let num_direct = source.direct_paths.len();
        let column = block.get_by_offset(source.column_offset);

        let mut direct_values = Vec::with_capacity(num_direct);
        let mut direct_validity = Vec::with_capacity(num_direct);
        for _ in 0..num_direct {
            direct_values.push(BinaryColumnBuilder::with_capacity(num_rows, 0));
            direct_validity.push(MutableBitmap::with_capacity(num_rows));
        }
        let mut remaining_keys = StringColumnBuilder::with_capacity(0);
        let mut remaining_values = BinaryColumnBuilder::with_capacity(0, 0);
        let mut remaining_offsets = Vec::with_capacity(num_rows + 1);
        remaining_offsets.push(0u64);

        // Same path identity as `VirtualColumnBuilder::extract_virtual_values`:
        // a SipHash-128 of the borrowed key paths, so a path is decoded and
        // classified once per block rather than once per value.
        let mut targets: HashMap<u128, PathTarget> = HashMap::new();
        let mut remaining_names: Vec<String> = Vec::new();
        // Encoded values of the current row, committed only if the row is valid JSONB.
        let mut row_bytes = Vec::new();
        let mut row_values: Vec<(PathTarget, usize, usize)> = Vec::new();
        let mut row_direct: Vec<Option<(usize, usize)>> = vec![None; num_direct];
        let mut num_remaining = 0u64;

        for row in 0..num_rows {
            row_bytes.clear();
            row_values.clear();
            if let ScalarRef::Variant(jsonb_bytes) = unsafe { column.index_unchecked(row) } {
                let visited =
                    RawJsonb::new(jsonb_bytes).visit_scalar_key_values(true, |key_paths, value| {
                        let mut hasher = SipHasher24::new();
                        key_paths.hash(&mut hasher);
                        let hash: u128 = hasher.finish128().into();
                        let target = match targets.get(&hash) {
                            Some(target) => *target,
                            None => {
                                let path = OwnedKeyPaths::from_key_path_slice(key_paths)
                                    .to_canonical_path();
                                let target = match source.direct_positions.get(&path) {
                                    Some(position) => PathTarget::Direct(*position),
                                    None => {
                                        remaining_names.push(path);
                                        PathTarget::Remaining(remaining_names.len() - 1)
                                    }
                                };
                                targets.insert(hash, target);
                                target
                            }
                        };
                        let start = row_bytes.len();
                        value.write_to_vec(&mut row_bytes);
                        row_values.push((target, start, row_bytes.len()));
                        Ok(())
                    });
                if visited.is_err() {
                    // Invalid JSONB (e.g. loaded with `disable_variant_check`) has no
                    // readable paths: the row is absent from every virtual column.
                    row_values.clear();
                }
            }

            for value in row_direct.iter_mut() {
                *value = None;
            }
            for (target, start, end) in row_values.iter().copied() {
                match target {
                    PathTarget::Direct(position) => {
                        // Valid JSONB has unique object keys; keep the first value
                        // if a key is repeated anyway.
                        if row_direct[position].is_none() {
                            row_direct[position] = Some((start, end));
                        }
                    }
                    PathTarget::Remaining(index) => {
                        remaining_keys.put_and_commit(&remaining_names[index]);
                        remaining_values.put_slice(&row_bytes[start..end]);
                        remaining_values.commit_row();
                        num_remaining += 1;
                    }
                }
            }
            remaining_offsets.push(num_remaining);
            for position in 0..num_direct {
                if let Some((start, end)) = row_direct[position] {
                    direct_values[position].put_slice(&row_bytes[start..end]);
                }
                direct_values[position].commit_row();
                direct_validity[position].push(row_direct[position].is_some());
            }
        }

        for (values, validity) in direct_values.into_iter().zip(direct_validity) {
            columns.push(Column::Variant(values.build()).wrap_nullable(Some(validity.into())));
        }
        let entries = Column::Tuple(vec![
            Column::String(remaining_keys.build()),
            Column::Variant(remaining_values.build()),
        ]);
        columns.push(Column::Map(Box::new(ArrayColumn::new(
            entries,
            Buffer::from(remaining_offsets),
        ))));
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use databend_common_catalog::plan::VirtualColumnPath;
    use databend_common_expression::FromData;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchemaRef;
    use databend_common_expression::arrow::deserialize_column;
    use databend_common_expression::arrow::serialize_column;
    use databend_common_expression::block_debug::assert_block_value_eq;
    use databend_common_expression::infer_table_schema;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::VariantType;
    use jsonb::Date;
    use jsonb::Decimal64;
    use jsonb::Decimal128;
    use jsonb::Interval;
    use jsonb::Number;
    use jsonb::Timestamp;
    use jsonb::TimestampTz;
    use jsonb::Value as JsonbValue;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
    use parquet::file::reader::FileReader;
    use parquet::file::reader::SerializedFileReader;

    use super::*;
    use crate::io::VirtualColumnBuilder;
    use crate::io::VirtualColumnLayoutPolicy;
    use crate::io::write::WriteSettings;

    fn jsonb(json: &str) -> Vec<u8> {
        json.parse::<jsonb::OwnedJsonb>().unwrap().to_vec()
    }

    fn encode(value: JsonbValue<'_>) -> Vec<u8> {
        let mut buf = Vec::new();
        value.write_to_vec(&mut buf);
        buf
    }

    fn object(entries: Vec<(&str, JsonbValue<'static>)>) -> Vec<u8> {
        encode(JsonbValue::Object(
            entries
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect::<BTreeMap<_, _>>(),
        ))
    }

    /// `[id, v, k, w]`: two Variant sources around a non-Variant column.
    fn schema() -> TableSchemaRef {
        Arc::new(TableSchema::new(vec![
            TableField::new("id", TableDataType::Number(NumberDataType::Int64)),
            TableField::new("v", TableDataType::Variant),
            TableField::new("k", TableDataType::Number(NumberDataType::Int64)),
            TableField::new(
                "w",
                TableDataType::Nullable(Box::new(TableDataType::Variant)),
            ),
        ]))
    }

    fn source_rows() -> Vec<Vec<u8>> {
        let mut truncated = jsonb(r#"{"a":1,"b":{"c":"xyz"}}"#);
        truncated.pop();
        let mut rows = vec![
            jsonb(r#"{"a":1,"b":{"c":"x"},"arr":[1,2],"n":null}"#),
            jsonb(r#"{"a":-5,"b":{"c":2},"e":{},"f":[]}"#),
            jsonb(r#"{"a":0,"z1":true}"#),
            // Invalid JSONB: a known path is visited before the error.
            truncated,
            // `disable_variant_check` stores what it fails to parse.
            Vec::new(),
            jsonb(r#"{"a":1.5,"b":{"c":null},"q":"s"}"#),
            jsonb(r#"{"a":18446744073709551615,"d":"2024","arr":[{"x":1}]}"#),
            object(vec![
                (
                    "a",
                    JsonbValue::Number(Number::Decimal64(Decimal64 {
                        scale: 2,
                        value: 12345,
                    })),
                ),
                (
                    "big",
                    JsonbValue::Number(Number::Decimal128(Decimal128 {
                        scale: 3,
                        value: i128::MAX / 7,
                    })),
                ),
                ("dt", JsonbValue::Date(Date { value: 19000 })),
                ("ts", JsonbValue::Timestamp(Timestamp { value: 1 << 40 })),
                (
                    "tz",
                    JsonbValue::TimestampTz(TimestampTz {
                        offset: 3600,
                        value: 1 << 41,
                    }),
                ),
                (
                    "iv",
                    JsonbValue::Interval(Interval {
                        months: 1,
                        days: 2,
                        micros: 3,
                    }),
                ),
                ("bin", JsonbValue::Binary(b"\x00\x01")),
                ("s", JsonbValue::String(Cow::Borrowed("text"))),
            ]),
            // A root scalar has no paths.
            jsonb(r#""scalar""#),
            jsonb(r#"{"weird.key":1,"'q'":2,"0":3,"":4,"中文":5}"#),
            jsonb(r#"{}"#),
            jsonb(r#"[1,2,3]"#),
        ];
        // Many sparse paths, so pieces see different shared path sets.
        for row in 0..240 {
            rows.push(jsonb(&format!(
                r#"{{"a":{row},"k{}":{row},"m{}":"s{row}","g":{{"h{}":[{row}],"i":{}}}}}"#,
                row % 37,
                (row * 7) % 53,
                row % 5,
                if row % 3 == 0 {
                    "null".to_string()
                } else {
                    format!("{}", row as f64 / 4.0)
                }
            )));
        }
        rows
    }

    fn source_block() -> DataBlock {
        let v = source_rows();
        let num_rows = v.len();
        let w = (0..num_rows)
            .map(|row| match row % 4 {
                0 => None,
                1 => Some(jsonb(&format!(r#"{{"x":{row},"y":{{"z":[{row}]}}}}"#))),
                2 => Some(jsonb(&format!(r#"{{"x":"s{row}","t{}":true}}"#, row % 9))),
                _ => Some(jsonb(r#"{"x":null}"#)),
            })
            .collect::<Vec<_>>();
        DataBlock::new_from_columns(vec![
            Int64Type::from_data((0..num_rows as i64).collect::<Vec<_>>()),
            VariantType::from_data(v),
            Int64Type::from_data(vec![7; num_rows]),
            VariantType::from_opt_data(w),
        ])
    }

    fn layout(schema: &TableSchema) -> VirtualColumnLayout {
        let v = schema.field_with_name("v").unwrap().column_id;
        let w = schema.field_with_name("w").unwrap().column_id;
        let mut direct_paths = ["a", "b.c", "arr", "n", "k3", "missing", "'weird.key'"]
            .into_iter()
            .map(|path| VirtualColumnPath {
                source_column_id: v,
                path: path.to_string(),
            })
            .chain(["x", "y.z"].into_iter().map(|path| VirtualColumnPath {
                source_column_id: w,
                path: path.to_string(),
            }))
            // A dropped source column contributes no extracted column.
            .chain(std::iter::once(VirtualColumnPath {
                source_column_id: 999,
                path: "a".to_string(),
            }))
            .collect::<Vec<_>>();
        direct_paths.sort();
        VirtualColumnLayout { direct_paths }
    }

    /// Footer key-value metadata, JSON values parsed so that object key order
    /// does not matter.
    type SidecarMetadata = BTreeMap<String, Option<serde_json::Value>>;

    /// Column data and footer key-value metadata of a sidecar file.
    fn decode_sidecar(data: Vec<u8>) -> Result<(Vec<RecordBatch>, SidecarMetadata)> {
        let data = bytes::Bytes::from(data);
        let reader = SerializedFileReader::new(data.clone())?;
        let metadata = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .into_iter()
            .flatten()
            .map(|entry| {
                let value = entry.value.as_deref().map(|value| {
                    serde_json::from_str(value)
                        .unwrap_or_else(|_| serde_json::Value::String(value.to_string()))
                });
                (entry.key.clone(), value)
            })
            .collect();
        let batches = ParquetRecordBatchReader::try_new(data, usize::MAX)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok((batches, metadata))
    }

    /// Carries the rows of `source` through extraction per piece, concat,
    /// reordering and re-slicing, then checks every output block against the
    /// writer-side extraction of the same rows.
    fn check_equivalent(
        schema: &TableSchemaRef,
        layout: VirtualColumnLayout,
        policy: VirtualColumnLayoutPolicy,
        source: &DataBlock,
    ) -> Result<()> {
        let extractor = VirtualColumnExtractor::try_create(schema, &layout).unwrap();
        let num_rows = source.num_rows();

        // Extract each input piece separately, as parallel readers do.
        let mut pieces = Vec::new();
        let mut start = 0;
        for len in [3, 9, 1, 50, num_rows] {
            let end = (start + len).min(num_rows);
            if start < end {
                pieces.push(extractor.extract(source.slice(start..end))?);
            }
            start = end;
        }
        let mut carried = DataBlock::concat(&pieces)?;
        assert_eq!(
            carried.num_columns(),
            schema.num_fields() + extractor.num_columns()
        );
        // A temporary key column follows the extracted columns.
        carried.add_column(Int64Type::from_data(
            (0..num_rows as i64).rev().collect::<Vec<_>>(),
        ));

        // Sort-like reordering, then output blocks of different sizes.
        let permutation = (0..num_rows as u32)
            .map(|row| (row * 101) % num_rows as u32)
            .collect::<Vec<_>>();
        let reordered = carried.take(permutation.as_slice())?;

        let template = VirtualColumnBuilder::try_create(schema.clone(), policy)?
            .with_adaptive_layout(Arc::new(layout));
        let write_settings = WriteSettings::default();
        let location = ("block.parquet".to_string(), 0);
        let mut start = 0;
        for len in [1, 17, 128, num_rows] {
            let end = (start + len).min(num_rows);
            if start == end {
                break;
            }
            let output = reordered.slice(start..end);
            start = end;

            let (mut output, entries) = extractor.split(output)?;
            output.pop_columns(1);
            assert_eq!(output.num_columns(), schema.num_fields());

            let mut expected = template.clone();
            expected.add_block(&output)?;
            let expected = expected.finalize(&write_settings, &location)?;

            let mut actual = template.clone();
            actual.add_extracted_columns(&extractor, &entries, output.num_rows())?;
            let mut actual = actual.finalize(&write_settings, &location)?;

            // Every finalize names a new sidecar file.
            if let (Some(actual), Some(expected)) = (
                actual.draft_virtual_block_meta.virtual_columns.as_mut(),
                expected.draft_virtual_block_meta.virtual_columns.as_ref(),
            ) {
                actual.virtual_location = expected.virtual_location.clone();
            }
            // Metadata first: its diff names the virtual column that differs.
            assert_eq!(
                actual.draft_virtual_block_meta,
                expected.draft_virtual_block_meta
            );
            // The footer serializes the path trie from a HashMap, so the bytes of
            // two writer-side runs differ too; compare the decoded sidecars.
            assert_eq!(
                decode_sidecar(actual.data.to_vec())?,
                decode_sidecar(expected.data.to_vec())?
            );
        }
        Ok(())
    }

    #[test]
    fn extracted_columns_match_writer_extraction() -> Result<()> {
        let schema = schema();
        let source = source_block();
        check_equivalent(
            &schema,
            layout(&schema),
            VirtualColumnLayoutPolicy::default(),
            &source,
        )?;
        // Truncated path statistics and few direct paths.
        check_equivalent(
            &schema,
            layout(&schema),
            VirtualColumnLayoutPolicy {
                max_direct_columns: 2,
                max_path_statistics: 3,
            },
            &source,
        )?;
        // No layout path is present: every value goes through the maps.
        let v = schema.field_with_name("v").unwrap().column_id;
        check_equivalent(
            &schema,
            VirtualColumnLayout {
                direct_paths: vec![VirtualColumnPath {
                    source_column_id: v,
                    path: "nowhere".to_string(),
                }],
            },
            VirtualColumnLayoutPolicy::default(),
            &source,
        )
    }

    #[test]
    fn extracted_column_layout() {
        let schema = schema();
        let extractor = VirtualColumnExtractor::try_create(&schema, &layout(&schema)).unwrap();
        assert_eq!(extractor.num_source_columns(), 4);
        // `v`: 7 direct paths and a map; `w`: 2 direct paths and a map.
        assert_eq!(extractor.num_columns(), 11);
        let sources = extractor.sources();
        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0].column_offset, 1);
        assert_eq!(sources[0].remaining_column(), 7);
        assert_eq!(sources[1].column_offset, 3);
        assert_eq!(sources[1].first_column, 8);
        let fields = extractor.fields();
        assert_eq!(fields.len(), 11);
        assert_eq!(
            fields[0].data_type(),
            &DataType::Nullable(Box::new(DataType::Variant))
        );
        assert!(matches!(fields[7].data_type(), DataType::Map(_)));

        let no_variant = TableSchema::new(vec![TableField::new(
            "id",
            TableDataType::Number(NumberDataType::Int64),
        )]);
        assert!(VirtualColumnExtractor::try_create(&no_variant, &layout(&schema)).is_none());
    }

    #[test]
    fn extractor_rejects_unexpected_columns() {
        let schema = schema();
        let extractor = VirtualColumnExtractor::try_create(&schema, &layout(&schema)).unwrap();
        let source = source_block();
        let mut wider = source.clone();
        wider.add_column(Int64Type::from_data(vec![0; source.num_rows()]));
        assert!(extractor.extract(wider).is_err());
        assert!(extractor.split(source).is_err());
    }

    /// Sort spill writes blocks as Arrow IPC or Parquet; extracted columns must survive both.
    #[test]
    fn extracted_columns_round_trip_through_spill_formats() -> Result<()> {
        let schema = schema();
        let extractor = VirtualColumnExtractor::try_create(&schema, &layout(&schema)).unwrap();
        let extracted = extractor.extract(source_block())?;
        let num_rows = extracted.num_rows();
        let (_, entries) = extractor.split(extracted)?;
        let block = DataBlock::new(entries, num_rows);

        for entry in block.columns() {
            let column = entry.to_column();
            assert_eq!(deserialize_column(&serialize_column(&column))?, column);
        }

        let table_schema = infer_table_schema(&block.infer_schema())?;
        let batch = block.clone().to_record_batch(&table_schema)?;
        let mut data = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut data, batch.schema(), None)?;
        writer.write(&batch)?;
        writer.close()?;
        let mut restored = Vec::new();
        for batch in ParquetRecordBatchReader::try_new(bytes::Bytes::from(data), usize::MAX)? {
            let batch = batch?;
            let columns = batch
                .columns()
                .iter()
                .zip(block.columns())
                .map(|(array, entry)| Column::from_arrow_rs(array.clone(), &entry.data_type()))
                .collect::<Result<Vec<_>>>()?;
            restored.push(DataBlock::new_from_columns(columns));
        }
        assert_block_value_eq(&block, &DataBlock::concat(&restored)?);

        // Nullable direct columns keep SQL NULL apart from JSON null.
        let Column::Nullable(direct) = block.get_by_offset(0).to_column() else {
            unreachable!()
        };
        assert!(direct.validity().null_count() > 0);
        Ok(())
    }
}
