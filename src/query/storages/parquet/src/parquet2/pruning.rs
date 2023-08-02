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
use std::collections::HashSet;
use std::io::Read;
use std::io::Seek;
use std::sync::Arc;

use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::io::parquet::read as pread;
use common_arrow::arrow::io::parquet::read::get_field_pages;
use common_arrow::arrow::io::parquet::read::indexes::compute_page_row_intervals;
use common_arrow::arrow::io::parquet::read::indexes::read_columns_indexes;
use common_arrow::arrow::io::parquet::read::indexes::FieldPageStatistics;
use common_arrow::parquet::indexes::Interval;
use common_arrow::parquet::metadata::FileMetaData;
use common_arrow::parquet::metadata::RowGroupMetaData;
use common_arrow::parquet::metadata::SchemaDescriptor;
use common_arrow::parquet::read::read_metadata_with_size;
use common_arrow::parquet::read::read_pages_locations;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::TopK;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Expr;
use common_expression::FieldIndex;
use common_expression::FunctionContext;
use common_expression::TableSchemaRef;
use common_storage::read_parquet_metas_in_parallel;
use common_storage::ColumnNodes;
use log::info;
use opendal::Operator;
use storages_common_pruner::RangePruner;
use storages_common_pruner::RangePrunerCreator;

use crate::parquet2::statistics::collect_row_group_stats;
use crate::parquet2::statistics::BatchStatistics;
use crate::parquet_part::collect_small_file_parts;
use crate::parquet_part::ColumnMeta;
use crate::parquet_part::ParquetPart;
use crate::parquet_part::ParquetRowGroupPart;
use crate::processors::SmallFilePrunner;

/// Prune parquet row groups and pages.
pub struct PartitionPruner {
    /// Table schema.
    pub schema: TableSchemaRef,
    pub schema_descr: SchemaDescriptor,
    pub schema_from: String,
    /// Pruner to prune row groups.
    pub row_group_pruner: Option<Arc<dyn RangePruner + Send + Sync>>,
    /// Pruners to prune pages.
    pub page_pruners: Option<ColumnRangePruners>,
    /// The projected column indices.
    pub columns_to_read: HashSet<FieldIndex>,
    /// The projected column nodes.
    pub column_nodes: ColumnNodes,
    /// Whether to skip pruning.
    pub skip_pruning: bool,
    /// top k information from pushed down information. The usize is the offset of top k column in `schema`.
    pub top_k: Option<(TopK, usize)>,
    // TODO: use limit information for pruning
    // /// Limit of this query. If there is order by and filter, it will not be used (assign to `usize::MAX`).
    // pub limit: usize,
    pub parquet_fast_read_bytes: usize,
    pub compression_ratio: f64,
    pub max_memory_usage: u64,
}

fn check_parquet_schema(
    expect: &SchemaDescriptor,
    actual: &SchemaDescriptor,
    path: &str,
    schema_from: &str,
) -> Result<()> {
    if expect.fields() != actual.fields() || expect.columns() != actual.columns() {
        return Err(ErrorCode::BadBytes(format!(
            "infer schema from '{}', but get diff schema in file '{}'. Expected schema: {:?}, actual: {:?}",
            schema_from, path, expect, actual
        )));
    }
    Ok(())
}

impl SmallFilePrunner for PartitionPruner {
    fn prune_one_file(
        &self,
        path: &str,
        op: &Operator,
        file_size: u64,
    ) -> Result<Vec<ParquetRowGroupPart>> {
        let blocking_op = op.blocking();
        let mut reader = blocking_op.reader(path)?;
        let file_meta = read_metadata_with_size(&mut reader, file_size).map_err(|e| {
            ErrorCode::Internal(format!("Read parquet file '{}''s meta error: {}", path, e))
        })?;
        let (_, parts) = self.read_and_prune_file_meta(path, file_meta, op.clone())?;
        Ok(parts)
    }
}

impl PartitionPruner {
    #[async_backtrace::framed]
    pub fn read_and_prune_file_meta(
        &self,
        path: &str,
        file_meta: FileMetaData,
        operator: Operator,
    ) -> Result<(PartStatistics, Vec<ParquetRowGroupPart>)> {
        check_parquet_schema(
            &self.schema_descr,
            file_meta.schema(),
            path,
            &self.schema_from,
        )?;
        let mut stats = PartStatistics::default();
        let mut partitions = vec![];

        let is_blocking_io = operator.info().can_blocking();
        let mut row_group_pruned = vec![false; file_meta.row_groups.len()];

        let no_stats = file_meta.row_groups.iter().any(|r| {
            r.columns()
                .iter()
                .any(|c| c.metadata().statistics.is_none())
        });

        let row_group_stats = if no_stats {
            None
        } else if self.row_group_pruner.is_some() && !self.skip_pruning {
            let pruner = self.row_group_pruner.as_ref().unwrap();
            // If collecting stats fails or `should_keep` is true, we still read the row group.
            // Otherwise, the row group will be pruned.
            if let Ok(row_group_stats) =
                collect_row_group_stats(&self.column_nodes, &file_meta.row_groups)
            {
                for (idx, (stats, _rg)) in row_group_stats
                    .iter()
                    .zip(file_meta.row_groups.iter())
                    .enumerate()
                {
                    row_group_pruned[idx] = !pruner.should_keep(stats, None);
                }
                Some(row_group_stats)
            } else {
                None
            }
        } else if self.top_k.is_some() {
            collect_row_group_stats(&self.column_nodes, &file_meta.row_groups).ok()
        } else {
            None
        };

        for (rg_idx, rg) in file_meta.row_groups.iter().enumerate() {
            if row_group_pruned[rg_idx] {
                continue;
            }

            stats.read_rows += rg.num_rows();
            stats.read_bytes += rg.total_byte_size();
            stats.partitions_scanned += 1;

            // Currently, only blocking io is allowed to prune pages.
            let row_selection = if self.page_pruners.is_some()
                && is_blocking_io
                && rg.columns().iter().all(|c| {
                    c.column_chunk().column_index_offset.is_some()
                        && c.column_chunk().column_index_length.is_some()
                }) {
                let mut reader = operator.blocking().reader(path)?;
                self.page_pruners
                    .as_ref()
                    .map(|pruners| filter_pages(&mut reader, &self.schema, rg, pruners))
                    .transpose()
                    .unwrap_or(None)
            } else {
                None
            };

            let mut column_metas = HashMap::with_capacity(self.columns_to_read.len());
            for index in self.columns_to_read.iter() {
                let c = &rg.columns()[*index];
                let (offset, length) = c.byte_range();

                let min_max = self
                    .top_k
                    .as_ref()
                    .filter(|(tk, _)| tk.column_id as usize == *index)
                    .zip(row_group_stats.as_ref())
                    .map(|((_, offset), stats)| {
                        let stat = stats[rg_idx].get(&(*offset as u32)).unwrap();
                        (stat.min().clone(), stat.max().clone())
                    });

                column_metas.insert(*index, ColumnMeta {
                    offset,
                    length,
                    num_values: c.num_values(),
                    compression: c.compression(),
                    uncompressed_size: c.uncompressed_size() as u64,
                    min_max,
                    has_dictionary: c.dictionary_page_offset().is_some(),
                });
            }

            partitions.push(ParquetRowGroupPart {
                location: path.to_string(),
                num_rows: rg.num_rows(),
                column_metas,
                row_selection,
                sort_min_max: None,
            })
        }
        Ok((stats, partitions))
    }

    /// Try to read parquet meta to generate row-group-wise partitions.
    /// And prune row groups an pages to generate the final row group partitions.
    #[async_backtrace::framed]
    pub async fn read_and_prune_partitions(
        &self,
        operator: Operator,
        locations: &Vec<(String, u64)>,
    ) -> Result<(PartStatistics, Partitions)> {
        // part stats
        let mut stats = PartStatistics::default();

        let mut large_files = vec![];
        let mut small_files = vec![];
        for (location, size) in locations {
            if *size > self.parquet_fast_read_bytes as u64 {
                large_files.push((location.clone(), *size));
            } else {
                small_files.push((location.clone(), *size));
            }
        }

        let mut partitions = Vec::with_capacity(locations.len());

        let is_blocking_io = operator.info().can_blocking();

        // 1. Read parquet meta data. Distinguish between sync and async reading.
        let file_metas = if is_blocking_io {
            let mut file_metas = Vec::with_capacity(locations.len());
            for (location, _size) in &large_files {
                let mut reader = operator.blocking().reader(location)?;
                let file_meta = pread::read_metadata(&mut reader).map_err(|e| {
                    ErrorCode::Internal(format!(
                        "Read parquet file '{}''s meta error: {}",
                        location, e
                    ))
                })?;
                file_metas.push(file_meta);
            }
            file_metas
        } else {
            read_parquet_metas_in_parallel(
                operator.clone(),
                large_files.clone(),
                16,
                64,
                self.max_memory_usage,
            )
            .await?
        };

        // 2. Use file meta to prune row groups or pages.
        let mut max_compression_ratio = self.compression_ratio;
        let mut max_compressed_size = 0u64;

        // If one row group does not have stats, we cannot use the stats for topk optimization.
        for (file_id, file_meta) in file_metas.into_iter().enumerate() {
            stats.partitions_total += file_meta.row_groups.len();
            let (sub_stats, parts) = self.read_and_prune_file_meta(
                &large_files[file_id].0,
                file_meta,
                operator.clone(),
            )?;
            for p in parts {
                max_compression_ratio = max_compression_ratio
                    .max(p.uncompressed_size() as f64 / p.compressed_size() as f64);
                max_compressed_size = max_compressed_size.max(p.compressed_size());
                partitions.push(ParquetPart::RowGroup(p));
            }
            stats.partitions_total += sub_stats.partitions_total;
            stats.partitions_scanned += sub_stats.partitions_scanned;
            stats.read_bytes += sub_stats.read_bytes;
            stats.read_rows += sub_stats.read_rows;
        }

        let num_large_partitions = partitions.len();

        collect_small_file_parts(
            small_files,
            max_compression_ratio,
            max_compressed_size,
            &mut partitions,
            &mut stats,
            self.columns_to_read.len(),
        );

        info!(
            "copy {num_large_partitions} large partitions and {} small partitions.",
            partitions.len() - num_large_partitions
        );

        let partition_kind = PartitionsShuffleKind::Mod;
        let partitions = partitions
            .into_iter()
            .map(|p| p.convert_to_part_info())
            .collect();

        Ok((stats, Partitions::create_nolazy(partition_kind, partitions)))
    }
}

/// [`RangePruner`]s for each column
type ColumnRangePruners = Vec<(usize, Arc<dyn RangePruner + Send + Sync>)>;

/// Build page pruner of each column.
/// Only one column expression can be used to build the page pruner.
pub fn build_column_page_pruners(
    func_ctx: FunctionContext,
    schema: &TableSchemaRef,
    filter: &Expr<String>,
) -> Result<ColumnRangePruners> {
    let mut results = vec![];
    for (column, _) in filter.column_refs() {
        let col_idx = schema.index_of(&column)?;
        let range_pruner = RangePrunerCreator::try_create(func_ctx.clone(), schema, Some(filter))?;
        results.push((col_idx, range_pruner));
    }
    Ok(results)
}

/// Filter pages by filter expression.
///
/// Returns the final selection of rows.
fn filter_pages<R: Read + Seek>(
    reader: &mut R,
    schema: &TableSchemaRef,
    row_group: &RowGroupMetaData,
    pruners: &ColumnRangePruners,
) -> Result<Vec<Interval>> {
    let mut fields = Vec::with_capacity(pruners.len());
    for (col_idx, _) in pruners {
        let field: ArrowField = schema.field(*col_idx).into();
        fields.push(field);
    }

    let num_rows = row_group.num_rows();

    // one vec per column
    let locations = read_pages_locations(reader, row_group.columns())?;
    // one Vec<Vec<>> per field (non-nested contain a single entry on the first column)
    let locations = fields
        .iter()
        .map(|field| get_field_pages(row_group.columns(), &locations, &field.name))
        .collect::<Vec<_>>();

    // one ColumnPageStatistics per field
    let page_stats = read_columns_indexes(reader, row_group.columns(), &fields)?;

    let intervals = locations
        .iter()
        .map(|locations| {
            locations
                .iter()
                .map(|locations| Ok(compute_page_row_intervals(locations, num_rows)?))
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    // Currently, only non-nested types are supported.
    let mut row_selections = Vec::with_capacity(pruners.len());
    for (i, (col_offset, pruner)) in pruners.iter().enumerate() {
        let stat = &page_stats[i];
        let page_intervals = &intervals[i][0];
        let data_type = schema.field(*col_offset).data_type();

        let mut row_selection = vec![];
        match stat {
            FieldPageStatistics::Single(stats) => {
                let stats = BatchStatistics::from_column_statistics(stats, &data_type.into())?;
                for (page_num, intv) in page_intervals.iter().enumerate() {
                    let stat = stats.get(page_num);
                    if pruner.should_keep(&HashMap::from([(*col_offset as u32, stat)]), None) {
                        row_selection.push(*intv);
                    }
                }
            }
            _ => {
                return Err(ErrorCode::Internal(
                    "Only non-nested types are supported in page filter.",
                ));
            }
        }
        row_selections.push(row_selection);
    }

    Ok(combine_intervals(row_selections))
}

/// Combine row selection of each column into a final selection of the whole row group.
fn combine_intervals(row_selections: Vec<Vec<Interval>>) -> Vec<Interval> {
    if row_selections.is_empty() {
        return vec![];
    }
    let mut selection = row_selections[0].clone();
    for sel in row_selections.iter().skip(1) {
        selection = and_intervals(&selection, sel);
    }

    // Merge intervals if they are consecutive
    let mut res = vec![];
    for sel in selection {
        if res.is_empty() {
            res.push(sel);
            continue;
        }
        let back = res.last_mut().unwrap();
        if back.start + back.length == sel.start {
            back.length += sel.length;
        } else {
            res.push(sel);
        }
    }

    res
}

/// Do "and" operation on two row selections.
/// Select the rows which both `sel1` and `sel2` select.
fn and_intervals(sel1: &[Interval], sel2: &[Interval]) -> Vec<Interval> {
    let mut res = vec![];

    for sel in sel1 {
        res.extend(is_in(*sel, sel2));
    }
    res
}

/// If `probe` overlaps with `intervals`,
/// return the overlapping part of `probe` in `intervals`.
/// Otherwise, return an empty vector.
fn is_in(probe: Interval, intervals: &[Interval]) -> Vec<Interval> {
    intervals
        .iter()
        .filter_map(|interval| {
            let interval_end = interval.start + interval.length;
            let probe_end = probe.start + probe.length;
            let overlaps = (probe.start < interval_end) && (probe_end > interval.start);
            if overlaps {
                let start = interval.start.max(probe.start);
                let end = interval_end.min(probe_end);
                Some(Interval::new(start, end - start))
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use common_arrow::parquet::compression::CompressionOptions;
    use common_arrow::parquet::encoding::hybrid_rle::encode_bool;
    use common_arrow::parquet::encoding::Encoding;
    use common_arrow::parquet::indexes::Interval;
    use common_arrow::parquet::metadata::Descriptor;
    use common_arrow::parquet::metadata::SchemaDescriptor;
    use common_arrow::parquet::page::DataPage;
    use common_arrow::parquet::page::DataPageHeader;
    use common_arrow::parquet::page::DataPageHeaderV1;
    use common_arrow::parquet::page::Page;
    use common_arrow::parquet::read::read_metadata;
    use common_arrow::parquet::schema::types::ParquetType;
    use common_arrow::parquet::schema::types::PhysicalType;
    use common_arrow::parquet::statistics::serialize_statistics;
    use common_arrow::parquet::statistics::PrimitiveStatistics;
    use common_arrow::parquet::statistics::Statistics;
    use common_arrow::parquet::types::NativeType;
    use common_arrow::parquet::write::Compressor;
    use common_arrow::parquet::write::DynIter;
    use common_arrow::parquet::write::DynStreamingIterator;
    use common_arrow::parquet::write::FileWriter;
    use common_arrow::parquet::write::Version;
    use common_arrow::parquet::write::WriteOptions;
    use common_exception::Result;
    use common_expression::types::DataType;
    use common_expression::types::NumberDataType;
    use common_expression::types::NumberScalar;
    use common_expression::FunctionContext;
    use common_expression::Scalar;
    use common_expression::TableDataType;
    use common_expression::TableField;
    use common_expression::TableSchemaRef;
    use common_expression::TableSchemaRefExt;
    use common_sql::plans::BoundColumnRef;
    use common_sql::plans::ConstantExpr;
    use common_sql::plans::FunctionCall;
    use common_sql::plans::ScalarExpr;
    use common_sql::ColumnBindingBuilder;
    use common_sql::Visibility;
    use common_storage::ColumnNodes;
    use storages_common_pruner::RangePrunerCreator;

    use crate::parquet2::pruning::and_intervals;
    use crate::parquet2::pruning::build_column_page_pruners;
    use crate::parquet2::pruning::combine_intervals;
    use crate::parquet2::pruning::filter_pages;
    use crate::parquet2::statistics::collect_row_group_stats;

    #[test]
    fn test_and_intervals() {
        // [12, 35), [38, 43)
        let sel1 = vec![Interval::new(12, 23), Interval::new(38, 5)];
        // [0, 5), [9, 24), [30, 40)
        let sel2 = vec![
            Interval::new(0, 5),
            Interval::new(9, 15),
            Interval::new(30, 10),
        ];

        // [12, 24), [30, 35), [38, 40)
        let expected = vec![
            Interval::new(12, 12),
            Interval::new(30, 5),
            Interval::new(38, 2),
        ];
        let actual = and_intervals(&sel1, &sel2);

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_combine_intervals() {
        {
            // sel1: [12, 35), [38, 43)
            // sel2: [0, 5), [9, 24), [30, 40)
            // sel3: [1,2), [4, 31), [30, 41)
            let intervals = vec![
                vec![Interval::new(12, 23), Interval::new(38, 5)],
                vec![
                    Interval::new(0, 5),
                    Interval::new(9, 15),
                    Interval::new(30, 10),
                ],
                vec![
                    Interval::new(1, 1),
                    Interval::new(4, 27),
                    Interval::new(39, 2),
                ],
            ];

            // [12, 24), [30, 31), [39, 40)
            let expected = vec![
                Interval::new(12, 12),
                Interval::new(30, 1),
                Interval::new(39, 1),
            ];

            let actual = combine_intervals(intervals);

            assert_eq!(expected, actual);
        }

        {
            // sel1: [1,2), [2, 4), [4, 7)
            let intervals = vec![vec![
                Interval::new(1, 1),
                Interval::new(2, 2),
                Interval::new(4, 3),
            ]];

            // [12, 24), [30, 31), [39, 40)
            let expected = vec![Interval::new(1, 6)];

            let actual = combine_intervals(intervals);

            assert_eq!(expected, actual);
        }
    }

    fn unzip_option<T: NativeType>(
        array: &[Option<T>],
    ) -> common_arrow::parquet::error::Result<(Vec<u8>, Vec<u8>)> {
        // leave the first 4 bytes announcing the length of the def level
        // this will be overwritten at the end, once the length is known.
        // This is unknown at this point because of the uleb128 encoding,
        // whose length is variable.
        let mut validity = std::io::Cursor::new(vec![0; 4]);
        validity.set_position(4);

        let mut values = vec![];
        let iter = array.iter().map(|value| {
            if let Some(item) = value {
                values.extend_from_slice(item.to_le_bytes().as_ref());
                true
            } else {
                false
            }
        });
        encode_bool(&mut validity, iter)?;

        // write the length, now that it is known
        let mut validity = validity.into_inner();
        let length = validity.len() - 4;
        // todo: pay this small debt (loop?)
        let length = length.to_le_bytes();
        validity[0] = length[0];
        validity[1] = length[1];
        validity[2] = length[2];
        validity[3] = length[3];

        Ok((values, validity))
    }

    pub fn array_to_page_v1<T: NativeType>(
        array: &[Option<T>],
        options: &WriteOptions,
        descriptor: &Descriptor,
    ) -> common_arrow::parquet::error::Result<Page> {
        let (values, mut buffer) = unzip_option(array)?;

        buffer.extend_from_slice(&values);

        let statistics = if options.write_statistics {
            let statistics = &PrimitiveStatistics {
                primitive_type: descriptor.primitive_type.clone(),
                null_count: Some((array.len() - array.iter().flatten().count()) as i64),
                distinct_count: None,
                max_value: array.iter().flatten().max_by(|x, y| x.ord(y)).copied(),
                min_value: array.iter().flatten().min_by(|x, y| x.ord(y)).copied(),
            } as &dyn Statistics;
            Some(serialize_statistics(statistics))
        } else {
            None
        };

        let header = DataPageHeaderV1 {
            num_values: array.len() as i32,
            encoding: Encoding::Plain.into(),
            definition_level_encoding: Encoding::Rle.into(),
            repetition_level_encoding: Encoding::Rle.into(),
            statistics,
        };

        Ok(Page::Data(DataPage::new(
            DataPageHeader::V1(header),
            buffer,
            descriptor.clone(),
            Some(array.len()),
        )))
    }

    fn write_test_parquet() -> Result<(TableSchemaRef, Vec<u8>)> {
        let page1 = vec![Some(0), Some(1), None, Some(3), Some(4), Some(5), Some(6)];
        let page2 = vec![Some(10), Some(11)];

        let options = WriteOptions {
            write_statistics: true,
            version: Version::V1,
        };

        let schema = SchemaDescriptor::new("schema".to_string(), vec![ParquetType::from_physical(
            "col1".to_string(),
            PhysicalType::Int32,
        )]);

        let pages = vec![
            array_to_page_v1::<i32>(&page1, &options, &schema.columns()[0].descriptor),
            array_to_page_v1::<i32>(&page2, &options, &schema.columns()[0].descriptor),
        ];

        let pages = DynStreamingIterator::new(Compressor::new(
            DynIter::new(pages.into_iter()),
            CompressionOptions::Uncompressed,
            vec![],
        ));
        let columns = std::iter::once(Ok(pages));

        let writer = Cursor::new(vec![]);
        let mut writer = FileWriter::new(writer, schema, options, None);

        writer.write(DynIter::new(columns))?;
        writer.end(None)?;

        Ok((
            TableSchemaRefExt::create(vec![TableField::new(
                "col1",
                TableDataType::Number(NumberDataType::Int32),
            )]),
            writer.into_inner().into_inner(),
        ))
    }

    #[test]
    fn test_prune_row_group() -> Result<()> {
        let (schema, data) = write_test_parquet()?;
        let mut reader = Cursor::new(data);
        let metadata = read_metadata(&mut reader)?;
        let rgs = metadata.row_groups;
        let arrow_schema = schema.to_arrow();
        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, None);

        let row_group_stats = collect_row_group_stats(&column_nodes, &rgs)?;

        // col1 > 12
        {
            let filter = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "gt".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: ColumnBindingBuilder::new(
                            "col1".to_string(),
                            0,
                            Box::new(DataType::Number(NumberDataType::Int32)),
                            Visibility::Visible,
                        )
                        .build(),
                    }),
                    ScalarExpr::ConstantExpr(ConstantExpr {
                        span: None,
                        value: Scalar::Number(NumberScalar::Int32(12)),
                    }),
                ],
            });
            let filter = filter
                .as_expr()?
                .project_column_ref(|col| col.column_name.clone());
            let pruner =
                RangePrunerCreator::try_create(FunctionContext::default(), &schema, Some(&filter))?;
            assert!(!pruner.should_keep(&row_group_stats[0], None));
        }

        // col1 < 0
        {
            let filter = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "lt".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: ColumnBindingBuilder::new(
                            "col1".to_string(),
                            0,
                            Box::new(DataType::Number(NumberDataType::Int32)),
                            Visibility::Visible,
                        )
                        .build(),
                    }),
                    ScalarExpr::ConstantExpr(ConstantExpr {
                        span: None,
                        value: Scalar::Number(NumberScalar::Int32(0)),
                    }),
                ],
            });
            let filter = filter
                .as_expr()?
                .project_column_ref(|col| col.column_name.clone());
            let pruner =
                RangePrunerCreator::try_create(FunctionContext::default(), &schema, Some(&filter))?;
            assert!(!pruner.should_keep(&row_group_stats[0], None));
        }

        // col1 <= 5
        {
            let filter = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "lte".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: ColumnBindingBuilder::new(
                            "col1".to_string(),
                            0,
                            Box::new(DataType::Number(NumberDataType::Int32)),
                            Visibility::Visible,
                        )
                        .build(),
                    }),
                    ScalarExpr::ConstantExpr(ConstantExpr {
                        span: None,
                        value: Scalar::Number(NumberScalar::Int32(5)),
                    }),
                ],
            });
            let filter = filter
                .as_expr()?
                .project_column_ref(|col| col.column_name.clone());
            let pruner =
                RangePrunerCreator::try_create(FunctionContext::default(), &schema, Some(&filter))?;
            assert!(pruner.should_keep(&row_group_stats[0], None));
        }

        Ok(())
    }

    #[test]
    fn test_filter_pages() -> Result<()> {
        let (schema, data) = write_test_parquet()?;
        let mut reader = Cursor::new(data);
        let metadata = read_metadata(&mut reader)?;
        let rg = &metadata.row_groups[0];

        // col1 > 12
        {
            let filter = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "gt".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: ColumnBindingBuilder::new(
                            "col1".to_string(),
                            0,
                            Box::new(DataType::Number(NumberDataType::Int32)),
                            Visibility::Visible,
                        )
                        .build(),
                    }),
                    ScalarExpr::ConstantExpr(ConstantExpr {
                        span: None,
                        value: Scalar::Number(NumberScalar::Int32(12)),
                    }),
                ],
            });
            let filter = filter
                .as_expr()?
                .project_column_ref(|col| col.column_name.clone());
            let pruners = build_column_page_pruners(FunctionContext::default(), &schema, &filter)?;
            let row_selection = filter_pages(&mut reader, &schema, rg, &pruners)?;

            assert_eq!(Vec::<Interval>::new(), row_selection);
        }

        // col1 <= 5
        {
            let filter = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "lte".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: ColumnBindingBuilder::new(
                            "col1".to_string(),
                            0,
                            Box::new(DataType::Number(NumberDataType::Int32)),
                            Visibility::Visible,
                        )
                        .build(),
                    }),
                    ScalarExpr::ConstantExpr(ConstantExpr {
                        span: None,
                        value: Scalar::Number(NumberScalar::Int32(5)),
                    }),
                ],
            });
            let filter = filter
                .as_expr()?
                .project_column_ref(|col| col.column_name.clone());
            let pruners = build_column_page_pruners(FunctionContext::default(), &schema, &filter)?;
            let row_selection = filter_pages(&mut reader, &schema, rg, &pruners)?;

            assert_eq!(vec![Interval::new(0, 7)], row_selection);
        }

        // col1 > 10
        {
            let filter = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "gt".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: ColumnBindingBuilder::new(
                            "col1".to_string(),
                            0,
                            Box::new(DataType::Number(NumberDataType::Int32)),
                            Visibility::Visible,
                        )
                        .build(),
                    }),
                    ScalarExpr::ConstantExpr(ConstantExpr {
                        span: None,
                        value: Scalar::Number(NumberScalar::Int32(10)),
                    }),
                ],
            });
            let filter = filter
                .as_expr()?
                .project_column_ref(|col| col.column_name.clone());
            let pruners = build_column_page_pruners(FunctionContext::default(), &schema, &filter)?;
            let row_selection = filter_pages(&mut reader, &schema, rg, &pruners)?;

            assert_eq!(vec![Interval::new(7, 2)], row_selection);
        }

        // col1 <= 10
        {
            let filter = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "lte".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: ColumnBindingBuilder::new(
                            "col1".to_string(),
                            0,
                            Box::new(DataType::Number(NumberDataType::Int32)),
                            Visibility::Visible,
                        )
                        .build(),
                    }),
                    ScalarExpr::ConstantExpr(ConstantExpr {
                        span: None,
                        value: Scalar::Number(NumberScalar::Int32(10)),
                    }),
                ],
            });
            let filter = filter
                .as_expr()?
                .project_column_ref(|col| col.column_name.clone());
            let pruners = build_column_page_pruners(FunctionContext::default(), &schema, &filter)?;
            let row_selection = filter_pages(&mut reader, &schema, rg, &pruners)?;

            assert_eq!(vec![Interval::new(0, 9)], row_selection);
        }

        Ok(())
    }
}
