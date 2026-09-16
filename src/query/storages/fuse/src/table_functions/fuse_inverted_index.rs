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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_args::string_value;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::VariantType;
use databend_storages_common_index::INVERTED_INDEX_BUNDLE_INITIAL_FOOTER_READ_SIZE;
use databend_storages_common_index::INVERTED_INDEX_FILE_FORMAT_VERSION;
use databend_storages_common_index::InvertedIndexBundleFooter;
use databend_storages_common_table_meta::meta::BlockIndexMeta;
use databend_storages_common_table_meta::meta::SegmentInfo;
use futures::StreamExt;
use jsonb::Object as JsonbObject;
use jsonb::Value as JsonbValue;
use opendal::Operator;

use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::table_functions::SimpleTableFunc;
use crate::table_functions::string_literal;

const FUSE_INVERTED_INDEX: &str = "fuse_inverted_index";

#[derive(Clone)]
struct FuseInvertedIndexArgs {
    database_name: String,
    table_name: String,
}

impl From<&FuseInvertedIndexArgs> for TableArgs {
    fn from(args: &FuseInvertedIndexArgs) -> Self {
        TableArgs::new_positioned(vec![
            string_literal(&args.database_name),
            string_literal(&args.table_name),
        ])
    }
}

/// Inspects inverted-index bundle footers for a Fuse table.
///
/// Usage: `SELECT * FROM fuse_inverted_index('<database>', '<table>')`.
/// One row is returned per bundle object referenced by `BlockIndexMeta`. If a block has
/// multiple indexes, it produces multiple rows. The bundle's raw Tantivy files are listed in
/// `footer.raw_files`. A damaged index has a null `footer` and a diagnostic in `error` without
/// preventing other index rows from being returned.
pub struct FuseInvertedIndexTable {
    args: FuseInvertedIndexArgs,
}

#[async_trait::async_trait]
impl SimpleTableFunc for FuseInvertedIndexTable {
    fn get_engine_name(&self) -> String {
        FUSE_INVERTED_INDEX.to_string()
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some((&self.args).into())
    }

    fn schema(&self) -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("snapshot_id", TableDataType::String),
            TableField::new("segment_location", TableDataType::String),
            TableField::new("block_location", TableDataType::String),
            TableField::new("row_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("index_name", TableDataType::String),
            TableField::new("index_version", TableDataType::String),
            TableField::new("index_location", TableDataType::String),
            TableField::new(
                "index_format_version",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("index_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("footer", TableDataType::Variant.wrap_nullable()),
            TableField::new("error", TableDataType::String.wrap_nullable()),
        ])
    }

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        plan: &DataSourcePlan,
    ) -> Result<Option<DataBlock>> {
        let catalog_name = ctx.get_current_catalog();
        let table = ctx
            .get_catalog(&catalog_name)
            .await?
            .get_table(
                &ctx.get_tenant(),
                &self.args.database_name,
                &self.args.table_name,
            )
            .await?;
        let table = FuseTable::try_from_table(table.as_ref()).map_err(|_| {
            ErrorCode::StorageOther(
                "Invalid table engine, only FUSE table supports fuse_inverted_index",
            )
        })?;
        let Some(snapshot) = table.read_table_snapshot().await? else {
            return Ok(Some(DataBlock::empty_with_schema(&self.schema().into())));
        };
        // A pushed-down limit is only safe without ORDER BY. Otherwise the upper Sort must choose
        // the final rows after this function has returned all index diagnostics.
        let limit = plan
            .push_downs
            .as_ref()
            .and_then(|push_down| {
                if push_down.order_by.is_empty() {
                    push_down.limit
                } else {
                    None
                }
            })
            .unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Some(DataBlock::empty_with_schema(&self.schema().into())));
        }

        let snapshot_id = snapshot.snapshot_id.simple().to_string();
        let mut snapshot_ids = Vec::new();
        let mut segment_locations = Vec::new();
        let mut block_locations = Vec::new();
        let mut row_counts = Vec::new();
        let mut index_names = Vec::new();
        let mut index_versions = Vec::new();
        let mut index_locations = Vec::new();
        let mut index_format_versions = Vec::new();
        let mut index_sizes = Vec::new();
        let mut footers = Vec::new();
        let mut errors = Vec::new();

        let segments_io = SegmentsIO::create(ctx.clone(), table.get_operator(), table.schema());
        let concurrency = (ctx.get_settings().get_max_threads()? as usize)
            .saturating_mul(2)
            .max(1);
        let chunk_size = concurrency.min(snapshot.segments.len()).max(1);
        'outer: for segment_chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(segment_chunk, true)
                .await?;
            for (segment_location, segment) in segment_chunk.iter().zip(segments) {
                let segment = segment?;
                let operator = table.get_operator();
                let remaining = limit.saturating_sub(snapshot_ids.len());
                let mut tasks = Vec::with_capacity(remaining.min(segment.blocks.len()));
                'blocks: for block in &segment.blocks {
                    for meta in block.inverted_index_metas.as_deref().unwrap_or_default() {
                        tasks.push(IndexInspectionTask {
                            block_location: block.location.0.clone(),
                            row_count: block.row_count,
                            meta: meta.clone(),
                        });
                        if tasks.len() >= remaining {
                            break 'blocks;
                        }
                    }
                }
                let rows = futures::stream::iter(tasks)
                    .map(|task| {
                        let operator = operator.clone();
                        async move { inspect_index(&operator, task).await }
                    })
                    .buffered(concurrency)
                    .collect::<Vec<_>>()
                    .await;

                for row in rows {
                    snapshot_ids.push(snapshot_id.clone());
                    segment_locations.push(segment_location.0.clone());
                    block_locations.push(row.block_location);
                    row_counts.push(row.row_count);
                    index_names.push(row.index_name);
                    index_versions.push(row.index_version);
                    index_locations.push(row.index_location);
                    index_format_versions.push(row.index_format_version);
                    index_sizes.push(row.index_size);
                    footers.push(row.footer);
                    errors.push(row.error);
                    if snapshot_ids.len() >= limit {
                        break 'outer;
                    }
                }
            }
        }

        Ok(Some(DataBlock::new_from_columns(vec![
            StringType::from_data(snapshot_ids),
            StringType::from_data(segment_locations),
            StringType::from_data(block_locations),
            UInt64Type::from_data(row_counts),
            StringType::from_data(index_names),
            StringType::from_data(index_versions),
            StringType::from_data(index_locations),
            UInt64Type::from_data(index_format_versions),
            UInt64Type::from_data(index_sizes),
            VariantType::from_opt_data(footers),
            StringType::from_opt_data(errors),
        ])))
    }

    fn create(func_name: &str, table_args: TableArgs) -> Result<Self> {
        let args = table_args.expect_all_positioned(func_name, Some(2))?;
        Ok(Self {
            args: FuseInvertedIndexArgs {
                database_name: string_value(&args[0])?,
                table_name: string_value(&args[1])?,
            },
        })
    }
}

struct IndexInspectionTask {
    block_location: String,
    row_count: u64,
    meta: BlockIndexMeta,
}

struct IndexInspection {
    block_location: String,
    row_count: u64,
    index_name: String,
    index_version: String,
    index_location: String,
    index_format_version: u64,
    index_size: u64,
    footer: Option<Vec<u8>>,
    error: Option<String>,
}

async fn inspect_index(operator: &Operator, task: IndexInspectionTask) -> IndexInspection {
    let result = load_footer(operator, &task.meta).await;
    let (footer, error) = match result {
        Ok(footer) => (Some(footer_to_jsonb(footer)), None),
        Err(error) => (None, Some(error.to_string())),
    };
    IndexInspection {
        block_location: task.block_location,
        row_count: task.row_count,
        index_name: task.meta.index_name,
        index_version: task.meta.index_version,
        index_location: task.meta.location.0,
        index_format_version: task.meta.location.1,
        index_size: task.meta.size,
        footer,
        error,
    }
}

async fn load_footer(
    operator: &Operator,
    meta: &BlockIndexMeta,
) -> Result<InvertedIndexBundleFooter> {
    if meta.location.1 != INVERTED_INDEX_FILE_FORMAT_VERSION {
        return Err(ErrorCode::StorageOther(format!(
            "unsupported inverted-index object format {}; expected {}",
            meta.location.1, INVERTED_INDEX_FILE_FORMAT_VERSION
        )));
    }
    let initial_read_size = u64::try_from(INVERTED_INDEX_BUNDLE_INITIAL_FOOTER_READ_SIZE)
        .map_err(|_| ErrorCode::StorageOther("inverted-index footer read size is invalid"))?;
    let tail_start = meta.size.saturating_sub(initial_read_size);
    let tail = operator
        .read_with(&meta.location.0)
        .range(tail_start..meta.size)
        .await?;
    let tail_bytes = tail.to_bytes();
    let footer_start = InvertedIndexBundleFooter::footer_start_from_tail(
        tail_bytes.as_ref(),
        meta.size,
        tail_start,
    )?;
    if footer_start >= tail_start {
        return Ok(InvertedIndexBundleFooter::parse_footer_from_tail(
            tail_bytes.as_ref(),
            meta.size,
            tail_start,
        )?);
    }

    let footer = operator
        .read_with(&meta.location.0)
        .range(footer_start..meta.size)
        .await?;
    let footer_bytes = footer.to_bytes();
    Ok(InvertedIndexBundleFooter::open_footer_for_object(
        footer_bytes.as_ref(),
        meta.size,
        Some(footer_start),
    )?)
}

fn footer_to_jsonb(footer: InvertedIndexBundleFooter) -> Vec<u8> {
    let InvertedIndexBundleFooter {
        bundle_version,
        footer_start,
        footer_size,
        file_ranges,
        open_slices,
        managed_json,
        meta_json,
    } = footer;
    let raw_file_count = file_ranges.files.len();
    let raw_files = file_ranges
        .files
        .iter()
        .map(|(path, range)| {
            let mut file = JsonbObject::new();
            file.insert(
                "path".to_string(),
                path.to_string_lossy().into_owned().into(),
            );
            file.insert("start".to_string(), range.start.into());
            file.insert("end".to_string(), range.end.into());
            file.insert("length".to_string(), (range.end - range.start).into());
            JsonbValue::Object(file)
        })
        .collect::<Vec<_>>();
    let open_slice_count = open_slices
        .values()
        .map(|slices| slices.len())
        .sum::<usize>();
    let open_slice_bytes = open_slices
        .values()
        .flat_map(|slices| slices.iter())
        .map(|slice| slice.bytes.len())
        .sum::<usize>();
    let open_slices = open_slices
        .iter()
        .map(|(path, slices)| {
            let slices = slices
                .iter()
                .map(|slice| {
                    let mut item = JsonbObject::new();
                    item.insert("start".to_string(), slice.range.start.into());
                    item.insert("end".to_string(), slice.range.end.into());
                    item.insert(
                        "length".to_string(),
                        (slice.range.end - slice.range.start).into(),
                    );
                    JsonbValue::Object(item)
                })
                .collect::<Vec<_>>();
            let mut file = JsonbObject::new();
            file.insert(
                "path".to_string(),
                path.to_string_lossy().into_owned().into(),
            );
            file.insert("slices".to_string(), JsonbValue::Array(slices));
            JsonbValue::Object(file)
        })
        .collect::<Vec<_>>();

    let mut object = JsonbObject::new();
    object.insert("bundle_version".to_string(), (bundle_version as u32).into());
    object.insert("footer_start".to_string(), footer_start.into());
    object.insert("footer_size".to_string(), footer_size.into());
    object.insert("raw_files".to_string(), JsonbValue::Array(raw_files));
    object.insert("raw_file_count".to_string(), raw_file_count.into());
    object.insert("open_slices".to_string(), JsonbValue::Array(open_slices));
    object.insert("open_slice_count".to_string(), open_slice_count.into());
    object.insert("open_slice_bytes".to_string(), open_slice_bytes.into());
    object.insert(
        "managed_json".to_string(),
        inline_json(managed_json.as_ref()),
    );
    object.insert("meta_json".to_string(), inline_json(meta_json.as_ref()));
    JsonbValue::Object(object).to_vec()
}

fn inline_json(bytes: &[u8]) -> JsonbValue<'_> {
    let mut object = JsonbObject::new();
    object.insert("size".to_string(), bytes.len().into());
    match jsonb::parse_value(bytes) {
        Ok(value) => {
            object.insert("value".to_string(), value);
        }
        Err(error) => {
            object.insert("text".to_string(), String::from_utf8_lossy(bytes).into());
            object.insert("parse_error".to_string(), error.to_string().into());
        }
    }
    JsonbValue::Object(object)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_requires_database_and_table() {
        let table = FuseInvertedIndexTable::create(
            FUSE_INVERTED_INDEX,
            TableArgs::new_positioned(vec![string_literal("db"), string_literal("table")]),
        )
        .unwrap();
        assert_eq!(table.args.database_name, "db");
        assert_eq!(table.args.table_name, "table");
        assert_eq!(table.get_engine_name(), FUSE_INVERTED_INDEX);
        let schema = table.schema();
        assert_eq!(schema.num_fields(), 11);
        assert_eq!(schema.field(0).name(), "snapshot_id");
        assert_eq!(schema.field(2).name(), "block_location");
        assert_eq!(schema.field(4).name(), "index_name");
        assert_eq!(schema.field(9).name(), "footer");
        assert_eq!(
            schema.field(9).data_type(),
            &TableDataType::Variant.wrap_nullable()
        );
        assert_eq!(schema.field(10).name(), "error");
        assert_eq!(
            schema.field(10).data_type(),
            &TableDataType::String.wrap_nullable()
        );

        assert!(
            FuseInvertedIndexTable::create(
                FUSE_INVERTED_INDEX,
                TableArgs::new_positioned(vec![string_literal("db")]),
            )
            .is_err()
        );
    }

    #[test]
    fn test_inline_json_diagnostics() {
        let JsonbValue::Object(valid) = inline_json(br#"{"value":1}"#) else {
            panic!("inline JSON must be an object");
        };
        assert!(matches!(valid.get("value"), Some(JsonbValue::Object(_))));

        let JsonbValue::Object(invalid) = inline_json(b"not-json") else {
            panic!("inline JSON must be an object");
        };
        assert!(matches!(invalid.get("size"), Some(JsonbValue::Number(_))));
        assert!(matches!(invalid.get("text"), Some(JsonbValue::String(_))));
        assert!(matches!(
            invalid.get("parse_error"),
            Some(JsonbValue::String(_))
        ));
    }
}
