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
use std::collections::BTreeSet;
use std::sync::Arc;

use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnRef;
use databend_common_expression::Constant;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableSchema;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::always_callback;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use futures::TryStreamExt;
use jsonb::RawJsonb;
use jsonb::from_raw_jsonb;
use serde::Deserialize;

use crate::interpreters::QueryFinishHooks;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::TableScan;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::schedulers::build_local_pipeline;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextQueryState;
use crate::stream::PullingExecutorStream;

const HISTORY_DATABASE: &str = "system_history";
const LINEAGE_TABLE: &str = "lineage_history";
// Keep each pushed-down key filter large enough to avoid too many child scans, but small
// enough that expression construction and pruning stay bounded. Frontier keys come from
// BTreeSet, so batches are deterministic and sorted by lineage key.
const FRONTIER_BATCH_SIZE: usize = 512;
const FIRST_LINEAGE_SCAN_ID: usize = 1_000;

pub(super) const EDGE_COLUMNS: &[&str] = &[
    "updated_on",
    "user_name",
    "query_parameterized_hash",
    "lineage_kind",
    "column_lineage_hash",
    "source_lineage_key",
    "source_address_kind",
    "source_catalog_type",
    "source_object_type",
    "source_catalog",
    "source_database",
    "source_name",
    "source_id",
    "target_lineage_key",
    "target_address_kind",
    "target_catalog_type",
    "target_object_type",
    "target_catalog",
    "target_database",
    "target_name",
    "target_id",
    "query_info",
    "column_lineage",
];

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum AddressKind {
    Id,
    Name,
}

impl AddressKind {
    fn parse(value: Option<String>) -> Result<Self> {
        match value.as_deref() {
            Some("ID") => Ok(Self::Id),
            Some("NAME") => Ok(Self::Name),
            other => Err(ErrorCode::Internal(format!(
                "invalid lineage address kind: {other:?}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum LineageObjectType {
    Table,
    View,
    Stage,
}

impl LineageObjectType {
    fn parse(value: Option<String>) -> Result<Self> {
        match value.as_deref() {
            Some("TABLE") => Ok(Self::Table),
            Some("VIEW") => Ok(Self::View),
            Some("STAGE") => Ok(Self::Stage),
            other => Err(ErrorCode::Internal(format!(
                "invalid lineage object type: {other:?}"
            ))),
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Table => "TABLE",
            Self::View => "VIEW",
            Self::Stage => "STAGE",
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct CapturedObject {
    pub lineage_key: String,
    pub address_kind: AddressKind,
    pub catalog_type: String,
    pub object_type: LineageObjectType,
    pub catalog: String,
    pub database: String,
    pub name: String,
    pub id: Option<u64>,
}

impl CapturedObject {
    pub(super) fn is_default_catalog(&self) -> bool {
        self.catalog_type.eq_ignore_ascii_case("DEFAULT")
    }
}

#[derive(Clone, Debug)]
pub(super) struct RawLineageEdge {
    pub updated_on: Option<i64>,
    pub user_name: Option<String>,
    pub query_parameterized_hash: Option<String>,
    pub query_info: LineageQueryInfo,
    pub lineage_kind: Option<String>,
    pub column_lineage_hash: String,
    pub source: CapturedObject,
    pub target: CapturedObject,
    pub source_column_address_kind: Option<AddressKind>,
    pub target_column_address_kind: Option<AddressKind>,
    pub source_to_target_columns: BTreeMap<String, Vec<String>>,
    pub target_to_source_columns: BTreeMap<String, Vec<String>>,
}

impl RawLineageEdge {
    pub(super) fn newer_than(&self, other: &Self) -> bool {
        (
            self.updated_on,
            self.query_info.query_id.as_deref().unwrap_or_default(),
            self.lineage_kind.as_deref().unwrap_or_default(),
            self.column_lineage_hash.as_str(),
        ) > (
            other.updated_on,
            other.query_info.query_id.as_deref().unwrap_or_default(),
            other.lineage_kind.as_deref().unwrap_or_default(),
            other.column_lineage_hash.as_str(),
        )
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub(super) struct LineageQueryInfo {
    pub query_id: Option<String>,
    pub query_text: Option<String>,
    pub query_duration_ms: Option<i64>,
    pub written_rows: Option<u64>,
    pub scan_rows: Option<u64>,
}

#[derive(Deserialize)]
struct PersistedColumnLineage {
    source_column_address_kind: String,
    target_column_address_kind: String,
    mappings: Vec<PersistedColumnMapping>,
}

#[derive(Deserialize)]
struct PersistedColumnMapping {
    target: PersistedColumnIdentity,
    sources: Vec<PersistedColumnIdentity>,
}

#[derive(Deserialize)]
struct PersistedColumnIdentity {
    name: String,
    id: Option<u32>,
}

#[derive(Default)]
struct DecodedColumnLineage {
    source_address_kind: Option<AddressKind>,
    target_address_kind: Option<AddressKind>,
    source_to_target: BTreeMap<String, Vec<String>>,
    target_to_source: BTreeMap<String, Vec<String>>,
}

pub(super) struct LineageEdgeReader {
    ctx: Arc<dyn TableContext>,
    table: Arc<dyn Table>,
    next_scan_id: usize,
}

impl LineageEdgeReader {
    pub(super) async fn try_create(ctx: Arc<dyn TableContext>) -> Result<Self> {
        let table = ctx
            .get_table(CATALOG_DEFAULT, HISTORY_DATABASE, LINEAGE_TABLE)
            .await?;
        Ok(Self {
            ctx,
            table,
            next_scan_id: FIRST_LINEAGE_SCAN_ID,
        })
    }

    /// Read every edge whose directional lineage key is in the current frontier.
    ///
    /// The table object is pinned for the lifetime of the reader, so every level reads the same
    /// Fuse snapshot even if the history transform commits concurrently. Scan ids are allocated
    /// monotonically per batch instead of reserving a fixed-size range for each distance level.
    pub(super) async fn read_frontier(
        &mut self,
        match_column: &str,
        frontier: &BTreeSet<String>,
    ) -> Result<Vec<RawLineageEdge>> {
        if frontier.is_empty() {
            return Ok(vec![]);
        }

        let mut edges = Vec::new();
        let keys = frontier.iter().cloned().collect::<Vec<_>>();
        for chunk in keys.chunks(FRONTIER_BATCH_SIZE) {
            let scan_id = self.next_scan_id;
            self.next_scan_id = self
                .next_scan_id
                .checked_add(1)
                .ok_or_else(|| ErrorCode::Internal("lineage scan id overflow"))?;
            edges.extend(self.read_batch(match_column, chunk, scan_id).await?);
        }
        Ok(edges)
    }

    async fn read_batch(
        &self,
        match_column: &str,
        keys: &[String],
        scan_id: usize,
    ) -> Result<Vec<RawLineageEdge>> {
        let parent_ctx = self
            .ctx
            .as_any()
            .downcast_ref::<QueryContext>()
            .ok_or_else(|| ErrorCode::Internal("GET_LINEAGE requires QueryContext"))?;
        let child_ctx = QueryContext::create_from(parent_ctx);
        if child_ctx.check_aborting().is_err() {
            return Err(ErrorCode::AbortedQuery("GET_LINEAGE query was aborted"));
        }
        let schema = self.table.schema();
        let projection = Projection::from_column_names(&schema, EDGE_COLUMNS)?;
        let filters = build_key_filter(&schema, match_column, keys)?;
        let push_downs = PushDownInfo {
            projection: Some(projection),
            filters: Some(filters),
            ..Default::default()
        };

        let mut source = self
            .table
            .read_plan(child_ctx.clone(), Some(push_downs), None, false, false)
            .await?;
        source.scan_id = scan_id;
        let output_schema = source.output_schema.clone();
        let name_mapping = source
            .output_schema
            .fields()
            .iter()
            .map(|field| (field.name().to_string(), field.name().to_string()))
            .collect();
        let physical_plan = PhysicalPlan::new(TableScan {
            meta: PhysicalPlanMeta::new("LineageEdgeScan"),
            scan_id,
            name_mapping,
            source: Box::new(source),
            internal_column: None,
            table_index: None,
            stat_info: None,
        });

        let mut build_res = build_local_pipeline(&child_ctx, &physical_plan).await?;
        build_res.main_pipeline.set_on_finished(always_callback(
            QueryFinishHooks::nested().into_callback(child_ctx.clone()),
        ));
        let settings = ExecutorSettings::try_create(child_ctx.clone())?;
        let pulling_executor = PipelinePullingExecutor::from_pipelines(build_res, settings)?;
        let child_executor = pulling_executor.get_inner();
        let abort_notify = child_ctx.get_abort_notify();
        let match_column = match_column.to_string();
        let frontier = keys.iter().cloned().collect::<BTreeSet<_>>();

        // Child contexts share the outer executor slot, so set_executor() would hide the outer
        // pipeline from query cancellation. Keep it registered and explicitly forward the shared
        // abort notification to this child executor instead.
        let mut stream = PullingExecutorStream::create(pulling_executor)?;
        let abort = abort_notify.notified();
        tokio::pin!(abort);
        let mut edges = Vec::new();
        loop {
            tokio::select! {
                block = stream.try_next() => match block? {
                    Some(block) => edges.extend(decode_edges(
                        &block,
                        &output_schema,
                        &match_column,
                        &frontier,
                    )?),
                    None => return Ok(edges),
                },
                () = &mut abort => {
                    let cause = ErrorCode::AbortedQuery(
                        "GET_LINEAGE child scan was aborted",
                    );
                    child_executor.finish(Some(cause.clone()));
                    return Err(cause);
                }
            }
        }
    }
}

pub(super) fn build_key_filter(
    schema: &TableSchema,
    column: &str,
    keys: &[String],
) -> Result<Filters> {
    let field = schema.field_with_name(column)?;
    let data_type = DataType::from(field.data_type());
    let column_expr = || {
        Expr::ColumnRef(ColumnRef {
            span: None,
            id: column.to_string(),
            data_type: data_type.clone(),
            display_name: column.to_string(),
        })
    };

    let predicates = keys
        .iter()
        .map(|key| {
            check_function(
                None,
                "eq",
                &[],
                &[
                    column_expr(),
                    Expr::Constant(Constant {
                        span: None,
                        scalar: Scalar::String(key.clone()),
                        data_type: DataType::String,
                    }),
                ],
                &BUILTIN_FUNCTIONS,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let filter = if predicates.len() == 1 {
        predicates[0].clone()
    } else {
        check_function(None, "or_filters", &[], &predicates, &BUILTIN_FUNCTIONS)?
    };
    let inverted_filter = check_function(None, "not", &[], &[filter.clone()], &BUILTIN_FUNCTIONS)?;
    Ok(Filters {
        filter: filter.as_remote_expr(),
        inverted_filter: inverted_filter.as_remote_expr(),
    })
}

pub(super) fn decode_edges(
    block: &DataBlock,
    schema: &TableSchema,
    match_column: &str,
    frontier: &BTreeSet<String>,
) -> Result<Vec<RawLineageEdge>> {
    let offset = |name: &str| schema.index_of(name);
    let mut edges = Vec::with_capacity(block.num_rows());
    for row in 0..block.num_rows() {
        let match_key = required_string(block, offset(match_column)?, row)?;
        if !frontier.contains(&match_key) {
            continue;
        }

        let source = CapturedObject {
            lineage_key: required_string(block, offset("source_lineage_key")?, row)?,
            address_kind: AddressKind::parse(optional_string(
                block,
                offset("source_address_kind")?,
                row,
            )?)?,
            catalog_type: optional_string(block, offset("source_catalog_type")?, row)?
                .unwrap_or_default(),
            object_type: LineageObjectType::parse(optional_string(
                block,
                offset("source_object_type")?,
                row,
            )?)?,
            catalog: optional_string(block, offset("source_catalog")?, row)?.unwrap_or_default(),
            database: optional_string(block, offset("source_database")?, row)?.unwrap_or_default(),
            name: optional_string(block, offset("source_name")?, row)?.unwrap_or_default(),
            id: optional_u64(block, offset("source_id")?, row)?,
        };
        let target = CapturedObject {
            lineage_key: required_string(block, offset("target_lineage_key")?, row)?,
            address_kind: AddressKind::parse(optional_string(
                block,
                offset("target_address_kind")?,
                row,
            )?)?,
            catalog_type: optional_string(block, offset("target_catalog_type")?, row)?
                .unwrap_or_default(),
            object_type: LineageObjectType::parse(optional_string(
                block,
                offset("target_object_type")?,
                row,
            )?)?,
            catalog: optional_string(block, offset("target_catalog")?, row)?.unwrap_or_default(),
            database: optional_string(block, offset("target_database")?, row)?.unwrap_or_default(),
            name: optional_string(block, offset("target_name")?, row)?.unwrap_or_default(),
            id: optional_u64(block, offset("target_id")?, row)?,
        };
        let column_lineage = decode_column_lineage(block, offset("column_lineage")?, row)?;
        edges.push(RawLineageEdge {
            updated_on: optional_timestamp(block, offset("updated_on")?, row)?,
            user_name: optional_string(block, offset("user_name")?, row)?,
            query_parameterized_hash: optional_string(
                block,
                offset("query_parameterized_hash")?,
                row,
            )?,
            query_info: optional_variant(block, offset("query_info")?, row)?.unwrap_or_default(),
            lineage_kind: optional_string(block, offset("lineage_kind")?, row)?,
            column_lineage_hash: required_string(block, offset("column_lineage_hash")?, row)?,
            source,
            target,
            source_column_address_kind: column_lineage.source_address_kind,
            target_column_address_kind: column_lineage.target_address_kind,
            source_to_target_columns: column_lineage.source_to_target,
            target_to_source_columns: column_lineage.target_to_source,
        });
    }
    Ok(edges)
}

fn scalar_at(block: &DataBlock, offset: usize, row: usize) -> Result<ScalarRef<'_>> {
    block
        .get_by_offset(offset)
        .index(row)
        .ok_or_else(|| ErrorCode::Internal("lineage block row is out of bounds"))
}

fn optional_string(block: &DataBlock, offset: usize, row: usize) -> Result<Option<String>> {
    match scalar_at(block, offset, row)? {
        ScalarRef::Null => Ok(None),
        ScalarRef::String(value) => Ok(Some(value.to_string())),
        value => Err(ErrorCode::Internal(format!(
            "expected lineage string, got {value:?}"
        ))),
    }
}

fn required_string(block: &DataBlock, offset: usize, row: usize) -> Result<String> {
    optional_string(block, offset, row)?
        .ok_or_else(|| ErrorCode::Internal("required lineage string is NULL"))
}

fn optional_u64(block: &DataBlock, offset: usize, row: usize) -> Result<Option<u64>> {
    match scalar_at(block, offset, row)? {
        ScalarRef::Null => Ok(None),
        ScalarRef::Number(NumberScalar::UInt64(value)) => Ok(Some(value)),
        value => Err(ErrorCode::Internal(format!(
            "expected lineage UInt64, got {value:?}"
        ))),
    }
}

fn optional_timestamp(block: &DataBlock, offset: usize, row: usize) -> Result<Option<i64>> {
    match scalar_at(block, offset, row)? {
        ScalarRef::Null => Ok(None),
        ScalarRef::Timestamp(value) => Ok(Some(value)),
        value => Err(ErrorCode::Internal(format!(
            "expected lineage timestamp, got {value:?}"
        ))),
    }
}

fn optional_variant<T>(block: &DataBlock, offset: usize, row: usize) -> Result<Option<T>>
where T: for<'de> Deserialize<'de> {
    match scalar_at(block, offset, row)? {
        ScalarRef::Null => Ok(None),
        ScalarRef::Variant(value) => deserialize_optional_variant(value),
        value => Err(ErrorCode::Internal(format!(
            "expected lineage variant, got {value:?}"
        ))),
    }
}

fn deserialize_optional_variant<T>(value: &[u8]) -> Result<Option<T>>
where T: for<'de> Deserialize<'de> {
    from_raw_jsonb::<Option<T>>(&RawJsonb::new(value))
        .map_err(|err| ErrorCode::Internal(format!("invalid lineage variant: {err}")))
}

fn decode_column_lineage(
    block: &DataBlock,
    offset: usize,
    row: usize,
) -> Result<DecodedColumnLineage> {
    let Some(lineage) = optional_variant::<PersistedColumnLineage>(block, offset, row)? else {
        return Ok(DecodedColumnLineage::default());
    };
    decode_persisted_column_lineage(lineage)
}

fn decode_persisted_column_lineage(
    lineage: PersistedColumnLineage,
) -> Result<DecodedColumnLineage> {
    let source_address_kind = AddressKind::parse(Some(lineage.source_column_address_kind))?;
    let target_address_kind = AddressKind::parse(Some(lineage.target_column_address_kind))?;
    let mut source_to_target = BTreeMap::<String, BTreeSet<String>>::new();
    let mut target_to_source = BTreeMap::<String, BTreeSet<String>>::new();
    for mapping in lineage.mappings {
        let target = persisted_column_address(target_address_kind, mapping.target)?;
        for source in mapping.sources {
            let source = persisted_column_address(source_address_kind, source)?;
            source_to_target
                .entry(source.clone())
                .or_default()
                .insert(target.clone());
            target_to_source
                .entry(target.clone())
                .or_default()
                .insert(source);
        }
    }
    let into_vec_map = |map: BTreeMap<String, BTreeSet<String>>| {
        map.into_iter()
            .map(|(key, values)| (key, values.into_iter().collect()))
            .collect()
    };
    Ok(DecodedColumnLineage {
        source_address_kind: Some(source_address_kind),
        target_address_kind: Some(target_address_kind),
        source_to_target: into_vec_map(source_to_target),
        target_to_source: into_vec_map(target_to_source),
    })
}

fn persisted_column_address(
    address_kind: AddressKind,
    column: PersistedColumnIdentity,
) -> Result<String> {
    match address_kind {
        AddressKind::Id => column.id.map(|id| id.to_string()).ok_or_else(|| {
            ErrorCode::Internal(format!(
                "ID-addressed lineage column '{}' has no id",
                column.name
            ))
        }),
        AddressKind::Name => Ok(column.name),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use jsonb::OwnedJsonb;

    use super::*;

    #[test]
    fn test_decode_column_lineage_builds_both_directions() -> Result<()> {
        let decoded = decode_persisted_column_lineage(PersistedColumnLineage {
            source_column_address_kind: "NAME".to_string(),
            target_column_address_kind: "ID".to_string(),
            mappings: vec![PersistedColumnMapping {
                target: PersistedColumnIdentity {
                    name: "x".to_string(),
                    id: Some(3),
                },
                sources: vec![
                    PersistedColumnIdentity {
                        name: "a".to_string(),
                        id: None,
                    },
                    PersistedColumnIdentity {
                        name: "b".to_string(),
                        id: None,
                    },
                ],
            }],
        })?;

        assert_eq!(decoded.source_address_kind, Some(AddressKind::Name));
        assert_eq!(decoded.target_address_kind, Some(AddressKind::Id));
        assert_eq!(
            decoded.source_to_target,
            BTreeMap::from([
                ("a".to_string(), vec!["3".to_string()]),
                ("b".to_string(), vec!["3".to_string()]),
            ])
        );
        assert_eq!(
            decoded.target_to_source,
            BTreeMap::from([("3".to_string(), vec!["a".to_string(), "b".to_string()])])
        );
        Ok(())
    }

    #[test]
    fn test_id_addressed_column_requires_id() {
        let result = persisted_column_address(AddressKind::Id, PersistedColumnIdentity {
            name: "a".to_string(),
            id: None,
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_json_null_column_lineage_is_absent() {
        let value = OwnedJsonb::from_str("null").expect("valid JSON");
        let decoded = deserialize_optional_variant::<PersistedColumnLineage>(value.as_ref())
            .expect("valid lineage variant");
        assert!(decoded.is_none());
    }
}
