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

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use chrono::DateTime;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_args::i64_value;
use databend_common_catalog::table_args::string_value;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::infer_table_schema;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::StringType;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_api::LineageApi;
use databend_common_meta_api::ListLineageReq;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_app::schema::ColumnRef;
use databend_common_meta_app::schema::LineageColumn;
use databend_common_meta_app::schema::LineageDetail;
use databend_common_meta_app::schema::LineageDirection;
use databend_common_meta_app::schema::LineageIdentity;
use databend_common_meta_app::schema::LineageKey;
use databend_common_meta_app::schema::LineageKind;
use databend_common_meta_app::schema::LineageObjectRef;
use databend_common_meta_app::schema::LineageObjectType;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_store::MetaStore;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_users::UserApiProvider;
use futures::TryStreamExt;
use futures::stream;
use futures::stream::StreamExt;
use serde_json::json;

use crate::meta_service_error;
use crate::sessions::TableContext;
use crate::sql::Planner;
use crate::table_functions::object_name::TableNameParser;

const GET_LINEAGE_FUNC: &str = "get_lineage";
const GET_LINEAGE_ENGINE: &str = "GET_LINEAGE";
const DEFAULT_DISTANCE: u8 = 5;
const MAX_DISTANCE: u8 = 5;
const LINEAGE_META_LOOKUP_MAX_CONCURRENCY: usize = 8;

pub struct GetLineageTable {
    table_info: TableInfo,
    args: GetLineageArgs,
    table_args: TableArgs,
}

impl GetLineageTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args = GetLineageArgs::parse(&table_args)?;
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: Self::schema(),
                engine: GET_LINEAGE_ENGINE.to_string(),
                created_on: DateTime::from_timestamp(0, 0).unwrap(),
                updated_on: DateTime::from_timestamp(0, 0).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(Self {
            table_info,
            args,
            table_args,
        }))
    }

    fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new(
                "distance",
                TableDataType::Number(databend_common_expression::types::NumberDataType::Int32),
            ),
            TableField::new("source_object_domain", TableDataType::String),
            TableField::new("source_object_name", TableDataType::String),
            TableField::new(
                "source_column_name",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new("target_object_domain", TableDataType::String),
            TableField::new("target_object_name", TableDataType::String),
            TableField::new(
                "target_column_name",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new("target_status", TableDataType::String),
            TableField::new("process", TableDataType::String),
        ])
    }
}

#[async_trait::async_trait]
impl Table for GetLineageTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(self.table_args.clone())
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(ctx.get_license_key(), Feature::Lineage)?;

        pipeline.add_source(
            |output| {
                GetLineageSource::create(
                    ctx.clone(),
                    output,
                    self.args.clone(),
                    self.table_info.meta.schema.clone(),
                )
            },
            1,
        )?;
        Ok(())
    }
}

impl TableFunction for GetLineageTable {
    fn function_name(&self) -> &str {
        GET_LINEAGE_FUNC
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

struct GetLineageSource {
    ctx: Arc<dyn TableContext>,
    finished: bool,
    args: GetLineageArgs,
    schema: DataSchemaRef,
}

impl GetLineageSource {
    fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        args: GetLineageArgs,
        schema: TableSchemaRef,
    ) -> Result<ProcessorPtr> {
        let schema = Arc::new(DataSchema::from(schema.as_ref()));
        AsyncSourcer::create(ctx.get_scan_progress(), output, Self {
            ctx,
            finished: false,
            args,
            schema,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for GetLineageSource {
    const NAME: &'static str = GET_LINEAGE_FUNC;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }
        self.finished = true;

        let block =
            collect_lineage(self.ctx.clone(), self.args.clone(), self.schema.clone()).await?;
        Ok(Some(block))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GetLineageArgs {
    object_name: String,
    object_domain: ObjectDomain,
    direction: QueryDirection,
    distance: u8,
}

impl GetLineageArgs {
    fn parse(table_args: &TableArgs) -> Result<Self> {
        let args = table_args.expect_all_positioned(GET_LINEAGE_FUNC, None)?;
        if !(args.len() == 3 || args.len() == 4) {
            return Err(ErrorCode::BadArguments(format!(
                "{GET_LINEAGE_FUNC} requires 3 or 4 positioned arguments: object_name, object_domain, direction[, distance]"
            )));
        }

        let distance = if args.len() == 4 {
            let distance = i64_value(&args[3])?;
            if !(1..=MAX_DISTANCE as i64).contains(&distance) {
                return Err(ErrorCode::BadArguments(format!(
                    "distance must be an integer in the range [1, {MAX_DISTANCE}]"
                )));
            }
            distance as u8
        } else {
            DEFAULT_DISTANCE
        };

        Ok(Self {
            object_name: string_value(&args[0])?,
            object_domain: ObjectDomain::parse(&string_value(&args[1])?)?,
            direction: QueryDirection::parse(&string_value(&args[2])?)?,
            distance,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObjectDomain {
    Table,
    View,
    Column,
    Stage,
}

impl ObjectDomain {
    fn parse(input: &str) -> Result<Self> {
        match input.trim().to_ascii_uppercase().as_str() {
            "TABLE" => Ok(Self::Table),
            "VIEW" => Ok(Self::View),
            "COLUMN" => Ok(Self::Column),
            "STAGE" => Ok(Self::Stage),
            other => Err(ErrorCode::BadArguments(format!(
                "unsupported object_domain '{other}' for get_lineage"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryDirection {
    Upstream,
    Downstream,
}

impl QueryDirection {
    fn parse(input: &str) -> Result<Self> {
        match input.trim().to_ascii_uppercase().as_str() {
            "UPSTREAM" => Ok(Self::Upstream),
            "DOWNSTREAM" => Ok(Self::Downstream),
            other => Err(ErrorCode::BadArguments(format!(
                "unsupported direction '{other}', expected UPSTREAM or DOWNSTREAM"
            ))),
        }
    }

    fn lineage_direction(self) -> LineageDirection {
        match self {
            QueryDirection::Upstream => LineageDirection::Upstream,
            QueryDirection::Downstream => LineageDirection::Downstream,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FrontierNode {
    resolved: ResolvedObject,
    lookup_objects: Vec<LineageObjectRef>,
    column_filter: Option<ColumnFilter>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResolvedObject {
    object: LineageObjectRef,
    domain: String,
    name: String,
    addressing: ObjectAddressing,
    columns: Option<ColumnMap>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObjectAddressing {
    TableId,
    NameAddressed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LineageEdge {
    kind: LineageKind,
    source_object: LineageObjectRef,
    target_object: LineageObjectRef,
    source_column: Option<ColumnRef>,
    target_column: Option<ColumnRef>,
    last_query_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ColumnMap {
    columns: Vec<ResolvedColumn>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ColumnFilter {
    columns: Vec<ResolvedColumn>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResolvedColumn {
    name: String,
    id: Option<u64>,
}

#[derive(Clone, Debug)]
struct LineageListRequest {
    current: ResolvedObject,
    object: LineageObjectRef,
    column_filter: Option<ColumnFilter>,
}

#[derive(Debug)]
struct LineageListResult {
    current: ResolvedObject,
    column_filter: Option<ColumnFilter>,
    entries: Vec<(LineageKey, LineageDetail)>,
}

#[derive(Debug)]
struct LineageResolveRequest {
    current: ResolvedObject,
    edge: LineageEdge,
}

#[derive(Debug)]
struct ResolvedLineageEdge {
    kind: LineageKind,
    last_query_id: Option<String>,
    source_column: Option<ResolvedColumn>,
    target_column: Option<ResolvedColumn>,
    source: ResolvedObject,
    target: ResolvedObject,
}

#[derive(Clone)]
struct LineageSpec {
    kind: LineageKind,
}

#[derive(Default)]
struct LineageRows {
    distances: Vec<i32>,
    source_object_domains: Vec<String>,
    source_object_names: Vec<String>,
    source_column_names: Vec<Option<String>>,
    target_object_domains: Vec<String>,
    target_object_names: Vec<String>,
    target_column_names: Vec<Option<String>>,
    target_statuses: Vec<String>,
    processes: Vec<String>,
}

impl LineageRows {
    fn push(&mut self, distance: u8, edge: ResolvedLineageEdge) {
        self.distances.push(distance as i32);
        self.source_object_domains.push(edge.source.domain);
        self.source_object_names.push(edge.source.name);
        self.source_column_names.push(
            edge.source_column
                .as_ref()
                .map(|column| column.name.clone()),
        );
        self.target_object_domains.push(edge.target.domain);
        self.target_object_names.push(edge.target.name);
        self.target_column_names.push(
            edge.target_column
                .as_ref()
                .map(|column| column.name.clone()),
        );
        self.target_statuses.push("ACTIVE".to_string());
        self.processes
            .push(process_json(edge.last_query_id.as_deref()));
    }

    fn into_block(self) -> DataBlock {
        DataBlock::new_from_columns(vec![
            Int32Type::from_data(self.distances),
            StringType::from_data(self.source_object_domains),
            StringType::from_data(self.source_object_names),
            StringType::from_opt_data(self.source_column_names),
            StringType::from_data(self.target_object_domains),
            StringType::from_data(self.target_object_names),
            StringType::from_opt_data(self.target_column_names),
            StringType::from_data(self.target_statuses),
            StringType::from_data(self.processes),
        ])
    }
}

async fn collect_lineage(
    ctx: Arc<dyn TableContext>,
    args: GetLineageArgs,
    schema: DataSchemaRef,
) -> Result<DataBlock> {
    let rows = match args.direction {
        QueryDirection::Upstream => collect_upstream_lineage(ctx, args).await?,
        QueryDirection::Downstream => collect_downstream_lineage(ctx, args).await?,
    };

    if rows.distances.is_empty() {
        Ok(DataBlock::empty_with_schema(&schema))
    } else {
        Ok(rows.into_block())
    }
}

async fn collect_upstream_lineage(
    ctx: Arc<dyn TableContext>,
    args: GetLineageArgs,
) -> Result<LineageRows> {
    let table_name_parser = Arc::new(TableNameParser::new(&ctx)?);
    let start = resolve_start_object(&ctx, table_name_parser.as_ref(), &args).await?;
    let meta = UserApiProvider::instance().get_meta_store_client();
    let tenant = ctx.get_tenant();

    let mut rows = LineageRows::default();
    let mut frontier = vec![start]
        .into_iter()
        .filter_map(upstream_start_frontier)
        .collect::<Vec<_>>();
    let mut emitted = HashSet::new();

    for distance in 1..=args.distance {
        let mut next_frontier = Vec::new();
        let list_requests = lineage_list_requests(&frontier);
        let list_results = list_lineage_entries_concurrently(
            meta.clone(),
            tenant.clone(),
            args.direction,
            list_requests,
        )
        .await?;

        let mut resolve_requests = Vec::new();
        for LineageListResult {
            current,
            column_filter,
            entries,
        } in list_results
        {
            for (key, detail) in entries {
                resolve_requests.extend(
                    upstream_edges_from_entry(&key, &detail, column_filter.as_ref())
                        .into_iter()
                        .map(|edge| LineageResolveRequest {
                            current: current.clone(),
                            edge,
                        }),
                );
            }
        }

        let resolved_edges = resolve_upstream_lineage_edges_concurrently(
            ctx.clone(),
            table_name_parser.clone(),
            resolve_requests,
        )
        .await?;

        for resolved_edge in resolved_edges {
            if !emitted.insert(resolved_edge_dedup_key(
                distance,
                &resolved_edge.source,
                &resolved_edge.target,
                resolved_edge.source_column.as_ref(),
                resolved_edge.target_column.as_ref(),
            )) {
                continue;
            }

            let next = next_upstream_frontier_for_edge(&resolved_edge);
            rows.push(distance, resolved_edge);
            if let Some(next) = next {
                next_frontier.push(next);
            }
        }
        frontier = next_frontier;
        if frontier.is_empty() {
            break;
        }
    }

    Ok(rows)
}

async fn collect_downstream_lineage(
    ctx: Arc<dyn TableContext>,
    args: GetLineageArgs,
) -> Result<LineageRows> {
    let table_name_parser = Arc::new(TableNameParser::new(&ctx)?);
    let start = resolve_start_object(&ctx, table_name_parser.as_ref(), &args).await?;
    let meta = UserApiProvider::instance().get_meta_store_client();
    let tenant = ctx.get_tenant();

    let mut rows = LineageRows::default();
    let mut frontier = vec![downstream_start_frontier(start)];
    let mut emitted = HashSet::new();

    for distance in 1..=args.distance {
        let mut next_frontier = Vec::new();
        let list_requests = lineage_list_requests(&frontier);
        let list_results = list_lineage_entries_concurrently(
            meta.clone(),
            tenant.clone(),
            QueryDirection::Downstream,
            list_requests,
        )
        .await?;

        let mut resolve_requests = Vec::new();
        for LineageListResult {
            current,
            column_filter,
            entries,
        } in list_results
        {
            for (key, detail) in entries {
                resolve_requests.extend(
                    downstream_edges_from_entry(&key, &detail, column_filter.as_ref())
                        .into_iter()
                        .map(|edge| LineageResolveRequest {
                            current: current.clone(),
                            edge,
                        }),
                );
            }
        }

        let resolved_edges = resolve_downstream_lineage_edges_concurrently(
            ctx.clone(),
            table_name_parser.clone(),
            resolve_requests,
        )
        .await?;

        for resolved_edge in resolved_edges {
            if !emitted.insert(resolved_edge_dedup_key(
                distance,
                &resolved_edge.source,
                &resolved_edge.target,
                resolved_edge.source_column.as_ref(),
                resolved_edge.target_column.as_ref(),
            )) {
                continue;
            }

            let next = next_downstream_frontier_for_edge(&resolved_edge);
            rows.push(distance, resolved_edge);
            if let Some(next) = next {
                next_frontier.push(next);
            }
        }
        frontier = next_frontier;
        if frontier.is_empty() {
            break;
        }
    }

    Ok(rows)
}

fn upstream_start_frontier(mut start: FrontierNode) -> Option<FrontierNode> {
    (start.resolved.object.object_type == LineageObjectType::Table
        && start.resolved.addressing == ObjectAddressing::TableId)
        .then(|| {
            start.lookup_objects = upstream_lookup_objects(&start.resolved);
            start
        })
}

fn downstream_start_frontier(mut start: FrontierNode) -> FrontierNode {
    start.lookup_objects = downstream_lookup_objects(&start.resolved);
    start
}

fn lineage_list_requests(frontier: &[FrontierNode]) -> Vec<LineageListRequest> {
    frontier
        .iter()
        .flat_map(|node| {
            node.lookup_objects
                .iter()
                .cloned()
                .map(|object| LineageListRequest {
                    current: node.resolved.clone(),
                    object,
                    column_filter: node.column_filter.clone(),
                })
        })
        .collect()
}

async fn list_lineage_entries_concurrently(
    meta: Arc<MetaStore>,
    tenant: databend_common_meta_app::tenant::Tenant,
    direction: QueryDirection,
    requests: Vec<LineageListRequest>,
) -> Result<Vec<LineageListResult>> {
    stream::iter(requests.into_iter().map(|request| {
        let meta = meta.clone();
        let tenant = tenant.clone();
        async move {
            let entries = meta
                .as_ref()
                .list_lineage(ListLineageReq {
                    tenant,
                    direction: direction.lineage_direction(),
                    object: request.object,
                })
                .await
                .map_err(meta_service_error)?
                .entries;
            Ok(LineageListResult {
                current: request.current,
                column_filter: request.column_filter,
                entries,
            })
        }
    }))
    .buffer_unordered(LINEAGE_META_LOOKUP_MAX_CONCURRENCY)
    .try_collect()
    .await
}

async fn resolve_downstream_lineage_edges_concurrently(
    ctx: Arc<dyn TableContext>,
    table_name_parser: Arc<TableNameParser>,
    requests: Vec<LineageResolveRequest>,
) -> Result<Vec<ResolvedLineageEdge>> {
    let resolved: Vec<Option<ResolvedLineageEdge>> =
        stream::iter(requests.into_iter().map(|request| {
            let ctx = ctx.clone();
            let table_name_parser = table_name_parser.clone();
            async move { resolve_downstream_lineage_edge(ctx, table_name_parser, request).await }
        }))
        .buffer_unordered(LINEAGE_META_LOOKUP_MAX_CONCURRENCY)
        .try_collect()
        .await?;

    Ok(resolved.into_iter().flatten().collect())
}

async fn resolve_upstream_lineage_edges_concurrently(
    ctx: Arc<dyn TableContext>,
    table_name_parser: Arc<TableNameParser>,
    requests: Vec<LineageResolveRequest>,
) -> Result<Vec<ResolvedLineageEdge>> {
    let resolved: Vec<Option<ResolvedLineageEdge>> =
        stream::iter(requests.into_iter().map(|request| {
            let ctx = ctx.clone();
            let table_name_parser = table_name_parser.clone();
            async move { resolve_upstream_lineage_edge(ctx, table_name_parser, request).await }
        }))
        .buffer_unordered(LINEAGE_META_LOOKUP_MAX_CONCURRENCY)
        .try_collect()
        .await?;

    Ok(resolved.into_iter().flatten().collect())
}

async fn resolve_downstream_lineage_edge(
    ctx: Arc<dyn TableContext>,
    table_name_parser: Arc<TableNameParser>,
    request: LineageResolveRequest,
) -> Result<Option<ResolvedLineageEdge>> {
    let LineageResolveRequest { current, edge } = request;
    let Some(target) =
        resolve_lineage_object(&ctx, table_name_parser.as_ref(), &edge.target_object).await?
    else {
        return Ok(None);
    };
    Ok(resolved_downstream_edge_from_current(current, edge, target))
}

async fn resolve_upstream_lineage_edge(
    ctx: Arc<dyn TableContext>,
    table_name_parser: Arc<TableNameParser>,
    request: LineageResolveRequest,
) -> Result<Option<ResolvedLineageEdge>> {
    let LineageResolveRequest { current, edge } = request;
    let Some(source) =
        resolve_lineage_object(&ctx, table_name_parser.as_ref(), &edge.source_object).await?
    else {
        return Ok(None);
    };
    Ok(resolved_upstream_edge_from_current(current, edge, source))
}

fn resolved_downstream_edge_from_current(
    current: ResolvedObject,
    edge: LineageEdge,
    target: ResolvedObject,
) -> Option<ResolvedLineageEdge> {
    let source_column = resolve_column_ref(&current, edge.source_column.as_ref())?;
    let target_column = resolve_column_ref(&target, edge.target_column.as_ref())?;
    Some(ResolvedLineageEdge {
        kind: edge.kind,
        last_query_id: edge.last_query_id,
        source_column,
        target_column,
        source: current,
        target,
    })
}

fn resolved_upstream_edge_from_current(
    current: ResolvedObject,
    edge: LineageEdge,
    source: ResolvedObject,
) -> Option<ResolvedLineageEdge> {
    let source_column = resolve_column_ref(&source, edge.source_column.as_ref())?;
    let target_column = resolve_column_ref(&current, edge.target_column.as_ref())?;
    Some(ResolvedLineageEdge {
        kind: edge.kind,
        last_query_id: edge.last_query_id,
        source_column,
        target_column,
        source,
        target: current,
    })
}

fn column_map_from_schema(schema: &TableSchemaRef) -> ColumnMap {
    ColumnMap {
        columns: schema
            .fields()
            .iter()
            .map(|field| ResolvedColumn {
                name: field.name().to_string(),
                id: Some(field.column_id() as u64),
            })
            .collect(),
    }
}

fn resolve_column_filter(object: &ResolvedObject, column: ColumnRef) -> Result<ColumnFilter> {
    let Some(Some(resolved_column)) = resolve_column_ref(object, Some(&column)) else {
        return Err(ErrorCode::BadArguments(format!(
            "column '{}' does not exist in object '{}'",
            column_ref_display(&column),
            object.name
        )));
    };
    Ok(ColumnFilter {
        columns: vec![resolved_column],
    })
}

fn resolve_column_ref(
    object: &ResolvedObject,
    column: Option<&ColumnRef>,
) -> Option<Option<ResolvedColumn>> {
    let Some(column) = column else {
        return Some(None);
    };
    let Some(column_map) = &object.columns else {
        return Some(Some(resolved_column_from_ref(column)));
    };
    column_map
        .columns
        .iter()
        .find(|candidate| column_ref_matches_resolved(column, candidate))
        .cloned()
        .map(Some)
}

fn column_matches_filter(column: &ColumnRef, filter: &ColumnFilter) -> bool {
    filter
        .columns
        .iter()
        .any(|candidate| column_ref_matches_resolved(column, candidate))
}

fn column_ref_matches_resolved(column: &ColumnRef, resolved: &ResolvedColumn) -> bool {
    match column {
        ColumnRef::Id(id) => resolved.id == Some(*id),
        ColumnRef::Name(name) => resolved.name.eq_ignore_ascii_case(name),
    }
}

fn resolved_column_from_ref(column: &ColumnRef) -> ResolvedColumn {
    match column {
        ColumnRef::Id(id) => ResolvedColumn {
            name: id.to_string(),
            id: Some(*id),
        },
        ColumnRef::Name(name) => ResolvedColumn {
            name: name.clone(),
            id: None,
        },
    }
}

fn column_ref_display(column: &ColumnRef) -> String {
    match column {
        ColumnRef::Id(id) => id.to_string(),
        ColumnRef::Name(name) => name.clone(),
    }
}

fn downstream_edges_from_entry(
    key: &LineageKey,
    detail: &LineageDetail,
    column_filter: Option<&ColumnFilter>,
) -> Vec<LineageEdge> {
    lineage_edges_from_entry(
        QueryDirection::Downstream,
        key.object.clone(),
        key.related_object.clone(),
        detail,
        column_filter,
    )
}

fn upstream_edges_from_entry(
    key: &LineageKey,
    detail: &LineageDetail,
    column_filter: Option<&ColumnFilter>,
) -> Vec<LineageEdge> {
    lineage_edges_from_entry(
        QueryDirection::Upstream,
        key.related_object.clone(),
        key.object.clone(),
        detail,
        column_filter,
    )
}

fn lineage_edges_from_entry(
    direction: QueryDirection,
    source_object: LineageObjectRef,
    target_object: LineageObjectRef,
    detail: &LineageDetail,
    column_filter: Option<&ColumnFilter>,
) -> Vec<LineageEdge> {
    let Some(spec) = LineageSpec::new(&detail.kind) else {
        return Vec::new();
    };
    if let Some(column_name) = column_filter {
        return detail
            .column_lineage
            .iter()
            .filter_map(|column| spec.column_edge(direction, column, column_name))
            .map(|(source_column_name, target_column_name)| LineageEdge {
                kind: detail.kind.clone(),
                source_object: source_object.clone(),
                target_object: target_object.clone(),
                source_column: Some(source_column_name),
                target_column: Some(target_column_name),
                last_query_id: detail.last_query_id.clone(),
            })
            .collect();
    }

    vec![LineageEdge {
        kind: detail.kind.clone(),
        source_object,
        target_object,
        source_column: None,
        target_column: None,
        last_query_id: detail.last_query_id.clone(),
    }]
}

impl LineageSpec {
    fn new(kind: &LineageKind) -> Option<Self> {
        match kind {
            LineageKind::Unknown(_) => None,
            _ => Some(Self { kind: kind.clone() }),
        }
    }

    fn column_edge(
        &self,
        direction: QueryDirection,
        column: &LineageColumn,
        column_filter: &ColumnFilter,
    ) -> Option<(ColumnRef, ColumnRef)> {
        match direction {
            QueryDirection::Downstream => column_matches_filter(&column.upstream, column_filter)
                .then(|| (column.upstream.clone(), column.downstream.clone())),
            QueryDirection::Upstream => column_matches_filter(&column.downstream, column_filter)
                .then(|| (column.upstream.clone(), column.downstream.clone())),
        }
    }

    fn upstream_next_lookup_objects(&self, resolved: &ResolvedObject) -> Vec<LineageObjectRef> {
        match &self.kind {
            LineageKind::View | LineageKind::MaterializedView => {
                table_like_id_and_name_lookup_objects(resolved)
            }
            LineageKind::Ctas | LineageKind::DataMovement | LineageKind::Unknown(_) => {
                canonical_lookup_object(resolved)
            }
        }
    }

    fn downstream_next_lookup_objects(&self, resolved: &ResolvedObject) -> Vec<LineageObjectRef> {
        table_like_id_and_name_lookup_objects(resolved)
    }
}

async fn resolve_start_object(
    ctx: &Arc<dyn TableContext>,
    table_name_parser: &TableNameParser,
    args: &GetLineageArgs,
) -> Result<FrontierNode> {
    match args.object_domain {
        ObjectDomain::Table | ObjectDomain::View => {
            let table = resolve_table_like(
                ctx,
                table_name_parser,
                &args.object_name,
                args.object_domain,
            )
            .await?;
            Ok(FrontierNode {
                lookup_objects: start_lookup_objects(args.direction, &table),
                resolved: table,
                column_filter: None,
            })
        }
        ObjectDomain::Column => {
            let (table_name, column_name) = split_column_name(&args.object_name)?;
            let table =
                resolve_table_like(ctx, table_name_parser, &table_name, ObjectDomain::Column)
                    .await?;
            let column_filter = resolve_column_filter(
                &table,
                ColumnRef::Name(table_name_parser.normalize_column_identifier(&column_name)),
            )?;
            Ok(FrontierNode {
                lookup_objects: start_lookup_objects(args.direction, &table),
                resolved: table,
                column_filter: Some(column_filter),
            })
        }
        ObjectDomain::Stage => {
            let stage = resolve_stage(ctx, &args.object_name).await?;
            Ok(FrontierNode {
                lookup_objects: start_lookup_objects(args.direction, &stage),
                resolved: stage,
                column_filter: None,
            })
        }
    }
}

async fn resolve_table_like(
    ctx: &Arc<dyn TableContext>,
    table_name_parser: &TableNameParser,
    object_name: &str,
    expected_domain: ObjectDomain,
) -> Result<ResolvedObject> {
    let (catalog_name, db_name, table_name) = table_name_parser.parse_table_name(object_name)?;
    let table = ctx.get_table(&catalog_name, &db_name, &table_name).await?;
    let table_id = table.get_table_info().ident.table_id;
    let engine = table.engine();
    let columns = Some(column_map_for_table(ctx, &table).await);
    match expected_domain {
        ObjectDomain::View if engine != VIEW_ENGINE => {
            return Err(ErrorCode::BadArguments(format!(
                "object '{}' is not a VIEW",
                object_name
            )));
        }
        ObjectDomain::Table if matches!(engine, VIEW_ENGINE | STREAM_ENGINE) => {
            return Err(ErrorCode::BadArguments(format!(
                "object '{}' is not a TABLE",
                object_name
            )));
        }
        _ => {}
    }

    Ok(ResolvedObject {
        object: LineageObjectRef {
            object_type: LineageObjectType::Table,
            identity: LineageIdentity::Id {
                id: table_id.to_string(),
            },
        },
        domain: table_domain(engine).to_string(),
        name: full_table_name(&catalog_name, &db_name, &table_name),
        addressing: ObjectAddressing::TableId,
        columns,
    })
}

async fn resolve_stage(ctx: &Arc<dyn TableContext>, object_name: &str) -> Result<ResolvedObject> {
    let stage_name = object_name.trim().trim_start_matches('@');
    if stage_name.is_empty() {
        return Err(ErrorCode::BadArguments("stage name must not be empty"));
    }

    let tenant = ctx.get_tenant();
    let stage_info = UserApiProvider::instance()
        .get_stage(&tenant, stage_name)
        .await?;

    Ok(ResolvedObject {
        object: LineageObjectRef {
            object_type: LineageObjectType::Stage,
            identity: LineageIdentity::Name {
                name: stage_info.stage_name.clone(),
            },
        },
        domain: "STAGE".to_string(),
        name: stage_info.stage_name,
        addressing: ObjectAddressing::NameAddressed,
        columns: None,
    })
}

async fn resolve_lineage_object(
    ctx: &Arc<dyn TableContext>,
    table_name_parser: &TableNameParser,
    object: &LineageObjectRef,
) -> Result<Option<ResolvedObject>> {
    match (&object.object_type, &object.identity) {
        (LineageObjectType::Table, LineageIdentity::Id { id }) => {
            let Ok(table_id) = id.parse::<u64>() else {
                return Ok(None);
            };
            // Stable table ids belong to the meta-backed catalog; external
            // catalogs such as Iceberg and Hive use name-addressed lineage.
            let catalog_name = ctx.get_current_catalog();
            let catalog = ctx.get_catalog(&catalog_name).await?;
            let meta = UserApiProvider::instance().get_meta_store_client();
            let Some(name_entry) = meta
                .get_pb(&TableIdToName { table_id })
                .await
                .map_err(meta_service_error)?
            else {
                return Ok(None);
            };
            let db_name = match catalog.get_db_name_by_id(name_entry.data.db_id).await {
                Ok(db_name) => db_name,
                Err(error) if error.code() == ErrorCode::UNKNOWN_DATABASE_ID => return Ok(None),
                Err(error) => return Err(error),
            };
            let table_name = name_entry.data.table_name;
            let Ok(table) = ctx.get_table(&catalog_name, &db_name, &table_name).await else {
                return Ok(None);
            };
            let actual_table_id = table.get_table_info().ident.table_id;
            if actual_table_id != table_id {
                log::warn!(
                    "skip lineage object '{}.{}.{}': expected table id {}, got {}",
                    catalog_name,
                    db_name,
                    table_name,
                    table_id,
                    actual_table_id
                );
                return Ok(None);
            }
            let full_name = full_table_name(&catalog_name, &db_name, &table_name);
            Ok(Some(ResolvedObject {
                object: object.clone(),
                domain: table_domain(table.engine()).to_string(),
                name: full_name,
                addressing: ObjectAddressing::TableId,
                columns: Some(column_map_for_table(ctx, &table).await),
            }))
        }
        (LineageObjectType::Table, LineageIdentity::Name { name }) => {
            let Ok((catalog_name, db_name, table_name)) = table_name_parser.parse_table_name(name)
            else {
                return Ok(None);
            };
            let table = ctx
                .get_table(&catalog_name, &db_name, &table_name)
                .await
                .ok();
            let Some(table) = table else {
                return Ok(None);
            };
            let table_id = table.get_table_info().ident.table_id;
            Ok(Some(ResolvedObject {
                object: LineageObjectRef {
                    object_type: LineageObjectType::Table,
                    identity: LineageIdentity::Id {
                        id: table_id.to_string(),
                    },
                },
                domain: table_domain(table.engine()).to_string(),
                name: full_table_name(&catalog_name, &db_name, &table_name),
                addressing: ObjectAddressing::TableId,
                columns: Some(column_map_for_table(ctx, &table).await),
            }))
        }
        (LineageObjectType::Stage, LineageIdentity::Name { name }) => Ok(Some(ResolvedObject {
            object: object.clone(),
            domain: "STAGE".to_string(),
            name: name.clone(),
            addressing: ObjectAddressing::NameAddressed,
            columns: None,
        })),
        _ => Ok(None),
    }
}

async fn column_map_for_table(ctx: &Arc<dyn TableContext>, table: &Arc<dyn Table>) -> ColumnMap {
    if table.engine() != VIEW_ENGINE {
        return column_map_from_schema(&table.schema());
    }

    let Some(query) = table.options().get(QUERY) else {
        return ColumnMap { columns: vec![] };
    };
    let mut planner = Planner::new(ctx.clone());
    match planner.plan_sql(query).await {
        Ok((plan, _)) => match infer_table_schema(&plan.schema()) {
            Ok(schema) => column_map_from_schema(&schema),
            Err(error) => {
                log::warn!(
                    "failed to infer view schema for lineage object '{}': {error}",
                    table.name()
                );
                ColumnMap { columns: vec![] }
            }
        },
        Err(error) => {
            log::warn!(
                "failed to plan view schema for lineage object '{}': {error}",
                table.name()
            );
            ColumnMap { columns: vec![] }
        }
    }
}

fn next_upstream_frontier_for_edge(edge: &ResolvedLineageEdge) -> Option<FrontierNode> {
    if edge.source.object.object_type != LineageObjectType::Table
        || edge.source.addressing != ObjectAddressing::TableId
    {
        return None;
    }
    let spec = LineageSpec::new(&edge.kind).expect("resolved lineage edge must have known kind");
    Some(FrontierNode {
        lookup_objects: spec.upstream_next_lookup_objects(&edge.source),
        resolved: edge.source.clone(),
        column_filter: edge.source_column.clone().map(|column| ColumnFilter {
            columns: vec![column],
        }),
    })
}

fn next_downstream_frontier_for_edge(edge: &ResolvedLineageEdge) -> Option<FrontierNode> {
    if edge.target.object.object_type != LineageObjectType::Table
        || edge.target.addressing != ObjectAddressing::TableId
    {
        return None;
    }
    let spec = LineageSpec::new(&edge.kind).expect("resolved lineage edge must have known kind");
    Some(FrontierNode {
        lookup_objects: spec.downstream_next_lookup_objects(&edge.target),
        resolved: edge.target.clone(),
        column_filter: edge.target_column.clone().map(|column| ColumnFilter {
            columns: vec![column],
        }),
    })
}

fn start_lookup_objects(
    direction: QueryDirection,
    resolved: &ResolvedObject,
) -> Vec<LineageObjectRef> {
    match direction {
        QueryDirection::Upstream => upstream_lookup_objects(resolved),
        QueryDirection::Downstream => downstream_lookup_objects(resolved),
    }
}

fn upstream_lookup_objects(resolved: &ResolvedObject) -> Vec<LineageObjectRef> {
    canonical_lookup_object(resolved)
}

fn downstream_lookup_objects(resolved: &ResolvedObject) -> Vec<LineageObjectRef> {
    table_like_id_and_name_lookup_objects(resolved)
}

fn table_like_id_and_name_lookup_objects(resolved: &ResolvedObject) -> Vec<LineageObjectRef> {
    if resolved.object.object_type == LineageObjectType::Table
        && resolved.addressing == ObjectAddressing::TableId
    {
        vec![resolved.object.clone(), LineageObjectRef {
            object_type: LineageObjectType::Table,
            identity: LineageIdentity::Name {
                name: resolved.name.clone(),
            },
        }]
    } else {
        vec![resolved.object.clone()]
    }
}

fn canonical_lookup_object(resolved: &ResolvedObject) -> Vec<LineageObjectRef> {
    vec![resolved.object.clone()]
}

fn resolved_edge_dedup_key(
    distance: u8,
    source: &ResolvedObject,
    target: &ResolvedObject,
    source_column: Option<&ResolvedColumn>,
    target_column: Option<&ResolvedColumn>,
) -> String {
    format!(
        "{distance}/{}/{}/{}/{}/{}/{}",
        object_type_key(&source.object.object_type),
        identity_key(&source.object.identity),
        column_key(source_column),
        object_type_key(&target.object.object_type),
        identity_key(&target.object.identity),
        column_key(target_column)
    )
}

fn column_key(column: Option<&ResolvedColumn>) -> String {
    match column {
        Some(ResolvedColumn { id: Some(id), .. }) => format!("id/{id}"),
        Some(ResolvedColumn { name, .. }) => format!("name/{name}"),
        None => String::new(),
    }
}

fn split_column_name(input: &str) -> Result<(String, String)> {
    let input = input.trim();
    if input.is_empty() {
        return Err(ErrorCode::BadArguments(
            "column object_name must not be empty",
        ));
    }

    let Some(index) = last_unquoted_dot(input) else {
        return Err(ErrorCode::BadArguments(
            "column object_name must be qualified by table name".to_string(),
        ));
    };
    let table = input[..index].trim();
    let column = input[index + 1..].trim();
    if table.is_empty() || column.is_empty() {
        return Err(ErrorCode::BadArguments(
            "column object_name must be in table.column format".to_string(),
        ));
    }
    Ok((table.to_string(), column.to_string()))
}

fn last_unquoted_dot(input: &str) -> Option<usize> {
    let mut in_quote = false;
    let mut last_dot = None;
    let mut iter = input.char_indices().peekable();
    while let Some((index, ch)) = iter.next() {
        match ch {
            '"' => {
                if in_quote && matches!(iter.peek(), Some((_, '"'))) {
                    iter.next();
                } else {
                    in_quote = !in_quote;
                }
            }
            '.' if !in_quote => last_dot = Some(index),
            _ => {}
        }
    }
    last_dot
}

fn table_domain(engine: &str) -> &'static str {
    match engine {
        VIEW_ENGINE => "VIEW",
        STREAM_ENGINE => "STREAM",
        _ => "TABLE",
    }
}

fn full_table_name(catalog: &str, database: &str, table: &str) -> String {
    if catalog == "default" {
        format!("{database}.{table}")
    } else {
        format!("{catalog}.{database}.{table}")
    }
}

fn process_json(last_query_id: Option<&str>) -> String {
    match last_query_id {
        Some(last_query_id) => json!({ "last_query_id": last_query_id }).to_string(),
        None => "{}".to_string(),
    }
}

fn object_type_key(object_type: &LineageObjectType) -> &'static str {
    match object_type {
        LineageObjectType::Table => "table",
        LineageObjectType::Stage => "stage",
    }
}

fn identity_key(identity: &LineageIdentity) -> String {
    match identity {
        LineageIdentity::Id { id } => format!("id/{id}"),
        LineageIdentity::Name { name } => format!("name/{name}"),
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use chrono::Utc;
    use databend_common_catalog::table_args::TableArgs;
    use databend_common_expression::Scalar;

    use super::*;

    #[test]
    fn test_parse_args() -> Result<()> {
        let args = TableArgs::new_positioned(vec![
            Scalar::String("db.t".to_string()),
            Scalar::String("table".to_string()),
            Scalar::String("downSTREAM".to_string()),
        ]);
        let parsed = GetLineageArgs::parse(&args)?;
        assert_eq!(parsed.object_name, "db.t");
        assert_eq!(parsed.object_domain, ObjectDomain::Table);
        assert_eq!(parsed.direction, QueryDirection::Downstream);
        assert_eq!(parsed.distance, DEFAULT_DISTANCE);

        let args = TableArgs::new_positioned(vec![
            Scalar::String("db.t".to_string()),
            Scalar::String("column".to_string()),
            Scalar::String("UPSTREAM".to_string()),
            Scalar::Number(databend_common_expression::types::NumberScalar::Int64(2)),
        ]);
        assert_eq!(GetLineageArgs::parse(&args)?.distance, 2);

        let args = TableArgs::new_positioned(vec![
            Scalar::String("db.t".to_string()),
            Scalar::String("stream".to_string()),
            Scalar::String("UPSTREAM".to_string()),
        ]);
        assert!(GetLineageArgs::parse(&args).is_err());

        for distance in [0, MAX_DISTANCE + 1] {
            let args = TableArgs::new_positioned(vec![
                Scalar::String("db.t".to_string()),
                Scalar::String("table".to_string()),
                Scalar::String("UPSTREAM".to_string()),
                Scalar::Number(databend_common_expression::types::NumberScalar::Int64(
                    i64::from(distance),
                )),
            ]);
            assert!(GetLineageArgs::parse(&args).is_err());
        }

        let invalid_direction = TableArgs::new_positioned(vec![
            Scalar::String("db.t".to_string()),
            Scalar::String("table".to_string()),
            Scalar::String("SIDEWAYS".to_string()),
        ]);
        assert!(GetLineageArgs::parse(&invalid_direction).is_err());

        let too_few = TableArgs::new_positioned(vec![
            Scalar::String("db.t".to_string()),
            Scalar::String("table".to_string()),
        ]);
        assert!(GetLineageArgs::parse(&too_few).is_err());

        Ok(())
    }

    #[test]
    fn test_split_column_name() -> Result<()> {
        assert_eq!(
            split_column_name("db.t.c")?,
            ("db.t".to_string(), "c".to_string())
        );
        assert_eq!(
            split_column_name(r#""db.name"."t.name"."c.name""#)?,
            (
                r#""db.name"."t.name""#.to_string(),
                r#""c.name""#.to_string()
            )
        );
        assert!(split_column_name("c").is_err());
        assert!(split_column_name("t.").is_err());
        Ok(())
    }

    #[test]
    fn test_directional_edges_from_entry_table_direction() {
        let key = lineage_key(LineageDirection::Downstream);
        let detail = LineageDetail {
            kind: LineageKind::DataMovement,
            last_query_id: Some("q1".to_string()),
            updated_on: lineage_time(),
            column_lineage: vec![],
        };

        let downstream = downstream_edges_from_entry(&key, &detail, None);
        assert_eq!(identity_key(&downstream[0].source_object.identity), "id/1");
        assert_eq!(identity_key(&downstream[0].target_object.identity), "id/2");

        let upstream = upstream_edges_from_entry(&key, &detail, None);
        assert_eq!(identity_key(&upstream[0].source_object.identity), "id/2");
        assert_eq!(identity_key(&upstream[0].target_object.identity), "id/1");
    }

    #[test]
    fn test_directional_edges_from_entry_column_filter() {
        let key = lineage_key(LineageDirection::Downstream);
        let detail = LineageDetail {
            kind: LineageKind::DataMovement,
            last_query_id: None,
            updated_on: lineage_time(),
            column_lineage: vec![
                LineageColumn {
                    upstream: ColumnRef::Id(1),
                    downstream: ColumnRef::Id(10),
                },
                LineageColumn {
                    upstream: ColumnRef::Id(2),
                    downstream: ColumnRef::Id(10),
                },
            ],
        };

        let downstream = downstream_edges_from_entry(
            &key,
            &detail,
            Some(&ColumnFilter {
                columns: vec![resolved_column("a", Some(1))],
            }),
        );
        assert_eq!(downstream.len(), 1);
        assert_eq!(
            downstream[0]
                .source_column
                .as_ref()
                .map(column_ref_display)
                .as_deref(),
            Some("1")
        );
        assert_eq!(
            downstream[0]
                .target_column
                .as_ref()
                .map(column_ref_display)
                .as_deref(),
            Some("10")
        );

        let upstream = upstream_edges_from_entry(
            &key,
            &detail,
            Some(&ColumnFilter {
                columns: vec![resolved_column("x", Some(10))],
            }),
        );
        assert_eq!(upstream.len(), 2);
    }

    #[test]
    fn test_table_lookup_objects_by_direction() {
        let resolved = resolved_table("1", "db.t");

        let downstream = downstream_lookup_objects(&resolved);
        assert_eq!(downstream.len(), 2);
        assert_eq!(identity_key(&downstream[0].identity), "id/1");
        assert_eq!(identity_key(&downstream[1].identity), "name/db.t");

        let upstream = upstream_lookup_objects(&resolved);
        assert_eq!(upstream.len(), 1);
        assert_eq!(identity_key(&upstream[0].identity), "id/1");

        let start_upstream = start_lookup_objects(QueryDirection::Upstream, &resolved);
        assert_eq!(start_upstream, upstream);

        let start_downstream = start_lookup_objects(QueryDirection::Downstream, &resolved);
        assert_eq!(start_downstream, downstream);
    }

    #[test]
    fn test_next_frontier_keeps_resolved_current_object() {
        let source = resolved_table("1", "db.source");
        let target = resolved_table("2", "db.target");
        let edge = ResolvedLineageEdge {
            kind: LineageKind::DataMovement,
            last_query_id: None,
            source_column: Some(resolved_column("a", Some(1))),
            target_column: Some(resolved_column("b", Some(2))),
            source: source.clone(),
            target: target.clone(),
        };

        let upstream = next_upstream_frontier_for_edge(&edge).unwrap();
        assert_eq!(upstream.resolved, source);
        assert_eq!(upstream.lookup_objects.len(), 1);
        assert_eq!(identity_key(&upstream.lookup_objects[0].identity), "id/1");
        assert_eq!(
            upstream
                .column_filter
                .as_ref()
                .and_then(|filter| filter.columns.first())
                .map(|column| column.name.as_str()),
            Some("a")
        );

        let downstream = next_downstream_frontier_for_edge(&edge).unwrap();
        assert_eq!(downstream.resolved, target);
        assert_eq!(downstream.lookup_objects.len(), 2);
        assert_eq!(identity_key(&downstream.lookup_objects[0].identity), "id/2");
        assert_eq!(
            identity_key(&downstream.lookup_objects[1].identity),
            "name/db.target"
        );
        assert_eq!(
            downstream
                .column_filter
                .as_ref()
                .and_then(|filter| filter.columns.first())
                .map(|column| column.name.as_str()),
            Some("b")
        );
    }

    #[test]
    fn test_upstream_view_kind_expands_next_lookup_to_id_and_name() {
        let source = resolved_table("1", "db.source");
        let target = resolved_table("2", "db.target");
        let edge = ResolvedLineageEdge {
            kind: LineageKind::View,
            last_query_id: None,
            source_column: None,
            target_column: None,
            source: source.clone(),
            target,
        };

        let upstream = next_upstream_frontier_for_edge(&edge).unwrap();
        assert_eq!(upstream.lookup_objects.len(), 2);
        assert_eq!(identity_key(&upstream.lookup_objects[0].identity), "id/1");
        assert_eq!(
            identity_key(&upstream.lookup_objects[1].identity),
            "name/db.source"
        );
    }

    #[test]
    fn test_upstream_stage_source_does_not_continue() {
        let source = ResolvedObject {
            object: LineageObjectRef {
                object_type: LineageObjectType::Stage,
                identity: LineageIdentity::Name {
                    name: "s".to_string(),
                },
            },
            domain: "STAGE".to_string(),
            name: "s".to_string(),
            addressing: ObjectAddressing::NameAddressed,
            columns: None,
        };
        let target = resolved_table("2", "db.target");
        let edge = ResolvedLineageEdge {
            kind: LineageKind::DataMovement,
            last_query_id: None,
            source_column: None,
            target_column: None,
            source,
            target,
        };

        assert!(next_upstream_frontier_for_edge(&edge).is_none());
    }

    fn resolved_table(id: &str, name: &str) -> ResolvedObject {
        ResolvedObject {
            object: LineageObjectRef {
                object_type: LineageObjectType::Table,
                identity: LineageIdentity::Id { id: id.to_string() },
            },
            domain: "TABLE".to_string(),
            name: name.to_string(),
            addressing: ObjectAddressing::TableId,
            columns: Some(ColumnMap {
                columns: vec![
                    resolved_column("a", Some(1)),
                    resolved_column("b", Some(2)),
                    resolved_column("c", Some(3)),
                    resolved_column("x", Some(10)),
                ],
            }),
        }
    }

    fn resolved_column(name: &str, id: Option<u64>) -> ResolvedColumn {
        ResolvedColumn {
            name: name.to_string(),
            id,
        }
    }

    fn lineage_key(direction: LineageDirection) -> LineageKey {
        LineageKey {
            direction,
            object: LineageObjectRef {
                object_type: LineageObjectType::Table,
                identity: LineageIdentity::Id {
                    id: "1".to_string(),
                },
            },
            related_object: LineageObjectRef {
                object_type: LineageObjectType::Table,
                identity: LineageIdentity::Id {
                    id: "2".to_string(),
                },
            },
        }
    }

    fn lineage_time() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 23, 0, 0, 0).unwrap()
    }
}
